/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.sequences.SequenceState;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.Transformation.Kind.AWAIT_ACCORD_TABLE_COMPLETE;
import static org.apache.cassandra.tcm.Transformation.Kind.DROP_ACCORD_TABLE;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;

public class DropAccordTable extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(DropAccordTable.class);

    public final Transformation.Kind next;
    public final TableReference table;
    public final AwaitAccordTableComplete awaitAccordTableComplete;
    public final DropTable dropTable;

    protected DropAccordTable(TableReference table, Epoch latestModification)
    {
        this(table, AWAIT_ACCORD_TABLE_COMPLETE, latestModification);
    }

    public DropAccordTable(DropAccordTable current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        table = current.table;
        awaitAccordTableComplete = current.awaitAccordTableComplete;
        dropTable = current.dropTable;
    }

    private DropAccordTable(TableReference table, Transformation.Kind next, Epoch lastModified)
    {
        super(nextToIndex(next), lastModified);
        this.next = next;
        this.table = table;
        awaitAccordTableComplete = new AwaitAccordTableComplete(table);
        dropTable = new DropTable(table);
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case AWAIT_ACCORD_TABLE_COMPLETE:
                return 0;
            case DROP_ACCORD_TABLE:
                return 1;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, Kind.DROP_ACCORD_TABLE));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return AWAIT_ACCORD_TABLE_COMPLETE;
            case 1:
                return DROP_ACCORD_TABLE;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", index, Kind.DROP_ACCORD_TABLE));
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropAccordTable that = (DropAccordTable) o;
        return latestModification.equals(that.latestModification)
               && next == that.next
               && table.equals(that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, next, table);
    }

    @Override
    public Kind kind()
    {
        return Kind.DROP_ACCORD_TABLE;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return table;
    }

    @Override
    public MetadataSerializer<? extends SequenceKey> keySerializer()
    {
        return TableReferenceSerializer.instance;
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return indexToNext(idx);
    }

    @Override
    public SequenceState executeNext()
    {
        switch (next)
        {
            case AWAIT_ACCORD_TABLE_COMPLETE:
                try
                {
                    return awaitAccordTableComplete.execute(ClusterMetadataService.instance());
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Exception committing await_accord_table_complete", t);
                    return continuable();
                }
            case DROP_ACCORD_TABLE:
                try
                {
                    return dropTable.execute(ClusterMetadataService.instance());
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Exception committing drop_accord_table", t);
                    return continuable();
                }
            default:
                return error(new IllegalStateException("Can't proceed with drop accord table from " + next));
        }
    }

    @Override
    public Transformation.Result applyTo(ClusterMetadata metadata)
    {
        return dropTable.execute(metadata);
    }

    @Override
    public DropAccordTable advance(Epoch epoch)
    {
        return new DropAccordTable(this, epoch);
    }

    @Override
    public ProgressBarrier barrier()
    {
        return ProgressBarrier.immediate();
    }

    public static class TableReference implements SequenceKey
    {
        public final String keyspace, name;
        public final TableId id;

        public TableReference(String keyspace, String name, TableId id)
        {
            this.keyspace = normalize(keyspace);
            this.name = normalize(name);
            this.id = id;
        }

        private static boolean isASCII(String input)
        {
            for (char c : input.toCharArray())
            {
                if (c > 127)
                    return false;
            }
            return true;
        }

        private static String normalize(String input)
        {
            if (isASCII(input)) return input;
            return new String(input.getBytes(StandardCharsets.US_ASCII), StandardCharsets.US_ASCII);
        }

        public static TableReference from(TableMetadata metadata)
        {
            return new TableReference(metadata.keyspace, metadata.name, metadata.id);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableReference that = (TableReference) o;
            return keyspace.equals(that.keyspace) && name.equals(that.name) && id.equals(that.id);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, name, id);
        }

        @Override
        public String toString()
        {
            return "TableReference{" +
                   "keyspace='" + keyspace + '\'' +
                   ", name='" + name + '\'' +
                   ", id=" + id +
                   '}';
        }
    }

    public interface Step
    {
        SequenceState execute(ClusterMetadataService cms) throws Exception;
    }

    public static abstract class BaseStep implements Step, Transformation
    {
        public final TableReference table;

        public BaseStep(TableReference table)
        {
            this.table = table;
        }

        @Override
        public SequenceState execute(ClusterMetadataService cms) throws Exception
        {
            cms.commit(this);
            return continuable();
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            ClusterMetadata.Transformer proposed = prev.transformer()
                                                       .with(prev.inProgressSequences.with(table, plan -> plan.advance(prev.nextEpoch())));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BaseStep that = (BaseStep) o;
            return table.equals(that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table);
        }
    }

    public static class PrepareDropAccordTable extends BaseStep
    {
        public PrepareDropAccordTable(TableReference table)
        {
            super(table);
        }

        @Override
        public Kind kind()
        {
            return Kind.PREPARE_DROP_ACCORD_TABLE;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            KeyspaceMetadata ks = prev.schema.getKeyspaces().getNullable(table.keyspace);
            if (ks == null)
                return new Rejected(ExceptionCode.INVALID, "Unknown keyspace " + table.keyspace);
            TableMetadata metadata = ks.tables.getNullable(table.id);
            if (metadata == null)
                return new Rejected(ExceptionCode.INVALID, "Table " + table + " is not known");
            if (!metadata.isAccordEnabled())
                return new Rejected(ExceptionCode.INVALID, "Table " + metadata + " is not an Accord table and should be dropped normally");
            if (metadata.params.pendingDrop)
                return new Rejected(ExceptionCode.INVALID, "Table " + metadata + " is in the process of being dropped");

            metadata = metadata.unbuild().params(metadata.params.unbuild().pendingDrop(true).build()).build();
            ks = ks.withSwapped(ks.tables.withSwapped(metadata));

            ClusterMetadata.Transformer proposed = prev.transformer()
                                                       .with(new DistributedSchema(prev.schema.getKeyspaces().withAddedOrUpdated(ks)))
                                                       .with(prev.inProgressSequences.with(table, new DropAccordTable(table, prev.nextEpoch())));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }
    }

    public static class AwaitAccordTableComplete extends BaseStep
    {
        public AwaitAccordTableComplete(TableReference table)
        {
            super(table);
        }

        @Override
        public Kind kind()
        {
            return AWAIT_ACCORD_TABLE_COMPLETE;
        }
    }

    public static class DropTable extends BaseStep
    {

        public DropTable(TableReference table)
        {
            super(table);
        }

        @Override
        public Kind kind()
        {
            return DROP_ACCORD_TABLE;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            AlterSchema alter = new AlterSchema(new DropTableStatement(table.keyspace, table.name, false, true), Schema.instance);
            Result result = alter.execute(prev);
            if (result.isRejected())
                return result;
            prev = result.success().metadata.forceEpoch(prev.epoch);
            ClusterMetadata.Transformer proposed = prev.transformer()
                                                       .add(result.success().affectedMetadata)
                                                       .with(prev.inProgressSequences.without(table));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }
    }

    private static <T extends BaseStep> AsymmetricMetadataSerializer<Transformation, T> createSerializer(Function<TableReference, T> fn)
    {
        return new AsymmetricMetadataSerializer<>()
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                BaseStep step = (BaseStep) t;
                TableReferenceSerializer.instance.serialize(step.table, out, version);
            }

            @Override
            public T deserialize(DataInputPlus in, Version version) throws IOException
            {
                return fn.apply(TableReferenceSerializer.instance.deserialize(in, version));
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                BaseStep step = (BaseStep) t;
                return TableReferenceSerializer.instance.serializedSize(step.table, version);
            }
        };
    }

    public static final AsymmetricMetadataSerializer<Transformation, PrepareDropAccordTable> PREPARE_DROP_ACCORD_TABLES_SERIALIZER = createSerializer(PrepareDropAccordTable::new);
    public static final AsymmetricMetadataSerializer<Transformation, AwaitAccordTableComplete> AWAIT_ACCORD_TABLE_COMPLETE_SERIALIZER = createSerializer(AwaitAccordTableComplete::new);
    public static final AsymmetricMetadataSerializer<Transformation, DropTable> DROP_TABLE_SERIALIZER = createSerializer(DropTable::new);

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, DropAccordTable>
    {
        public static final Serializer instance = new Serializer();

        @Override
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            DropAccordTable plan = (DropAccordTable) t;

            Epoch.serializer.serialize(plan.latestModification, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            TableReferenceSerializer.instance.serialize(plan.table, out, version);
        }

        @Override
        public DropAccordTable deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            TableReference table = TableReferenceSerializer.instance.deserialize(in, version);
            return new DropAccordTable(table, next, lastModified);
        }

        @Override
        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            DropAccordTable plan = (DropAccordTable) t;
            long size = 0;
            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += VIntCoding.computeVIntSize(plan.kind().ordinal());
            size += TableReferenceSerializer.instance.serializedSize(plan.table, version);
            return size;
        }
    }

    public static class TableReferenceSerializer implements MetadataSerializer<TableReference>
    {
        public static final TableReferenceSerializer instance = new TableReferenceSerializer();

        @Override
        public void serialize(TableReference t, DataOutputPlus out, Version version) throws IOException
        {
            //TODO (now, perf): keyspace/name are for human debugging, should they be serialized?
            t.id.serialize(out);
            out.writeUTF(t.keyspace);
            out.writeUTF(t.name);
        }

        @Override
        public TableReference deserialize(DataInputPlus in, Version version) throws IOException
        {
            TableId id = TableId.deserialize(in);
            String keyspace = in.readUTF();
            String name = in.readUTF();
            return new TableReference(keyspace, name, id);
        }

        @Override
        public long serializedSize(TableReference t, Version version)
        {
            long size = t.id.serializedSize();
            size += TypeSizes.sizeof(t.keyspace);
            size += TypeSizes.sizeof(t.name);
            return size;
        }
    }
}
