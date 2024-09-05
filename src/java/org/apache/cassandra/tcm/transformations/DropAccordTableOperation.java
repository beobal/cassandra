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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
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
import static org.apache.cassandra.tcm.Transformation.Kind.MARK_ACCORD_TABLE_DROPPING;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;

public class DropAccordTableOperation extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(DropAccordTableOperation.class);

    public final Transformation.Kind next;
    public final TableReference table;
    public final MarkAccordTableDropping markAccordTableDropping;
    public final AwaitAccordTableComplete awaitAccordTableComplete;
    public final DropAccordTable dropAccordTable;

    protected DropAccordTableOperation(TableReference table, Epoch latestModification)
    {
        this(table, MARK_ACCORD_TABLE_DROPPING, latestModification);
    }

    public DropAccordTableOperation(DropAccordTableOperation current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        table = current.table;
        markAccordTableDropping = current.markAccordTableDropping;
        awaitAccordTableComplete = current.awaitAccordTableComplete;
        dropAccordTable = current.dropAccordTable;
    }

    private DropAccordTableOperation(TableReference table, Transformation.Kind next, Epoch lastModified)
    {
        super(nextToIndex(next), lastModified);
        this.next = next;
        this.table = table;
        markAccordTableDropping = new MarkAccordTableDropping(table);
        awaitAccordTableComplete = new AwaitAccordTableComplete(table);
        dropAccordTable = new DropAccordTable(table);
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case MARK_ACCORD_TABLE_DROPPING:
                return 0;
            case AWAIT_ACCORD_TABLE_COMPLETE:
                return 1;
            case DROP_ACCORD_TABLE:
                return 2;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, Kind.DROP_ACCORD_TABLE));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return MARK_ACCORD_TABLE_DROPPING;
            case 1:
                return AWAIT_ACCORD_TABLE_COMPLETE;
            case 2:
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
        DropAccordTableOperation that = (DropAccordTableOperation) o;
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
            case MARK_ACCORD_TABLE_DROPPING:
                try
                {
                    return markAccordTableDropping.execute(ClusterMetadataService.instance());
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Exception committing mark_accord_table_dropping", t);
                    return continuable();
                }
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
                    return dropAccordTable.execute(ClusterMetadataService.instance());
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
        return dropAccordTable.execute(metadata);
    }

    @Override
    public DropAccordTableOperation advance(Epoch epoch)
    {
        return new DropAccordTableOperation(this, epoch);
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

    public static class PrepareDropAccordTable implements Transformation
    {
        public final TableReference table;

        public PrepareDropAccordTable(TableReference table)
        {
            this.table = table;
        }

        @Override
        public Kind kind()
        {
            return null;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            TableMetadata metadata = prev.schema.getTableMetadata(table.id);
            if (metadata == null)
                return new Rejected(ExceptionCode.INVALID, "Table " + table + " is not known");
            if (!metadata.isAccordEnabled())
                return new Rejected(ExceptionCode.INVALID, "Table " + metadata + " is not an Accord table and should be dropped normally");
            DropAccordTableOperation opt = new DropAccordTableOperation(table, prev.nextEpoch());
            ClusterMetadata.Transformer proposed = prev.transformer()
                                                   .with(prev.inProgressSequences.with(opt.sequenceKey(), opt));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }
    }

    public interface Step
    {
        SequenceState execute(ClusterMetadataService cms) throws Exception;
    }

    public static class BaseStep implements Step, Transformation
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
        public Kind kind()
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            ClusterMetadata.Transformer proposed = prev.transformer()
                                                       .with(prev.inProgressSequences.with(table, plan -> plan.advance(prev.nextEpoch())));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }
    }

    public static class MarkAccordTableDropping extends BaseStep
    {
        public MarkAccordTableDropping(TableReference table)
        {
            super(table);
        }
    }

    public static class AwaitAccordTableComplete extends BaseStep
    {

        public AwaitAccordTableComplete(TableReference table)
        {
            super(table);
        }
    }

    public static class DropAccordTable extends BaseStep
    {

        public DropAccordTable(TableReference table)
        {
            super(table);
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            AlterSchema alter = new AlterSchema(new DropTableStatement(table.keyspace, table.name, false), Schema.instance);
            Result result = alter.execute(prev);
            if (result.isRejected())
                return result;
            prev = result.success().metadata;
            ClusterMetadata.Transformer proposed = prev.transformer()
                                                       .with(prev.inProgressSequences.without(table));
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, DropAccordTableOperation>
    {
        public static final Serializer instance = new Serializer();

        @Override
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            DropAccordTableOperation plan = (DropAccordTableOperation) t;

            Epoch.serializer.serialize(plan.latestModification, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            TableReferenceSerializer.instance.serialize(plan.table, out, version);
        }

        @Override
        public DropAccordTableOperation deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            TableReference table = TableReferenceSerializer.instance.deserialize(in, version);
            return new DropAccordTableOperation(table, next, lastModified);
        }

        @Override
        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            DropAccordTableOperation plan = (DropAccordTableOperation) t;
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
