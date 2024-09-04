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

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ExceptionCode;
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
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
        super(nextToIndex(MARK_ACCORD_TABLE_DROPPING), latestModification);
        this.next = MARK_ACCORD_TABLE_DROPPING;
        this.table = table;
        markAccordTableDropping = new MarkAccordTableDropping();
        awaitAccordTableComplete = new AwaitAccordTableComplete();
        dropAccordTable = new DropAccordTable();
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
    public Kind kind()
    {
        return Kind.DROP_ACCORD_TABLE;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public MetadataSerializer<? extends SequenceKey> keySerializer()
    {
        throw new UnsupportedOperationException("TODO");
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
                break;
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
                break;
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
                break;
            default:
                return error(new IllegalStateException("Can't proceed with drop accord table from " + next));
        }
        return continuable();
    }

    @Override
    public Transformation.Result applyTo(ClusterMetadata metadata)
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public DropAccordTableOperation advance(Epoch epoch)
    {
        return new DropAccordTableOperation(this, epoch);
    }

    @Override
    public ProgressBarrier barrier()
    {
        throw new UnsupportedOperationException("TODO");
    }

    public static class TableReference implements SequenceKey
    {
        public final String keyspace, name;
        public final TableId id;

        public TableReference(String keyspace, String name, TableId id)
        {
            this.keyspace = keyspace;
            this.name = name;
            this.id = id;
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

    public static class MarkAccordTableDropping implements Step
    {

        @Override
        public SequenceState execute(ClusterMetadataService cms) throws Exception
        {
            return null;
        }
    }

    public static class AwaitAccordTableComplete implements Step
    {

        @Override
        public SequenceState execute(ClusterMetadataService cms) throws Exception
        {
            return null;
        }
    }

    public static class DropAccordTable implements Step
    {

        @Override
        public SequenceState execute(ClusterMetadataService cms) throws Exception
        {
            return null;
        }
    }
}
