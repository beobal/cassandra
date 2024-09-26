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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.DropAccordTable.TableReference;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_DROP_ACCORD_TABLE;

/**
 *
 */
public class FinishDropAccordTable implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(FinishDropAccordTable.class);

    public static final Serializer serializer = new Serializer();
    public final TableReference tableRef;

    public FinishDropAccordTable(TableReference tableRef)
    {
        this.tableRef = tableRef;
    }

    @Override
    public Kind kind()
    {
        return FINISH_DROP_ACCORD_TABLE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        // Dropping an Accord table is a three step process.
        // 1. Mark the table as pending drop
        // 2. Await all in-flight txns to finish
        // 3. Drop the table from schema (this step)
        // Hypothetically it is possible that after {1} has been committed, but before {3} is executed
        // interleaving metadata changes occur. These could include dropping the table's keyspace, or
        // modifying the transactional mode of the table to make it a non-accord table. Validation
        // exists to prevent these schema changes from being committed while the drop is in-flight.
        // However, if something like this did happen, by the time we come to execute this transformation,
        // there's nothing really to do other than return success (as the table has indeed already been dropped).

        // In every case we remove the operation to drop this table from the set of in-flight sequences
        ClusterMetadata.Transformer proposed = prev.transformer()
                                                   .with(prev.inProgressSequences.without(tableRef));

        Keyspaces keyspaces = prev.schema.getKeyspaces();
        KeyspaceMetadata keyspace = keyspaces.getNullable(tableRef.keyspace);
        // Parent keyspace was dropped
        if (keyspace == null)
        {
            logger.warn("Enclosing keyspace was removed before {} could be dropped", tableRef);
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }

        TableMetadata table = keyspace.getTableNullable(tableRef.name);
        // Table was already dropped
        if (table == null)
        {
            logger.warn("Table {} was dropped while drop accord table sequence was in flight", tableRef);
            return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
        }

        // Actually drop the table
        Keyspaces withoutTable = keyspaces.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.without(table)));
        proposed = proposed.with(new DistributedSchema(withoutTable));
        return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof FinishDropAccordTable)) return false;

        FinishDropAccordTable that = (FinishDropAccordTable) o;

        return Objects.equals(tableRef, that.tableRef);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableRef);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, FinishDropAccordTable>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            FinishDropAccordTable plan = (FinishDropAccordTable) t;
            TableReference.serializer.serialize(plan.tableRef, out, version);
        }

        @Override
        public FinishDropAccordTable deserialize(DataInputPlus in, Version version) throws IOException
        {
            TableReference table = TableReference.serializer.deserialize(in, version);
            return new FinishDropAccordTable(table);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            FinishDropAccordTable plan = (FinishDropAccordTable) t;
            return TableReference.serializer.serializedSize(plan.tableRef, version);
        }
    }
}
