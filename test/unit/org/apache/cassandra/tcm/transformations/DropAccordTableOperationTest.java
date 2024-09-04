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

import java.util.stream.Stream;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.transformations.DropAccordTableOperation.PrepareDropAccordTable;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.qt;

public class DropAccordTableOperationTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    private static final TransactionalMode[] ACCORD_ENABLED_MODES = Stream.of(TransactionalMode.values())
                                                                    .filter(t -> t.accordIsEnabled)
                                                                    .toArray(TransactionalMode[]::new);

    private static final Gen<TableMetadata> TABLE_GEN = Generators.toGen(new TableMetadataBuilder()
                                                                         .withUseCounter(false)
                                                                         .withTransactionalMode(SourceDSL.arbitrary().pick(ACCORD_ENABLED_MODES))
                                                                         .build());

    @Test
    public void e2e()
    {
        qt().check(rs -> {
            ClusterMetadata current = new ClusterMetadata(Murmur3Partitioner.instance);
            TableMetadata metadata = TABLE_GEN.next(rs);
            current = addTable(current, metadata);

            PrepareDropAccordTable prepare = new PrepareDropAccordTable(DropAccordTableOperation.TableReference.from(metadata));

            current = process(current, prepare).metadata;
            Assertions.assertThat(current.inProgressSequences.isEmpty()).isFalse();

            while (current.inProgressSequences.contains(prepare.table))
                current = process(current, current.inProgressSequences.get(prepare.table)).metadata;

            // table is dropped
            Assertions.assertThat(current.schema.getTableMetadata(metadata.id)).isNull();
        });
    }

    private static ClusterMetadata addTable(ClusterMetadata prev, TableMetadata table)
    {
        // first mock out a keyspace
        KeyspaceMetadata schema = KeyspaceMetadata.create(table.keyspace, KeyspaceParams.simple(3));
        schema = schema.withSwapped(schema.tables.with(table));
        Keyspaces keyspaces = prev.schema.getKeyspaces().withAddedOrUpdated(schema);
        return prev.transformer().with(new DistributedSchema(keyspaces)).build().metadata;
    }


    private static Transformation.Success process(ClusterMetadata current, Transformation transformation)
    {
        Transformation.Result result = transformation.execute(current);
        if (result.isRejected())
            throw new IllegalStateException("Unable to make TCM transition: " + result.rejected());
        return result.success();
    }

    private static Transformation.Success process(ClusterMetadata current, MultiStepOperation<?> transformation)
    {
        Transformation.Result result = transformation.applyTo(current);
        if (result.isRejected())
            throw new IllegalStateException("Unable to make TCM transition");
        return result.success();
    }
}