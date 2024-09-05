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
import java.util.stream.Stream;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.transformations.DropAccordTable.PrepareDropAccordTable;
import org.apache.cassandra.tcm.transformations.DropAccordTable.TableReference;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.qt;

public class DropAccordTableTest
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
                                                                         .withPartitioner(Murmur3Partitioner.instance)
                                                                         .withTransactionalMode(SourceDSL.arbitrary().pick(ACCORD_ENABLED_MODES))
                                                                         .build());

    @Test
    public void e2e()
    {
        qt().check(rs -> {
            MockClusterMetadataService cms = new MockClusterMetadataService();
            TableMetadata metadata = TABLE_GEN.next(rs);
            addTable(cms, metadata); // hack this table into the schema...

            TableReference table = TableReference.from(metadata);

            process(cms, new PrepareDropAccordTable(table));

            // This is only here because "applyTo" is not touched without it...
            for (KeyspaceMetadata ks : cms.metadata().schema.getKeyspaces())
                cms.metadata().writePlacementAllSettled(ks);

            Assertions.assertThat(cms.metadata().inProgressSequences.isEmpty()).isFalse();
            InProgressSequences.finishInProgressSequences(table);
            Assertions.assertThat(cms.metadata().inProgressSequences.isEmpty()).isTrue();

            // table is dropped
            Assertions.assertThat(cms.metadata().schema.getTableMetadata(metadata.id)).isNull();
        });
    }

    private static void addTable(MockClusterMetadataService cms, TableMetadata table)
    {
        class Ref { Types types;}
        // first mock out a keyspace
        ClusterMetadata prev = cms.metadata();
        KeyspaceMetadata schema = KeyspaceMetadata.create(table.keyspace, KeyspaceParams.simple(3));
        Ref ref = new Ref();
        ref.types = schema.types;
        CassandraGenerators.visitUDTs(table, udt -> ref.types = ref.types.with(udt.unfreeze()));
        schema = schema.withSwapped(ref.types);
        schema = schema.withSwapped(schema.tables.with(table));
        Keyspaces keyspaces = prev.schema.getKeyspaces().withAddedOrUpdated(schema);
        ClusterMetadata metadata = prev.transformer().with(new DistributedSchema(keyspaces)).build().metadata;
        cms.setMetadata(metadata);
    }


    private static void process(ClusterMetadataService cms, Transformation transformation)
    {
        cms.commit(transformation);
    }

    private static class MockClusterMetadataService extends StubClusterMetadataService
    {
        private static final DataOutputBuffer output = new DataOutputBuffer();

        public MockClusterMetadataService()
        {
            super(new ClusterMetadata(Murmur3Partitioner.instance));

            ClusterMetadataService.unsetInstance();
            ClusterMetadataService.setInstance(this);
        }

        @Override
        public <T1> T1 commit(Transformation transform, CommitSuccessHandler<T1> onSuccess, CommitFailureHandler<T1> onFailure)
        {
            try
            {
                AsymmetricMetadataSerializers.testSerde(output, transform.kind().serializer(), transform, NodeVersion.CURRENT_METADATA_VERSION);
            }
            catch (IOException e)
            {
                throw new AssertionError(transform.toString(), e);
            }

            return super.commit(transform, onSuccess, onFailure);
        }

        @Override
        public void setMetadata(ClusterMetadata metadata)
        {
            try
            {
                AsymmetricMetadataSerializers.testSerde(output, ClusterMetadata.serializer, metadata, NodeVersion.CURRENT_METADATA_VERSION);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            super.setMetadata(metadata);
        }
    }
}