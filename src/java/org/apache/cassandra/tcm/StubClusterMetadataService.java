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

package org.apache.cassandra.tcm;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;

import static org.apache.cassandra.schema.DistributedMetadataLogKeyspace.Log;

public class StubClusterMetadataService extends ClusterMetadataService
{

    public static StubClusterMetadataService forClientTools()
    {
        KeyspaceMetadata ks = KeyspaceMetadata.create(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                      new KeyspaceParams(true,
                                                                         ReplicationParams.local().asMeta()),
                                                      Tables.of(Log));
        return new StubClusterMetadataService(new ClusterMetadata(DatabaseDescriptor.getPartitioner(),
                                                                  Directory.EMPTY,
                                                                  new DistributedSchema(Keyspaces.of(ks))));
    }

    public static StubClusterMetadataService forTesting()
    {
        return new StubClusterMetadataService(new ClusterMetadata(DatabaseDescriptor.getPartitioner()));
    }

    public static StubClusterMetadataService forTesting(ClusterMetadata metadata)
    {
        return new StubClusterMetadataService(metadata);
    }

    private ClusterMetadata metadata;

    private StubClusterMetadataService(ClusterMetadata initial)
    {
        super(new UniformRangePlacement(),
              MetadataSnapshots.NO_OP,
              LocalLog.sync(initial, LogStorage.None, false, false),
              new StubProcessor(),
              Commit.Replicator.NO_OP,
              false);
        this.metadata = initial;
    }

    @Override
    public <T1> T1 commit(Transformation transform, CommitSuccessHandler<T1> onSuccess, CommitFailureHandler<T1> onFailure)
    {
        Transformation.Result result = transform.execute(metadata);
        if (result.isSuccess())
        {
            metadata = result.success().metadata;
            return  onSuccess.accept(result.success().metadata);
        }
        return onFailure.accept(result.rejected().code, result.rejected().reason);
    }

    @Override
    public ClusterMetadata fetchLogFromCMS(Epoch awaitAtLeast)
    {
        return metadata;
    }

    @Override
    public ClusterMetadata metadata()
    {
        return metadata;
    }

    public void setMetadata(ClusterMetadata metadata)
    {
        this.metadata = metadata;
    }

    private static class StubProcessor implements Processor
    {

        private StubProcessor() {}

        @Override
        public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry.Deadline retryPolicy)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy)
        {
            throw new UnsupportedOperationException();
        }
    }
}
