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

package org.apache.cassandra.service.reads.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class BlockingReadRepairs
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepairs.class);

    private static final boolean DROP_OVERSIZED_READ_REPAIR_MUTATIONS =
        Boolean.getBoolean("cassandra.drop_oversized_readrepair_mutations");

    /**
     * Returns all of the endpoints that are replicas for the given key. If the consistency level is datacenter
     * local, only the endpoints in the local dc will be returned.
     */
    static ReplicaCollection getCandidateReplicas(Keyspace keyspace, Token token, ConsistencyLevel consistency)
    {
        ReplicaList replicas = StorageProxy.getLiveSortedReplicas(keyspace, token);
        IEndpointSnitch snitch = keyspace.getReplicationStrategy().snitch;
        String localDC = snitch.getDatacenter(FBUtilities.getBroadcastAddressAndPort());

        return consistency.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy
               ? replicas.filter(replica -> snitch.getDatacenter(replica).equals(localDC))
               : replicas;
    }

    /**
     * Create a read repair mutation from the given update, if the mutation is not larger than the maximum
     * mutation size, otherwise return null. Or, if we're configured to be strict, throw an exception.
     */
    public static Mutation createRepairMutation(PartitionUpdate update, ConsistencyLevel consistency, Replica destination, boolean suppressException)
    {
        if (update == null)
            return null;

        DecoratedKey key = update.partitionKey();
        Mutation mutation = new Mutation(update);
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        TableMetadata metadata = update.metadata();

        int messagingVersion = MessagingService.instance().getVersion(destination.getEndpoint());

        int    mutationSize = (int) Mutation.serializer.serializedSize(mutation, messagingVersion);
        int maxMutationSize = DatabaseDescriptor.getMaxMutationSize();


        if (mutationSize <= maxMutationSize)
        {
            return mutation;
        }
        else if (DROP_OVERSIZED_READ_REPAIR_MUTATIONS)
        {
            logger.debug("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                         mutationSize,
                         maxMutationSize,
                         metadata,
                         metadata.partitionKeyType.getString(key.getKey()),
                         destination);
            return null;
        }
        else
        {
            logger.warn("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                        mutationSize,
                        maxMutationSize,
                        metadata,
                        metadata.partitionKeyType.getString(key.getKey()),
                        destination);

            if (!suppressException)
            {
                int blockFor = consistency.blockFor(keyspace);
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
            }
            return null;
        }
    }
}
