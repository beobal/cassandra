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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACEMENT_ALLOWED_GOSSIP_STATUSES;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitGossipStatus;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseAssassinatedCase extends TestBaseImpl
{
    abstract void consume(Cluster cluster, IInvokableInstance nodeToRemove);

    @Test
    public void test() throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 || node == 5 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance peer = cluster.get(3);

            setupCluster(cluster);

            consume(cluster, nodeToRemove);

            assertRingState(seed, nodeToRemove, "Normal");

            // assassinate the node
            peer.nodetoolResult("assassinate", nodeToRemove.config().broadcastAddress().getAddress().getHostAddress()).asserts().success();

            // wait until the peer sees this assassination
            awaitGossipStatus(seed, nodeToRemove, "LEFT");

            // attempt to replace the node should fail as the status is LEFT
            assertThatThrownBy(() -> replaceHostAndStart(cluster, nodeToRemove))
            .hasMessage("Cannot replace_address /127.0.0.2:7012 because it's status is not in [NORMAL, shutdown], status is LEFT");

            // allow replacing nodes with the LEFT state, this should fail since the token isn't in the ring
            assertThatThrownBy(() ->
                               replaceHostAndStart(cluster, nodeToRemove, properties -> {
                                   // since there are downed nodes its possible gossip has the downed node with an old schema, so need
                                   // this property to allow startup
                                   properties.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                                   // since the bootstrap should fail because the token, don't wait "too long" on schema as it doesn't
                                   // matter for this test
                                   properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, 10);

                                   // allow LEFT status to be replaced
                                   properties.set(REPLACEMENT_ALLOWED_GOSSIP_STATUSES, "NORMAL", "shutdown", "LEFT");
                               })).hasMessage("Cannot replace token " + getTokens(nodeToRemove).get(0) + " which does not exist!");
        }
    }
}
