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
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_QUARANTINE_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACEMENT_ALLOW_EMPTY;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertGossipInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertNotInRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitHealthyRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitJoinRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokenMetadataTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAll;

public class HostReplacementOfDowedClusterTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HostReplacementOfDowedClusterTest.class);

    static
    {
        // by default empty replace isn't allowed, so enable for these tests
        REPLACEMENT_ALLOW_EMPTY.setBoolean(true);

        // Gossip has a notiion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        GOSSIPER_QUARANTINE_DELAY.setInt(0);
    }

    /**
     * When the full cluster crashes, make sure that we can replace a dead node after recovery.  This can happen
     * with DC outages (assuming single DC setup) where the recovery isn't able to recover a specific node.
     */
    @Test
    public void hostReplacementOfDeadNode() throws IOException
    {
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // now stop all nodes
            stopAll(cluster);

            // with all nodes down, now start the seed (should be first node)
            seed.startup();

            // at this point node2 should be known in gossip, but with generation/version of 0
            assertGossipInfo(seed, addressToReplace, 0, -1);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            awaitJoinRing(seed, replacingNode);
            awaitJoinRing(replacingNode, seed);
            assertNotInRing(seed, nodeToRemove);
            logger.info("Current ring is {}", assertNotInRing(replacingNode, nodeToRemove));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * Cluster stops completely, then start seed, then host replace node2; after all complete start node3 to make sure
     * it comes up correctly with the new host in the ring.
     */
    @Test
    public void hostReplacementOfDeadNodeAndOtherNodeStartsAfter() throws IOException
    {
        // start with 3 nodes, stop both nodes, start the seed, host replace the down node)
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes);
        try (Cluster cluster = Cluster.build(numStartNodes)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == (numStartNodes + 1) ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance nodeToStartAfterReplace = cluster.get(3);
            InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // now stop all nodes
            stopAll(cluster);

            // with all nodes down, now start the seed (should be first node)
            seed.startup();

            // at this point node2 should be known in gossip, but with generation/version of 0
            assertGossipInfo(seed, addressToReplace, 0, -1);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            // wait till the replacing node is in the ring
            awaitJoinRing(seed, replacingNode);
            awaitJoinRing(replacingNode, seed);

            // we see that the replaced node is properly in the ring, now lets add the other node back
            nodeToStartAfterReplace.startup();

            awaitJoinRing(seed, nodeToStartAfterReplace);
            awaitJoinRing(replacingNode, nodeToStartAfterReplace);

            // make sure all nodes are healthy
            awaitHealthyRing(seed);

            assertRingIs(seed, seed, replacingNode, nodeToStartAfterReplace);
            assertRingIs(replacingNode, seed, replacingNode, nodeToStartAfterReplace);
            logger.info("Current ring is {}", assertRingIs(nodeToStartAfterReplace, seed, replacingNode, nodeToStartAfterReplace));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
     * as the status is non-normal.
     */
    @Test
    public void hostReplacementOfAssassinatedNodeFails() throws IOException
    {
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // now stop all nodes
            stopAll(cluster);

            // with all nodes down, now start the seed (should be first node)
            seed.startup();

            // at this point node2 should be known in gossip, but with generation/version of 0
            assertGossipInfo(seed, addressToReplace, 0, -1);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            seed.nodetoolResult("assassinate", nodeToRemove.config().broadcastAddress().getAddress().getHostAddress()).asserts().success();

            // now create a new node to replace the other node
            Assertions.assertThatThrownBy(() -> replaceHostAndStart(cluster, nodeToRemove))
                      .hasMessage("Cannot replace_address /127.0.0.2:7012 because it's status is not in [NORMAL, shutdown], status is LEFT");
        }
    }

    private void setupCluster(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        init(cluster);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    void validateRows(ICoordinator coordinator, SimpleQueryResult expected)
    {
        expected.reset();
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.QUORUM);
        AssertUtils.assertRows(rows, expected);
    }
}
