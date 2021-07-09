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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.StreamStateStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_QUARANTINE_DELAY;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertInRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getLocalToken;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokenMetadataTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.runAndWaitForLogs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HostReplacementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HostReplacementTest.class);

    static
    {
        // Gossip has a notiion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        GOSSIPER_QUARANTINE_DELAY.setInt(0);
    }

    /**
     * Attempt to do a host replacement on a down host
     */
    @Test
    public void replaceDownedHost() throws IOException
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

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            stopUnchecked(nodeToRemove);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, props -> {
                // since we have a downed host there might be a schema version which is old show up but
                // can't be fetched since the host is down...
                props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });

            // wait till the replacing node is in the ring
            awaitRingJoin(seed, replacingNode);
            awaitRingJoin(replacingNode, seed);

            // make sure all nodes are healthy
            awaitRingHealthy(seed);

            assertRingIs(seed, seed, replacingNode);
            logger.info("Current ring is {}", assertRingIs(replacingNode, seed, replacingNode));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * Attempt to do a host replacement on a alive host
     */
    @Test
    public void replaceAliveHost() throws IOException
    {
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            // now create a new node to replace the other node
            Assertions.assertThatThrownBy(() -> replaceHostAndStart(cluster, nodeToRemove))
                      .as("Startup of instance should have failed as you can not replace a alive node")
                      .hasMessageContaining("Cannot replace a live node")
                      .isInstanceOf(UnsupportedOperationException.class);

            // make sure all nodes are healthy
            awaitRingHealthy(seed);

            assertRingIs(seed, seed, nodeToRemove);
            logger.info("Current ring is {}", assertRingIs(nodeToRemove, seed, nodeToRemove));

            validateRows(seed.coordinator(), expectedState);
            validateRows(nodeToRemove.coordinator(), expectedState);
        }
    }

    /**
     * If the seed goes down, then another node, once the seed comes back, make sure host replacements still work.
     */
    @Test
    public void seedGoesDownBeforeDownHost() throws IOException
    {
        // start with 3 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                      .start())
        {
            // call early as this can't be touched on a down node
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance nodeToStayAlive = cluster.get(3);

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // shutdown the seed, then the node to remove
            stopUnchecked(seed);
            stopUnchecked(nodeToRemove);

            // restart the seed
            seed.startup();

            // make sure the node to remove is still in the ring
            assertInRing(seed, nodeToRemove);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            List<IInvokableInstance> expectedRing = Arrays.asList(seed, replacingNode, nodeToStayAlive);

            // wait till the replacing node is in the ring
            awaitRingJoin(seed, replacingNode);
            awaitRingJoin(replacingNode, seed);
            awaitRingJoin(nodeToStayAlive, replacingNode);

            // make sure all nodes are healthy
            logger.info("Current ring is {}", awaitRingHealthy(seed));

            expectedRing.forEach(i -> assertRingIs(i, expectedRing));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * Successfully perform a replacement on a down node which was previously MOVING.
     * Start with 3 nodes and initiate a token movement for node2 but terminate the instance before it completes.
     * Next, replace node2 and ensure that once the replacement has successfully joined, node1 and node3 clear
     * the moving status and pending ranges for the now dead node2.
     */
    @Test
    public void successfullyReplaceDownAndMovingHost() throws IOException, TimeoutException
    {
        testReplacingDownMovingNode(FailureHelper::installMoveFailureOnly, cluster -> {
            // successfully replace node2 then verify that the ring is as expected
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            stopUnchecked(node2);
            IInvokableInstance node4 = replaceHostAndStart(cluster, node2, properties -> {
                // since there are downed nodes its possible gossip has the downed node with an old schema, so need
                // this property to allow startup
                properties.setProperty("cassandra.skip_schema_check", "true");
            });

            // wait till the replacing node is in the ring
            awaitRingJoin(node1, node4);
            awaitRingJoin(node4, node1);

            // make sure all nodes are healthy and the ring looks like it should
            awaitRingHealthy(node1);
            assertRingIs(node1, node1, node3, node4);
            // expect node1 and node3 to have cleared their pending ranges for node2
            return false;
        });
    }

    /**
     * Fail to perform a replacement on a down node which was previously MOVING.
     * Start with 3 nodes and initiate a token movement for node2 but terminate the instance before it completes.
     * Next, try to replace node2. Should this replacement fail, node1 and node3 should still have cleared
     * the moving status and pending ranges for the now dead node2.
     */
    @Test
    public void failureToReplaceDownAndMovingHost() throws Exception
    {
        testReplacingDownMovingNode(FailureHelper::installMoveAndReplaceFailures, cluster -> {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            stopUnchecked(node2);
            IInvokableInstance node4 = replaceHostAndStart(cluster, node2, properties -> {
                // since there are downed nodes its possible gossip has the downed node with an old schema, so need
                // this property to allow startup
                properties.setProperty("cassandra.skip_schema_check", "true");
            });

            assertFalse(node4.logs()
                             .grep("Some data streaming failed. Use nodetool to check bootstrap state and resume")
                             .getResult()
                             .isEmpty());

            // wait till the replacing node is visible in the ring
            awaitRingState(node1, node4, "Joining");
            // expect node1 and node3 to have cleared their pending ranges for node2
            return false;
        });
    }

    private void testReplacingDownMovingNode(BiConsumer<ClassLoader, Integer> instanceInitializer,
                                             Function<Cluster, Boolean> stopAndReplace) throws IOException, TimeoutException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                      .withInstanceInitializer(instanceInitializer)
                                      .start())
        {
            setupCluster(cluster, 2);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            // initiate a move for node2, which will not complete due to the
            // ByteBuddy interceptor we injected. Wait for the other two nodes
            // to mark node2 as moving before proceeding.
            long t2 = Long.parseLong(getLocalToken(node2));
            long t3 = Long.parseLong(getLocalToken(node3));
            long moveTo = t2 + ((t3 - t2)/2);
            String logMsg = "Node /127.0.0.2:7012 state moving, new token " + moveTo;
            runAndWaitForLogs(cluster,
                              () -> node2.nodetoolResult("move", "--", Long.toString(moveTo)).asserts().failure(),
                              logMsg);

            // node1 & node3 should now consider some ranges pending for node2
            assertPendingRangesForPeer(true, "127.0.0.2", node1, node3);

            // kill node2 and replace it. Note that if the node died without a graceful shutdown
            // the replacement will fail as long as cassandra.consistent.rangemovement is set to true
            // because the new node will learn a MOVING status for it from the remaining peers. A
            // graceful shutdown causes that status to be replaced with SHUTDOWN, but prior to
            // CASSANDRA-xxx this doesn't update TokenMetadata on those peers, so they maintain
            // pending ranges for the down node, even after it has been removed from the ring.
            boolean expectPendingRanges = stopAndReplace.apply(cluster);

            // should node1 & node3 consider any ranges as still pending for node2
            assertPendingRangesForPeer(expectPendingRanges, "127.0.0.2", node1, node3);
        }
    }

    void assertPendingRangesForPeer(final boolean expectPending, final String peerAddress, IInvokableInstance...instances)
    {
        for (IInvokableInstance inst : instances)
        {
            String i = inst.broadcastAddress().getHostString();
            boolean hasPending = inst.callOnInstance(
            () -> {
                try
                {
                    InetAddressAndPort target = InetAddressAndPort.getByName(peerAddress);
                    boolean isMoving = StorageService.instance.getTokenMetadata()
                                                              .getMovingEndpoints()
                                                              .stream()
                                                              .map(pair -> pair.right)
                                                              .anyMatch(target::equals);

                    return isMoving && !StorageService.instance.getTokenMetadata()
                                                               .getPendingRanges(KEYSPACE, target)
                                                               .isEmpty();

                }
                catch (UnknownHostException e)
                {
                    Assertions.fail("Unable to resolve host", e);
                    // make the compiler happy
                    return false;
                }
            });
            assertEquals(String.format("%s should %shave PENDING RANGES for %s", i, expectPending ? "" : "not ", peerAddress),
                         hasPending, expectPending);
        }
    }

    public static class FailureHelper
    {
        static void installMoveFailureOnly(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(StreamPlan.class)
                               .method(named("execute"))
                               .intercept(MethodDelegation.to(FailureHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        static void installMoveAndReplaceFailures(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(StreamPlan.class)
                               .method(named("execute"))
                               .intercept(MethodDelegation.to(FailureHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
            if (nodeNumber == 4)
            {
                new ByteBuddy().redefine(BootStrapper.class)
                               .method(named("bootstrap"))
                               .intercept(MethodDelegation.to(FailureHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static StreamResultFuture execute()
        {
            throw new RuntimeException("failing to execute move");
        }

        public static ListenableFuture<StreamState> bootstrap(StreamStateStore store, boolean strict)
        {
            return Futures.immediateFailedFuture(new RuntimeException("failing to execute replace"));
        }
    }

    static void setupCluster(Cluster cluster, int replicationFactor)
    {
        fixDistributedSchemas(cluster);
        init(cluster, replicationFactor);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    static void setupCluster(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        init(cluster);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    static void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    static void validateRows(ICoordinator coordinator, SimpleQueryResult expected)
    {
        expected.reset();
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
        AssertUtils.assertRows(rows, expected);
    }
}
