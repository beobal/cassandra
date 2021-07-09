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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.runAndWaitForLogs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class HostReplacementTest extends TestBaseImpl
{
    static
    {
        // Gossip has a notion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        System.setProperty("cassandra.gossip_quarantine_delay_ms", "0");
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

            // make sure all nodes are healthy and the ring looks like it should
            assertRingIs(node1, node1, node3, node4);
            assertRingState(node1, node4, "Joining");

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
            String logMsg = "Node " + node2.broadcastAddress().getAddress() + " state moving, new token " + moveTo;
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
                    InetAddress target = InetAddress.getByName(peerAddress);
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

    private String getLocalToken(IInvokableInstance inst)
    {
        return inst.callOnInstance(() -> {
            List<String> tokens = new ArrayList<>();
            for (Token t : StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddress()))
                tokens.add(t.getTokenValue().toString());

            assert tokens.size() == 1 : "getLocalToken assumes a single token, but multiple tokens found";
            return tokens.get(0);
        });
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

        public static ListenableFuture<StreamState> bootstrap()
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
}
