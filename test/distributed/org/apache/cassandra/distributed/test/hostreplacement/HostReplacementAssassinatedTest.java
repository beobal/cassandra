package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACEMENT_ALLOWED_GOSSIP_STATUSES;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertGossipInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAll;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HostReplacementAssassinatedTest extends TestBaseImpl
{
    /**
     * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
     * as the status is non-normal.
     *
     * The node is removed gracefully before assassinate, leaving gossip without an empty entry.
     */
    @Test
    public void assassinateGracefullNode() throws IOException
    {
        assassinateTest((cluster, nodeToRemove) -> stopUnchecked(nodeToRemove));
    }

    /**
     * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
     * as the status is non-normal.
     *
     * The node is removed abruptly before assassinate, leaving gossip without an empty entry.
     */
    @Test
    public void assassinateAbruptDownedNode() throws IOException
    {
        assassinateTest((cluster, nodeToRemove) -> stopAbrupt(cluster, nodeToRemove));
    }

    /**
     * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
     * as the status is non-normal.
     *
     * The cluster is put into the "empty" state for the node to remove.
     */
    @Test
    public void assassinatedEmptyNode() throws IOException
    {
        assassinateTest((cluster, nodeToRemove) -> {
            IInvokableInstance seed = cluster.get(1);
            InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

            // now stop all nodes
            stopAll(cluster);

            // with all nodes down, now start the seed (should be first node)
            seed.startup();

            // at this point node2 should be known in gossip, but with generation/version of 0
            assertGossipInfo(seed, addressToReplace, 0, -1);
        });
    }

    private static void assassinateTest(BiConsumer<Cluster, IInvokableInstance> fn) throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 3 || node == 4 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            fn.accept(cluster, nodeToRemove);

            assertRingState(seed, nodeToRemove, "Normal");

            // assassinate the node
            seed.nodetoolResult("assassinate", nodeToRemove.config().broadcastAddress().getAddress().getHostAddress()).asserts().success();

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
