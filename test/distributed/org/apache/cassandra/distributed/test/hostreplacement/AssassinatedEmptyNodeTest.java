package org.apache.cassandra.distributed.test.hostreplacement;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.shared.ClusterUtils.assertGossipInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAll;

/**
 * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
 * as the status is non-normal.
 *
 * The cluster is put into the "empty" state for the node to remove.
 */
public class AssassinatedEmptyNodeTest extends BaseAssassinatedCase
{
    @Override
    void consume(Cluster cluster, IInvokableInstance nodeToRemove)
    {
        IInvokableInstance seed = cluster.get(1);
        IInvokableInstance peer = cluster.get(3);
        InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

        // now stop all nodes
        stopAll(cluster);

        // with all nodes down, now start the seed (should be first node)
        seed.startup();
        peer.startup();

        // at this point node2 should be known in gossip, but with generation/version of 0
        assertGossipInfo(seed, addressToReplace, 0, -1);
        assertGossipInfo(peer, addressToReplace, 0, -1);
    }
}
