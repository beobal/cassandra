package org.apache.cassandra.distributed.test.hostreplacement;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;

/**
 * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
 * as the status is non-normal.
 *
 * The node is removed abruptly before assassinate, leaving gossip without an empty entry.
 */
public class AssassinateAbruptDownedNodeTest extends BaseAssassinatedCase
{
    @Override
    void consume(Cluster cluster, IInvokableInstance nodeToRemove)
    {
        stopAbrupt(cluster, nodeToRemove);
    }
}
