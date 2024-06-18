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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

// TODO convert to Locator & InitialLocationProvider - currently this is being handled like any other
//  legacy snitch, by using SnitchAdapter to provide access via those new interfaces. This is valid and
//  useful as it exercises the migration path, but we do also need to verify the new methods of configuration
public class DistributedTestSnitch extends AbstractNetworkTopologySnitch
{
    private static NetworkTopology mapping = null;
    private static final Map<InetAddressAndPort, InetSocketAddress> cache = new ConcurrentHashMap<>();
    private static final Map<InetSocketAddress, InetAddressAndPort> cacheInverse = new ConcurrentHashMap<>();

    public static InetAddressAndPort toCassandraInetAddressAndPort(InetSocketAddress addressAndPort)
    {
        InetAddressAndPort m = cacheInverse.get(addressAndPort);
        if (m == null)
        {
            m = InetAddressAndPort.getByAddressOverrideDefaults(addressAndPort.getAddress(), addressAndPort.getPort());
            cache.put(m, addressAndPort);
        }
        return m;
    }

    public static InetSocketAddress fromCassandraInetAddressAndPort(InetAddressAndPort addressAndPort)
    {
        InetSocketAddress m = cache.get(addressAndPort);
        if (m == null)
        {
            m = NetworkTopology.addressAndPort(addressAndPort.getAddress(), addressAndPort.getPort());
            cache.put(addressAndPort, m);
        }
        return m;
    }

    @Override
    public String getLocalRack()
    {
        return mapping.localRack(FBUtilities.getBroadcastAddressAndPort());
    }

    @Override
    public String getLocalDatacenter()
    {
        return mapping.localDC(FBUtilities.getBroadcastAddressAndPort());
    }

    static void assign(NetworkTopology newMapping)
    {
        mapping = new NetworkTopology(newMapping);
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT,
                                                   StorageService.instance.valueFactory.internalAddressAndPort(FBUtilities.getLocalAddressAndPort()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                                                   StorageService.instance.valueFactory.internalIP(FBUtilities.getJustLocalAddress()));
    }
}
