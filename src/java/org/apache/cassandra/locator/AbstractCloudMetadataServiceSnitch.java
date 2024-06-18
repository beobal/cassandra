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

package org.apache.cassandra.locator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.Pair;

abstract class AbstractCloudMetadataServiceSnitch extends AbstractNetworkTopologySnitch
{
    static final Logger logger = LoggerFactory.getLogger(AbstractCloudMetadataServiceSnitch.class);

    protected final CloudMetadataLocationProvider locationProvider;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;

    /**
     * Retained for compatibility with existing subclass implementations.
     * @deprecated
     */
    @Deprecated(since="5.1")
    public AbstractCloudMetadataServiceSnitch(AbstractCloudMetadataServiceConnector connector, Pair<String, String> dcAndRack)
    {
        this(new CloudMetadataLocationProvider(connector, new Location(dcAndRack.left, dcAndRack.right)));
    }

    public AbstractCloudMetadataServiceSnitch(CloudMetadataLocationProvider locationProvider)
    {
        this.locationProvider = locationProvider;
    }

    @Override
    public String getLocalRack()
    {
        return locationProvider.initialLocation().rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return locationProvider.initialLocation().datacenter;
    }
}
