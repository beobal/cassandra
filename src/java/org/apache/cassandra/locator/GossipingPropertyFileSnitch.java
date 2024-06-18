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

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.FBUtilities;


public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    private final Location fromConfig;
    private final boolean preferLocal;
    private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;
    private static final Location DEFAULT_REMOTE = new Location("UNKNOWN_DC", "UNKNOWN_RACK");

    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        SnitchProperties properties = loadConfiguration();

        fromConfig = new Location(properties.get("dc", DEFAULT_REMOTE.datacenter).trim(),
                                  properties.get("rack", DEFAULT_REMOTE.rack).trim());
        preferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
        snitchHelperReference = new AtomicReference<>();
    }

    private static SnitchProperties loadConfiguration() throws ConfigurationException
    {
        final SnitchProperties properties = new SnitchProperties();
        if (!properties.contains("dc") || !properties.contains("rack"))
            throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: " + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        return properties;
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT,
                                                   StorageService.instance.valueFactory.internalAddressAndPort(FBUtilities.getLocalAddressAndPort()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                StorageService.instance.valueFactory.internalIP(FBUtilities.getJustLocalAddress()));

        loadGossiperState();
    }

    private void loadGossiperState()
    {
        assert Gossiper.instance != null;

        ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(DatabaseDescriptor.getLocator(), fromConfig.datacenter, preferLocal);
        Gossiper.instance.register(pendingHelper);

        pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
        if (pendingHelper != null)
            Gossiper.instance.unregister(pendingHelper);
    }

    @Override
    public String getLocalRack()
    {
        return fromConfig.rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return fromConfig.datacenter;
    }
}
