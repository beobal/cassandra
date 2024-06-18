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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PRIVATE_IP_QUERY;
import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PUBLIC_IP_QUERY;
import static org.apache.cassandra.locator.Ec2Snitch.EC2_NAMING_LEGACY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class Ec2SnitchTest
{
    private final SnitchProperties legacySnitchProps = new SnitchProperties()
    {
        public String get(String propertyName, String defaultValue)
        {
            return propertyName.equals("ec2_naming_scheme") ? EC2_NAMING_LEGACY : super.get(propertyName, defaultValue);
        }
    };

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void resetCMS()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void testLegacyRac() throws Exception
    {
        Ec2MetadataServiceConnector connectorMock = mock(Ec2MetadataServiceConnector.class);
        doReturn("us-east-1d").when(connectorMock).apiCall(anyString());
        doReturn(legacySnitchProps).when(connectorMock).getProperties();

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(connectorMock);
        testLegacyRacInternal(locationProvider);

        Ec2Snitch snitch = new Ec2Snitch(connectorMock);
        testLegacyRacInternal(snitch);
    }

    @Test
    public void testMultiregionLegacyRac() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doAnswer((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-1d";
        }).when(spy).apiCall(anyString());

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(spy);
        testLegacyRacInternal(locationProvider);

        Ec2Snitch snitch = new Ec2MultiRegionSnitch(spy);
        testLegacyRacInternal(snitch);
    }

    @Test
    public void testLegacyNewRegions() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doReturn("us-east-2d").when(spy).apiCall(anyString());
        testLegacyNewRegionsInternal(new Ec2LocationProvider(spy));
        testLegacyNewRegionsInternal(new Ec2Snitch(spy));
    }

    @Test
    public void testLegacyMultiRegionNewRegions() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doAnswer((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-2d";
        }).when(spy).apiCall(anyString());

        testLegacyNewRegionsInternal(new Ec2LocationProvider(spy));
        testLegacyNewRegionsInternal(new Ec2MultiRegionSnitch(spy));
    }

    @Test
    public void testFullNamingScheme() throws Exception
    {
        Ec2MetadataServiceConnector connectorMock = mock(Ec2MetadataServiceConnector.class);
        when(connectorMock.apiCall(anyString())).thenReturn("us-east-2d");
        when(connectorMock.getProperties()).thenReturn(new SnitchProperties());

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(connectorMock);
        assertEquals("us-east-2", locationProvider.location.datacenter);
        assertEquals("us-east-2d", locationProvider.location.rack);

        Ec2Snitch snitch = new Ec2Snitch(connectorMock);
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", snitch.getLocalDatacenter());
        assertEquals("us-east-2d", snitch.getLocalRack());

        Ec2MetadataServiceConnector multiRegionConnectorMock = mock(Ec2MetadataServiceConnector.class);
        when(multiRegionConnectorMock.getProperties()).thenReturn(new SnitchProperties());
        when(multiRegionConnectorMock.apiCall(anyString())).then((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-2d";
        });

        Ec2LocationProvider mrLocationProvider = new Ec2LocationProvider(connectorMock);
        assertEquals("us-east-2", mrLocationProvider.location.datacenter);
        assertEquals("us-east-2d", mrLocationProvider.location.rack);

        Ec2MultiRegionSnitch mrSnitch = new Ec2MultiRegionSnitch(multiRegionConnectorMock);
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", mrSnitch.getLocalDatacenter());
        assertEquals("us-east-2d", mrSnitch.getLocalRack());
    }

    @Test
    public void validateDatacenter_RequiresLegacy_CorrectAmazonName()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1");
        assertTrue(Ec2LocationProvider.validate(datacenters, Collections.emptySet(), true));
    }

    @Test
    public void validateDatacenter_RequiresLegacy_LegacyName()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east");
        assertTrue(Ec2LocationProvider.validate(datacenters, Collections.emptySet(), true));
    }

    @Test
    public void validate_RequiresLegacy_HappyPath()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east");
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        assertTrue(Ec2LocationProvider.validate(datacenters, racks, true));
    }

    @Test
    public void validate_RequiresLegacy_HappyPathWithDCSuffix()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east_CUSTOM_SUFFIX");
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        assertTrue(Ec2LocationProvider.validate(datacenters, racks, true));
    }

    @Test
    public void validateRack_RequiresAmazonName_CorrectAmazonName()
    {
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        assertTrue(Ec2LocationProvider.validate(Collections.emptySet(), racks, false));
    }

    @Test
    public void validateRack_RequiresAmazonName_LegacyName()
    {
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        assertFalse(Ec2LocationProvider.validate(Collections.emptySet(), racks, false));
    }

    @Test
    public void validate_RequiresAmazonName_HappyPath()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1");
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        assertTrue(Ec2LocationProvider.validate(datacenters, racks, false));
    }

    @Test
    public void validate_RequiresAmazonName_HappyPathWithDCSuffix()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1_CUSTOM_SUFFIX");
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        assertTrue(Ec2LocationProvider.validate(datacenters, racks, false));
    }

    /**
     * Validate upgrades in legacy mode for regions that didn't change name between the standard and legacy modes.
     */
    @Test
    public void validate_RequiresLegacy_DCValidStandardAndLegacy()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-west-2");
        Set<String> racks = new HashSet<>();
        racks.add("2a");
        racks.add("2b");
        assertTrue(Ec2LocationProvider.validate(datacenters, racks, true));
    }

    /**
     * Check that racks names are enough to detect a mismatch in naming conventions.
     */
    @Test
    public void validate_RequiresLegacy_RackInvalidForLegacy()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-west-2");
        Set<String> racks = new HashSet<>();
        racks.add("us-west-2a");
        assertFalse(Ec2LocationProvider.validate(datacenters, racks, true));
    }

    private void testLegacyRacInternal(Ec2LocationProvider provider) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east", provider.location.datacenter);
        assertEquals("1d", provider.location.rack);
    }

    private void testLegacyRacInternal(Ec2Snitch snitch) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east", snitch.getLocalDatacenter());
        assertEquals("1d", snitch.getLocalRack());
    }

    private void testLegacyNewRegionsInternal(Ec2LocationProvider provider) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", provider.location.datacenter);
        assertEquals("2d", provider.location.rack);
    }

    private void testLegacyNewRegionsInternal(Ec2Snitch snitch) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", snitch.getLocalDatacenter());
        assertEquals("2d", snitch.getLocalRack());
    }
}
