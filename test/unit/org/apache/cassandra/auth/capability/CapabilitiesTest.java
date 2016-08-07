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

package org.apache.cassandra.auth.capability;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.Util.setDatabaseDescriptorField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CapabilitiesTest
{
    @Test
    public void systemCapabilitiesAreRegisteredByDefault()
    {
        for (Capability cap : Capabilities.System.ALL)
            assertSame(cap, Capabilities.capability(cap.getFullName()));
    }

    @Test
    public void customCapabilitiesCanBeRegistered()
    {
        String domain = randomDomain();
        Capability custom = randomCapability(domain);
        try
        {
            Capabilities.capability(custom.getDomain(), custom.getName());
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        Capabilities.register(custom);
        assertSame(custom, Capabilities.capability(custom.getDomain(), custom.getName()));
    }

    @Test
    public void registeringCapabilitySetsOrdinalForDomain()
    {
        String domain = randomDomain();
        Capability foo = randomCapability(domain);
        Capability bar = randomCapability(domain);
        assertEquals(-1, foo.getOrdinal());
        assertEquals(-1, bar.getOrdinal());

        Capabilities.register(bar);
        Capabilities.register(foo);
        assertEquals(0, bar.getOrdinal());
        assertEquals(1, foo.getOrdinal());
    }

    @Test
    public void lookupMethodsAreEquivalent()
    {
        assertSame(Capabilities.capability(Capabilities.System.DOMAIN, "LWT"),
                   Capabilities.capability("system.lwt"));
    }

    @Test
    public void nameIsNotCaseSensitive()
    {
        assertEquals(new Capability("domain", "name"){},
                     new Capability("DOMAIN", "NAME"){});
    }

    @Test
    public void testNamespacing()
    {
        assertFalse(new Capability("domain_a", "same_name"){}.equals(new Capability("domain_b", "same_name"){}));
    }

    @Test
    public void getCapabilityByDomainOrdinal()
    {
        String domain = randomDomain();
        Capability foo = randomCapability(domain);
        Capability bar = randomCapability(domain);

        Capabilities.register(foo);
        Capabilities.register(bar);

        assertSame(foo, Capabilities.getByIndex(domain, foo.getOrdinal()));
        assertSame(bar, Capabilities.getByIndex(domain, bar.getOrdinal()));
        try
        {
            Capabilities.getByIndex(domain, 99);
            fail("Expected an exception");
        }
        catch (AssertionError e)
        {
            // expected
        }
    }

    @Test
    public void registerDuplicates()
    {
        String domain = randomDomain();
        Capability foo = randomCapability(domain);
        Capability bar = new TestCapability(domain, foo.getName());

        Capabilities.register(foo);
        try
        {
            Capabilities.register(bar);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void domainAndNameMustNotContainPeriod()
    {
        try
        {
            new TestCapability("some.domain", "a");
            fail("Expected an error");
        }
        catch (AssertionError e)
        {
            // expected
        }

        try
        {
            new TestCapability(randomDomain(), "some.name");
            fail("Expected an error");
        }
        catch (AssertionError e)
        {
            // expected
        }
    }

    @Test
    public void testConsistencyLevelMappings()
    {
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            try
            {
                Capability readCapability = Capabilities.System.forReadConsistencyLevel(cl);
                assertEquals(String.format("CL_%s_READ", cl.name()).toUpperCase(),
                             readCapability.getName().toUpperCase());
            }
            catch(IllegalArgumentException e)
            {
                // only CL_ANY is not valid for reads
                assertEquals(cl, ConsistencyLevel.ANY);
            }

            try
            {
                Capability writeCapability = Capabilities.System.forWriteConsistencyLevel(cl);
                assertEquals(String.format("CL_%s_WRITE", cl.name()).toUpperCase(),
                             writeCapability.getName().toUpperCase());
            }
            catch(IllegalArgumentException e)
            {
                // SERIAL & LOCAL_SERIAL are not valid for writes
                assertTrue(cl == ConsistencyLevel.SERIAL || cl == ConsistencyLevel.LOCAL_SERIAL);
            }
        }
    }

    @Test
    public void testValidationOfCapabilityAndResource()
    {
        // For built-in capabilties, a resource itself can answer whether or not a given
        // capability can be meaningfully applied to it. In the first instance, only
        // DataResources are supported, so any other IResource impl should be considered
        // incompatible with any system capability

        IResource allData = DataResource.root();
        IResource keyspace = DataResource.keyspace("ks");
        IResource table = DataResource.table("ks", "t1");
        IResource allRoles = RoleResource.root();
        IResource role = RoleResource.role("role1");
        IResource allFunctions = FunctionResource.root();
        IResource ksFunctions = FunctionResource.keyspace("ks");
        IResource function = FunctionResource.function("ks", "fun1", Collections.emptyList());
        IResource allMbeans = JMXResource.root();
        IResource mbean = JMXResource.mbean("org.apache.cassandra.db:type=Tables,keyspace=ks,table=t1");
        IResource[] dataResources = new IResource[] { allData, keyspace, table };
        IResource[] nonDataResources = new IResource[] { allRoles, role,
                                                         allFunctions, ksFunctions, function,
                                                         allMbeans, mbean };

        StubCapabilityManager manager = new StubCapabilityManager();
        setDatabaseDescriptorField("capabilityManager", manager);
        assertEquals(0, manager.delegatedCallCount);

        for (Capability capability : Capabilities.System.ALL)
        {
            for (IResource resource : dataResources)
            {
                assertTrue(Capabilities.validateForRestriction(capability, resource));
            }

            for (IResource resource : nonDataResources)
            {
                assertFalse(Capabilities.validateForRestriction(capability, resource));
            }
        }
        // none of those calls should have been delegated to the capability manager
        assertEquals(0, manager.delegatedCallCount);

        // For custom capabilities, we delegate to the configured ICapabilityManager. The
        // default (CassandraCapabilityManager) simply re-directs the call back to the
        // IResource, so to use custom capabilities, a custom manager is required.

        Capability customCapability = randomCapability(randomDomain());
        Stream.of(dataResources, nonDataResources)
              .flatMap(Stream::of)
              .forEach(r -> Capabilities.validateForRestriction(customCapability, r));

        assertEquals(dataResources.length + nonDataResources.length, manager.delegatedCallCount);
    }

    private String randomDomain()
    {
        return UUID.randomUUID().toString();
    }

    private Capability randomCapability(String domain)
    {
        return new TestCapability(domain, UUID.randomUUID().toString());
    }

    private static class TestCapability extends Capability
    {
        protected TestCapability(String domain, String name)
        {
            super(domain, name);
        }
    }

    private static class StubCapabilityManager implements ICapabilityManager
    {
        public void createRestriction(AuthenticatedUser performedBy, Restriction restriction)
        {
            throw new UnsupportedOperationException();
        }

        public void dropRestriction(AuthenticatedUser performedBy, Restriction restriction)
        {
            throw new UnsupportedOperationException();
        }

        public void dropAllRestrictionsOn(RoleResource role)
        {
            throw new UnsupportedOperationException();
        }

        public void dropAllRestrictionsWith(IResource resource)
        {
            throw new UnsupportedOperationException();

        }

        public ImmutableSet<Restriction> listRestrictions(Restriction.Specification specification, boolean includeInherited)
        {
            return ImmutableSet.of();
        }

        public CapabilitySet getRestricted(RoleResource primaryRole, IResource resource)
        {
            return CapabilitySet.emptySet();
        }

        public int delegatedCallCount = 0;
        public boolean validateForRestriction(Capability capability, IResource resource)
        {
            delegatedCallCount++;
            return false;
        }

        public boolean enforcesRestrictions()
        {
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public void setup()
        {
        }
    }
}
