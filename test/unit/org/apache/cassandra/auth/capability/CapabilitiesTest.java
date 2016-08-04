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

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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
        fail("Fix me");
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
}
