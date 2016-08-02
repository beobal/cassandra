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

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.auth.capability.Capabilities.System.*;
import static org.junit.Assert.fail;

public class CapabilitySetTest
{
    private static final TestCapability CAP_A = new TestCapability("a");
    private static final TestCapability CAP_B = new TestCapability("b");
    private static final TestCapability CAP_C = new TestCapability("c");

    static
    {
        Capabilities.register(CAP_A);
        Capabilities.register(CAP_B);
        Capabilities.register(CAP_C);
    }

    @Test
    public void testIsEmpty()
    {
        assertTrue(new CapabilitySet().isEmpty());
        assertTrue(CapabilitySet.emptySet().isEmpty());
        assertFalse(new CapabilitySet(FILTERING).isEmpty());
    }

    @Test
    public void testIntersection()
    {
        // 2 empty sets
        assertTrue(CapabilitySet.emptySet()
                                .intersection(CapabilitySet.emptySet())
                                .isEmpty());

        // single domain, same capabilities
        assertEquals(Collections.singleton(LWT),
                     new CapabilitySet(LWT).intersection(new CapabilitySet(LWT)));

        // single domain, disjoint capabilities
        assertEquals(Collections.emptySet(),
                     new CapabilitySet(LWT, FILTERING).intersection(new CapabilitySet(TRUNCATE, CL_ALL_WRITE)));

        // single domain, intersecting capabilities
        assertEquals(Sets.newHashSet(LWT, CL_ALL_WRITE),
                     new CapabilitySet(LWT, FILTERING, CL_ALL_WRITE).intersection(new CapabilitySet(CL_ALL_WRITE, TRUNCATE, LWT)));

        // 1 empty, 1 single domain
        assertEquals(Collections.emptySet(),
                     CapabilitySet.emptySet().intersection(new CapabilitySet(LWT, TRUNCATE)));
        assertEquals(Collections.emptySet(),
                     new CapabilitySet(LWT, TRUNCATE).intersection(CapabilitySet.emptySet()));

        // 2 disjoint single domains
        assertEquals(Collections.emptySet(),
                     new CapabilitySet(LWT, TRUNCATE).intersection(new CapabilitySet(CAP_A, CAP_B)));

        // multiple domains, all disjoint capabilities
        assertEquals(Collections.emptySet(),
                     new CapabilitySet(LWT, TRUNCATE, CAP_A).intersection(new CapabilitySet(FILTERING, CAP_B)));

        // multiple domains, intersecting in 1 domain
        assertEquals(Sets.newHashSet(CAP_A),
                     new CapabilitySet(LWT, CAP_C, CAP_A, TRUNCATE).intersection(new CapabilitySet(CAP_A, FILTERING, CAP_B)));

        // multiple identical domains
        assertEquals(Sets.newHashSet(CAP_A, CAP_C, LWT, TRUNCATE),
                     new CapabilitySet(TRUNCATE, CAP_C, LWT, CAP_A).intersection(new CapabilitySet(LWT, TRUNCATE, CAP_A, CAP_C)));
    }

    @Test
    public void testEquality()
    {
        // empty sets
        assertSame(CapabilitySet.emptySet(), CapabilitySet.emptySet());
        assertEquals(CapabilitySet.emptySet(), new CapabilitySet());

        // single domain, same capabilities
        assertEquals(new CapabilitySet(LWT), new CapabilitySet(LWT));

        // single domain, disjoint capabilities
        assertFalse(new CapabilitySet(LWT, FILTERING).equals(new CapabilitySet(TRUNCATE, CL_ALL_WRITE)));

        // single domain, intersecting capabilities
        assertFalse(new CapabilitySet(LWT, FILTERING, CL_ALL_WRITE).equals(new CapabilitySet(CL_ALL_WRITE, TRUNCATE, LWT)));

        // 1 empty, 1 single domain
        assertFalse(CapabilitySet.emptySet().equals(new CapabilitySet(LWT, TRUNCATE)));
        assertFalse(new CapabilitySet(LWT, TRUNCATE).equals(CapabilitySet.emptySet()));

        // 2 disjoint single domains
        assertFalse(new CapabilitySet(LWT, TRUNCATE).equals(new CapabilitySet(CAP_A, CAP_B)));

        // multiple domains, all disjoint capabilities
        assertFalse(new CapabilitySet(LWT, TRUNCATE, CAP_A).equals(new CapabilitySet(FILTERING, CAP_B)));

        // multiple domains, intersecting in 1 domain
        assertFalse(new CapabilitySet(LWT, CAP_C, CAP_A, TRUNCATE).equals(new CapabilitySet(CAP_A, FILTERING, CAP_B)));

        // multiple identical domains
        assertEquals(new CapabilitySet(TRUNCATE, CAP_C, LWT, CAP_A), new CapabilitySet(LWT, TRUNCATE, CAP_A, CAP_C));
    }

    @Test
    public void supplyingUnregisteredCapabilityErrors()
    {
        try
        {
            new CapabilitySet(new TestCapability("unregistered"));
            fail("Expected an assert error");
        }
        catch (AssertionError e)
        {
            // expected
        }
    }

    private static class TestCapability extends Capability
    {
        private static final String domain = "TESTING";
        protected TestCapability(String name)
        {
            super(domain, name);
        }
    }
}
