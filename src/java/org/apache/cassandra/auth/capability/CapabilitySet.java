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

import java.util.*;

import com.google.common.base.Objects;

/**
 * Capabilities are namespaced by their domain, and when registered (in Capabilities::register)
 * a per-domain counter is incremented and assigned to the Capability. This allows Capabilities
 * for a given domain to be represented as a BitSet, where a set bit represents a restricted
 * capability. CapabilitySet is essentially a map of domain->bitset and is used to represents
 * capabilities as they relate to a specific user & resource.
 *
 * For instance, if the role 'db_user' is the subject of a restriction which prevents
 * ALLOW FILTERING queries to be run on 'table_foo', then the bitset mapped to the 'system'
 * domain will have the corresponding bit set. This CapabilitySet is obtained from
 * ICapabilityManager::getRestricted and may be cached for the client session in
 * AuthenticatedUser.restrictionCache.
 *
 * To continue this example, during the execution of a query, a CapabilitySet representing the
 * capabilities required to execute the statement is constructed. This is then compared to the
 * compared to the user's restricted capabilites for the target table. Any intersection indicates
 * that a required capability is restricted for that user and results in an AuthenticationException.
 * This comparison takes into account both the hierarchy of granted roles (so the restrictions for
 * all roles granted to the authenticated user are checked) and the resource hierarchy (so
 * restrictions defined against a specific table, its enclosing keyspace and all keyspaces are
 * also checked).
 */
public class CapabilitySet implements Iterable<Capability>
{
    private static final CapabilitySet EMPTY_SET = new CapabilitySet();

    public static CapabilitySet emptySet()
    {
        return EMPTY_SET;
    }

    private final Map<String, BitSet> domains = new HashMap<>();

    public CapabilitySet(Capability...capabilities)
    {
        this(Arrays.asList(capabilities));
    }

    private CapabilitySet(Iterable<Capability> capabilities)
    {
        for (Capability capability : capabilities)
        {
            assert capability.isRegistered() : String.format("Unregistered capability used in constructing " +
                                                             "capability set: %s", capability.toString());

            BitSet domain = domains.computeIfAbsent(capability.getDomain(), (s) -> new BitSet());
            domain.set(capability.getOrdinal());
        }
    }

    private CapabilitySet(Map<String, BitSet> domains)
    {
        this.domains.putAll(domains);
    }

    /**
     * Return a union of the supplied CapabilitySets, by first identifying all named domains
     * represented in them, then for each ORing together all present BitSets for each domain.
     *
     * @param sets A list of CapabilitySets to combine into a single instance.
     * @return A new CapabilitySet representing the logical union of the list provided
     */
    public static CapabilitySet union(List<CapabilitySet> sets)
    {
        Map<String, BitSet> union = new HashMap<>();

        // First, initialize the aggregate domains from all the CapabilitySets
        for (CapabilitySet capSet : sets)
        {
            for (String domain : capSet.domains.keySet())
            {
                union.putIfAbsent(domain, new BitSet());
            }
        }

        // For each domain, OR each bitset that we have for that domain
        for (Map.Entry<String, BitSet> unionedDomain : union.entrySet())
        {
            BitSet unionedBits = unionedDomain.getValue();
            for (CapabilitySet capSet : sets)
            {
                BitSet caps = capSet.domains.get(unionedDomain.getKey());
                if (caps != null)
                {
                    unionedBits.or(caps);
                }
            }
        }
        return new CapabilitySet(union);
    }

    public boolean isEmpty()
    {
        for (BitSet domain : domains.values())
            if (domain.cardinality() != 0)
                return false;

        return true;
    }

    /**
     * Return the intersection of a supplied CapabilitySet with this one.
     * Used when checking whether any capabilities required to perform a
     * particular operation are included in the restricted set for a given
     * role. There, this instance is the set of required capabilities and
     * the argument represents the set of restricted capabilites. The return
     * value is a set containing those capabilities which are in both sets.
     * @param other CapabilitySet to intersect with
     * @return Set of Capability instances representing the intersection
     */
    public Set<Capability> intersection(CapabilitySet other)
    {
        Set<Capability> intersection = null;
        for (Map.Entry<String, BitSet> entry : domains.entrySet())
        {
            BitSet otherDomain = other.domains.get(entry.getKey());
            // there are no capabilities of this domain in other
            if (otherDomain == null)
                continue;

            // there are capabilites of this domain in other, but none of the ones specified here
            if (! entry.getValue().intersects(otherDomain) )
                continue;

            // at least one of the capabilities for this domain is also set in other, figure out which
            BitSet thisDomain = entry.getValue();
            for ( int i = thisDomain.nextSetBit(0); i >= 0; i = thisDomain.nextSetBit(i + 1) )
            {
                if (otherDomain.get(i))
                {
                    if (intersection == null)
                    {
                        intersection = new HashSet<>();
                    }
                    intersection.add(Capabilities.getByIndex(entry.getKey(), i));
                }
            }
        }
        return intersection == null ? Collections.emptySet() : intersection;
    }

    public Iterator<Capability> iterator()
    {
        List<Capability> list = new ArrayList<>();
        for (Map.Entry<String, BitSet> entry : domains.entrySet())
        {
            BitSet thisDomain = entry.getValue();
            for ( int i = thisDomain.nextSetBit(0); i >= 0; i = thisDomain.nextSetBit(i + 1) )
                list.add(Capabilities.getByIndex(entry.getKey(), i));
        }
        return list.iterator();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder("CapabilitySet [");
        for (Map.Entry<String, BitSet> entry : domains.entrySet())
        {
            BitSet thisDomain = entry.getValue();
            for ( int i = thisDomain.nextSetBit(0); i >= 0; i = thisDomain.nextSetBit(i + 1) )
                builder.append(Capabilities.getByIndex(entry.getKey(), i).toString());
        }
        builder.append("]");
        return builder.toString();
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CapabilitySet))
            return false;

        CapabilitySet capSet = (CapabilitySet)o;

        return Objects.equal(this.domains, capSet.domains);
    }

    public int hashCode()
    {
        return Objects.hashCode(domains);
    }

    public static final class Builder
    {
        List<Capability> capabilities = new ArrayList<>();

        public Builder add(Capability capability)
        {
            capabilities.add(capability);
            return this;
        }
        public CapabilitySet build()
        {
            return new CapabilitySet(capabilities);
        }
    }
}
