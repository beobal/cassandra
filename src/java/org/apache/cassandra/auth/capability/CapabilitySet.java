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
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CapabilitySet
{
    private static final CapabilitySet EMPTY_SET = new CapabilitySet();

    public static final CapabilitySet emptySet()
    {
        return EMPTY_SET;
    }

    private final Map<String, BitSet> domains = new HashMap<>();

    public CapabilitySet(Capability...capabilities)
    {
        this(Arrays.asList(capabilities));
    }

    public CapabilitySet(Iterable<Capability> capabilities)
    {
        for (Capability capability : capabilities)
        {
            assert capability.isRegistered() : String.format("Unregistered capability used in constructing " +
                                                             "capability set: %s", capability.toString());

            BitSet domain = domains.computeIfAbsent(capability.getDomain(), (s) -> new BitSet());
            domain.set(capability.getOrdinal());
        }
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
private static final Logger logger = LoggerFactory.getLogger(Builder.class);
        public CapabilitySet build()
        {
//            logger.info("XXX Set of collected caps: {}", capabilities.stream().map(Capability::toString).collect(Collectors.joining(",")));
            return new CapabilitySet(capabilities);
        }
    }
}
