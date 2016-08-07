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

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

public class Capabilities
{
    private static final Logger logger = LoggerFactory.getLogger(Capabilities.class);
    private static final Registry registry = new Registry();
    static
    {
        FBUtilities.classForName(System.class.getName(), "System defined capabilities");
    }

    public static Capability capability(String domain, String name)
    {
        Capability capability = registry.lookup(domain, name);
        if (capability == null)
            throw new IllegalArgumentException(String.format("Unknown capability %s.%s", domain, name));
        return capability;
    }

    public static Capability capability(String fullName)
    {
        int delim = fullName.indexOf('.');
        assert delim > 0;

        String domain = fullName.substring(0, delim);
        String name = fullName.substring(delim + 1);
        return capability(domain, name);
    }

    public static boolean validateForRestriction(Capability capability, IResource resource)
    {
        if (capability.getDomain().equals(System.DOMAIN))
            return resource.validForCapabilityRestriction(capability);
        else
            // defer to ICapabilityManager
            return DatabaseDescriptor.getCapabilityManager().validateForRestriction(capability, resource);
    }

    /**
     * Given a domain and the ordinal value of a Capability within it (which is its position in
     * a BitSet representing a set of required or restricted capabilities), fetch the Capability
     * instance itself from the registry.
     * This is a relatively expensive operation, as the ordinal value of the Capability within
     * the domain is not indexed, so we end up iterating until we find the match. However, this
     * is only used when a client request is missing a required capability to build the error
     * message for the client response and so performance is not critical.
     * @param domain name of the domain the Capability belongs to
     * @param index the ordinal value of the Capability within the domain
     * @return the Capability instance referred to by the domain + ordinal
     */
    public static Capability getByIndex(String domain, int index)
    {
        Capability capability = registry.getByIndex(domain, index);
        assert capability != null : String.format("Unregistered capability found in capability set " +
                                                  "(domain: %s, index: %s", domain, index);
        return capability;
    }

    public static void register(Capability capability)
    {
        registry.register(capability);
    }

    public static final class System
    {
        public static final String DOMAIN = "system";
        public static final Capability FILTERING = new SystemCapability("FILTERING");
        public static final Capability LWT = new SystemCapability("LWT");
        public static final Capability NON_LWT_UPDATE = new SystemCapability("NON_LWT_UPDATE");
        public static final Capability TRUNCATE = new SystemCapability("TRUNCATE");
        public static final Capability MULTI_PARTITION_READ = new SystemCapability("MULTI_PARTITION_READ");
        public static final Capability MULTI_PARTITION_AGGREGATION =  new SystemCapability("MULTI_PARTITION_AGGREGATION");
        public static final Capability PARTITION_RANGE_READ = new SystemCapability("PARTITION_RANGE_READ");
        public static final Capability UNLOGGED_BATCH = new SystemCapability("UNLOGGED_BATCH");
        public static final Capability LOGGED_BATCH = new SystemCapability("LOGGED_BATCH");
        public static final Capability COUNTER_BATCH = new SystemCapability("COUNTER_BATCH");
        public static final Capability NATIVE_SECONDARY_INDEX= new SystemCapability("NATIVE_SECONDARY_INDEX");
        public static final Capability CUSTOM_SECONDARY_INDEX= new SystemCapability("CUSTOM_SECONDARY_INDEX");
        public static final Capability QUERY_TRACING = new SystemCapability("QUERY_TRACING");
        public static final Capability UNPREPARED_STATEMENT = new SystemCapability("UNPREPARED_STATEMENT");

        public static final Capability CL_ANY_WRITE = new SystemCapability("CL_ANY_WRITE");
        public static final Capability CL_ONE_READ = new SystemCapability("CL_ONE_READ");
        public static final Capability CL_ONE_WRITE = new SystemCapability("CL_ONE_WRITE");
        public static final Capability CL_LOCAL_ONE_READ = new SystemCapability("CL_LOCAL_ONE_READ");
        public static final Capability CL_LOCAL_ONE_WRITE = new SystemCapability("CL_LOCAL_ONE_WRITE");
        public static final Capability CL_TWO_READ = new SystemCapability("CL_TWO_READ");
        public static final Capability CL_TWO_WRITE = new SystemCapability("CL_TWO_WRITE");
        public static final Capability CL_THREE_READ = new SystemCapability("CL_THREE_READ");
        public static final Capability CL_THREE_WRITE = new SystemCapability("CL_THREE_WRITE");
        public static final Capability CL_QUORUM_READ = new SystemCapability("CL_QUORUM_READ");
        public static final Capability CL_QUORUM_WRITE = new SystemCapability("CL_QUORUM_WRITE");
        public static final Capability CL_LOCAL_QUORUM_READ = new SystemCapability("CL_LOCAL_QUORUM_READ");
        public static final Capability CL_LOCAL_QUORUM_WRITE = new SystemCapability("CL_LOCAL_QUORUM_WRITE");
        public static final Capability CL_EACH_QUORUM_READ = new SystemCapability("CL_EACH_QUORUM_READ");
        public static final Capability CL_EACH_QUORUM_WRITE = new SystemCapability("CL_EACH_QUORUM_WRITE");
        public static final Capability CL_ALL_READ = new SystemCapability("CL_ALL_READ");
        public static final Capability CL_ALL_WRITE = new SystemCapability("CL_ALL_WRITE");
        public static final Capability CL_SERIAL_READ = new SystemCapability("CL_SERIAL_READ");
        public static final Capability CL_LOCAL_SERIAL_READ = new SystemCapability("CL_LOCAL_SERIAL_READ");

        @VisibleForTesting
        public static final Capability[] ALL =
        {
            FILTERING, LWT, NON_LWT_UPDATE, TRUNCATE, MULTI_PARTITION_READ, MULTI_PARTITION_AGGREGATION,
            PARTITION_RANGE_READ, UNLOGGED_BATCH, LOGGED_BATCH, COUNTER_BATCH, NATIVE_SECONDARY_INDEX,
            CUSTOM_SECONDARY_INDEX, QUERY_TRACING, UNPREPARED_STATEMENT,
            CL_ALL_WRITE, CL_ONE_READ, CL_ONE_WRITE, CL_LOCAL_ONE_READ, CL_LOCAL_ONE_WRITE,
            CL_TWO_READ, CL_TWO_WRITE, CL_THREE_READ, CL_THREE_WRITE, CL_QUORUM_READ, CL_QUORUM_WRITE,
            CL_LOCAL_QUORUM_READ, CL_LOCAL_QUORUM_WRITE, CL_EACH_QUORUM_READ, CL_EACH_QUORUM_WRITE,
            CL_ALL_READ, CL_ALL_WRITE, CL_SERIAL_READ, CL_LOCAL_SERIAL_READ
        };

        private static final class SystemCapability extends Capability
        {
            private SystemCapability(String name)
            {
                super(DOMAIN, name);
                registry.register(this);
                logger.trace("Registered system CAPABILITY {}", this);
            }
        }

        public static Capability forReadConsistencyLevel(ConsistencyLevel cl)
        {
            switch(cl)
            {
                case ALL:
                    return CL_ALL_READ;
                case ONE:
                    return CL_ONE_READ;
                case TWO:
                    return CL_TWO_READ;
                case THREE:
                    return CL_THREE_READ;
                case LOCAL_ONE:
                    return CL_LOCAL_ONE_READ;
                case LOCAL_QUORUM:
                    return CL_LOCAL_QUORUM_READ;
                case EACH_QUORUM:
                    return CL_EACH_QUORUM_READ;
                case QUORUM:
                    return CL_QUORUM_READ;
                case LOCAL_SERIAL:
                    return CL_LOCAL_SERIAL_READ;
                case SERIAL:
                    return CL_SERIAL_READ;
                default:
                    throw new IllegalArgumentException(String.format("Consistency Level %s is not valid for reads", cl));
            }
        }

        public static Capability forWriteConsistencyLevel(ConsistencyLevel cl)
        {
            switch(cl)
            {
                case ALL:
                    return CL_ALL_WRITE;
                case ONE:
                    return CL_ONE_WRITE;
                case TWO:
                    return CL_TWO_WRITE;
                case THREE:
                    return CL_THREE_WRITE;
                case LOCAL_ONE:
                    return CL_LOCAL_ONE_WRITE;
                case LOCAL_QUORUM:
                    return CL_LOCAL_QUORUM_WRITE;
                case EACH_QUORUM:
                    return CL_EACH_QUORUM_WRITE;
                case QUORUM:
                    return CL_QUORUM_WRITE;
                case ANY:
                    return CL_ANY_WRITE;
                default:
                    throw new IllegalArgumentException(String.format("Consistency Level %s is not valid for writes", cl));
            }
        }
    }

    private static class Registry
    {
        private static class DomainRegistry implements Iterable<Capability>
        {
            private final Map<String, Capability> capabilities = Maps.newConcurrentMap();
            private final AtomicInteger counter = new AtomicInteger(0);

            private void put(final Capability capability)
            {
                Capability registered =
                    capabilities.computeIfAbsent(capability.getName(),
                                                 (s) -> capability.withOrdinal(counter.getAndIncrement()));

                // if the capability was previously registered, throw an exception to notify of the duplication
                if (registered != capability)
                    throw new IllegalArgumentException(String.format("Capability %s was already registered", capability));
            }

            private Capability get(String name)
            {
                return capabilities.get(name);
            }

            public Iterator<Capability> iterator()
            {
                return capabilities.values().iterator();
            }
        }

        private final Map<String, DomainRegistry> domainRegistries = new ConcurrentHashMap<>();

        public Capability lookup(String domain, String name)
        {
            String d = domain.toLowerCase(Locale.US);
            if (!domainRegistries.containsKey(d))
                return null;

            return domainRegistries.get(d).get(name.toLowerCase(Locale.US));
        }

        public Capability getByIndex(String domain, int index)
        {
            String d = domain.toLowerCase(Locale.US);
            for (Capability capability : domainRegistries.get(d))
                if (capability.getOrdinal() == index)
                    return capability;

            return null;
        }


        private void register(Capability capability)
        {
            logger.trace("Registering CAPABILITY {}", capability);
            domainRegistries.computeIfAbsent(capability.getDomain(), (s) -> new DomainRegistry())
                            .put(capability);

        }
    }
}
