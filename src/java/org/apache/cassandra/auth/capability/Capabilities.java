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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

public class Capabilities
{
    private static final Logger logger = LoggerFactory.getLogger(Capabilities.class);
    private static final Registry registry = new Registry();
    static
    {
        FBUtilities.classForName(System.class.getName(), "System defined capabilities");
    }

    // todo: ensure non-duplication

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

        private static final class SystemCapability extends Capability
        {
            private SystemCapability(String name)
            {
                super(DOMAIN, name);
                logger.trace("Instantiating system CAPABILITY {}", name);
                registry.register(this);
            }
        }
    }

    private static class Registry
    {
        private final Map<String, Map<String, Capability>> capabilities = new ConcurrentHashMap<>();

        public Capability lookup(String domain, String name)
        {
            if (!capabilities.containsKey(domain))
                return null;

            return capabilities.get(domain).get(name);
        }


        private void register(Capability capability)
        {
            logger.trace("Registering CAPABILITY {}", capability);
            capabilities.computeIfAbsent(capability.getDomain(), (s) -> Maps.newConcurrentMap())
                        .put(capability.getName(), capability);
        }
    }
}
