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
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;

public class Restriction
{
    private final RoleResource role;
    private final String resource;
    private final Capability capability;

    public Restriction(RoleResource role, String resource, Capability capability)
    {
        this.role = role;
        this.resource = resource;
        this.capability = capability;
    }

    public Capability getCapability()
    {
        return capability;
    }

    public String getResourceName()
    {
        return resource;
    }

    public RoleResource getRole()
    {
        return role;
    }

    public String toString()
    {
        return String.format("Restriction OF %s ON %s USING %s", capability, role, resource);
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (! (o instanceof Restriction))
            return false;

        Restriction r = (Restriction) o;
        return Objects.equal(capability, r.capability)
               && Objects.equal(resource, r.resource)
               && Objects.equal(role, r.role);
    }

    public int hashCode()
    {
        return Objects.hashCode(capability, resource, role);
    }

    public static final class Specification
    {
        public static final RoleResource ANY_ROLE = RoleResource.role("");
        public static final IResource ANY_RESOURCE = new AnyResource();
        public static Capability ANY_CAPABILITY = new AnyCapability();

        private final RoleResource role;
        private final IResource resource;
        private final Capability capability;

        public Specification(RoleResource role, IResource resource, Capability capability)
        {
            this.role = role;
            this.resource = resource;
            this.capability = capability;
        }

        public boolean matches(RoleResource role, String resourceName, Capability capability)
        {
            if (this.role != ANY_ROLE && !this.role.equals(role))
                return false;

            if (this.resource != ANY_RESOURCE && !this.resource.getName().equals(resourceName))
                return false;

            if (this.capability != ANY_CAPABILITY && !this.capability.equals(capability))
                return false;

            return true;
        }

        public boolean isAnyRole()
        {
            return role == ANY_ROLE;
        }

        public RoleResource getRole()
        {
            return role;
        }

        public boolean isAnyResource()
        {
            return resource == ANY_RESOURCE;
        }

        public IResource getResource()
        {
            return resource;
        }

        public Specification withUpdatedRole(RoleResource role)
        {
            return new Specification(role, this.resource, this.capability);
        }

        public String toString()
        {
            return String.format("[role: %s, resource: %s, capability: %s]",
                                 role == ANY_ROLE ? "ANY ROLE" : role.getName(),
                                 resource == ANY_RESOURCE ? "ANY RESOURCE" : resource.getName(),
                                 capability == ANY_CAPABILITY ? "ANY CAPABILITY" : capability.getFullName());
        }

        private static final class AnyResource implements IResource
        {
            public String getName()
            {
                return "";
            }

            public IResource getParent()
            {
                return null;
            }

            public boolean hasParent()
            {
                return false;
            }

            public boolean exists()
            {
                return false;
            }

            public Set<Permission> applicablePermissions()
            {
                return Collections.emptySet();
            }
        }

        private static final class AnyCapability extends Capability
        {
            private AnyCapability()
            {
                super("", "");
            }
        }
    }
}
