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

import java.util.Locale;

import com.google.common.base.Objects;

/**
 * Base class for Capability implementations.
 *
 * Before use, any Capability instance must be registered by calling
 * {@link org.apache.cassandra.auth.capability.Capabilities#register(Capability)}.
 * This makes it available for use in DCL statements (CREATE/DROP RESTRICTION) and enables
 * it to be used in execution paths which include third party extensions, such as in custom
 * index or trigger implementations.
 *
 * A key part of the registration process is to assign an ordinal to the instance. This is
 * simply an auto-incrementing counter, maintained per capability domain by the system's
 * capability registry. It allows a capability domain to be represented as a bitset, with
 * set bits indicating either required or restricted capabilities, dependent on context. See
 * {@link org.apache.cassandra.auth.capability.CapabilitySet} for details of this usage.
 *
 * As these ordinals are purely node-local and never shared between nodes, or otherwise
 * serialized, the exact assignment of ordinal values is not meaningful, as long as each
 * Capability instance is immutable.
 *
 * For this reason, {@link org.apache.cassandra.auth.capability.ICapabilityManager}
 * implementations must not store ordinal values, which should not be necessary anyway as
 * a {@link org.apache.cassandra.auth.capability.Capability}'s full name uniquely identifies it.
 */
public abstract class Capability
{
    private final String domain;
    private final String name;
    private final String fullName;
    private int ordinal = -1;

    protected Capability(String domain, String name)
    {
        assert domain.indexOf('.') == -1 : "Capability domain must not include '.'";
        assert name.indexOf('.') == -1 : "Capability name must not include '.'";

        this.domain = domain.toLowerCase(Locale.US);
        this.name = name.toLowerCase(Locale.US);
        this.fullName = this.domain + '.' + this.name;
    }

    Capability withOrdinal(int ordinal)
    {
        assert this.ordinal == -1 : String.format("Attempted to remap an already assigned ordinal for capability %s. " +
                               "Is this capability being registered multiple times?", this.fullName);
        this.ordinal = ordinal;
        return this;
    }

    public boolean isRegistered()
    {
        return ordinal >= 0;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public String getFullName()
    {
        return fullName;
    }

    public String getDomain()
    {
        return domain;
    }

    public String getName()
    {
        return name;
    }

    public String toString()
    {
        return String.format("<%s>", fullName);
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof Capability))
            return false;

        Capability cap = (Capability) o;

        return Objects.equal(domain, cap.domain)
               && Objects.equal(name, cap.name)
               && Objects.equal(ordinal, cap.ordinal);
    }

    public int hashCode()
    {
        return Objects.hashCode(domain, name);
    }


}
