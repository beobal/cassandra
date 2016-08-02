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
