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

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.RequestExecutionException;

public interface RestrictionHandler
{
    enum Operator
    {
        ADD("+"),
        REMOVE("-");

        String op;
        Operator(String op)
        {
            this.op = op;
        }
    }

    void init();

    CapabilitySet getRestrictions(RoleResource primaryRole, IResource resource);

    ImmutableSet<Restriction> fetch(Restriction.Specification spec, boolean includeInherited);

    void modifyForRole(Restriction restriction, Operator operator) throws RequestExecutionException;

    void removeAllForRole(RoleResource role);

    void removeAllForResource(IResource resource);
}
