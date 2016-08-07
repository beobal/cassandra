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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface ICapabilityManager
{
    void createRestriction(AuthenticatedUser performedBy, Restriction restriction);

    void dropRestriction(AuthenticatedUser performedBy, Restriction restriction);

    void dropAllRestrictionsOn(RoleResource role);

    void dropAllRestrictionsWith(IResource resource);

    ImmutableSet<Restriction> listRestrictions(Restriction.Specification specification, boolean includeInherited);

    CapabilitySet getRestricted(RoleResource primaryRole, IResource resource);

    boolean validateForRestriction(Capability capability, IResource resource);

    /**
     * Hook to perform validation of an implementation's configuration (if supported).
     *
     * @throws ConfigurationException
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Hook to perform implementation specific initialization, called once upon system startup.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();
}
