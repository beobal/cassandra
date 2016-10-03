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

/**
 * Responsible for managing blacklists of restricted cabilities by role and resource.
 */
public interface ICapabilityManager
{
    /**
     * Define a new restriction, called during execution of a CREATE RESTRICTION
     * statement.
     *
     * @param performedBy primary role of the authenticated client issuing the
     *                    create restriction statement.
     * @param restriction the restriction to create.
     */
    void createRestriction(AuthenticatedUser performedBy, Restriction restriction);

    /**
     * Remove a restriction, called during execution of a DROP RESTRICTION statement.
     *
     * @param performedBy primary role of the authenticated client issuing the drop
     *                    restriction statement.
     * @param restriction the restriction to drop.
     */
    void dropRestriction(AuthenticatedUser performedBy, Restriction restriction);

    /**
     * Called when a role is dropped, removes all restrictions which reference it.
     * @param role The role being dropped.
     */
    void dropAllRestrictionsOn(RoleResource role);

    /**
     * Called when an {@link org.apache.cassandra.auth.IResource} is dropped from the system (i.e. for
     * a {@link org.apache.cassandra.auth.DataResource}, when
     * the table or keyspace is dropped). Removes all restrictions which reference
     * the dropped resource.
     * @param resource The resource being removed.
     */
    void dropAllRestrictionsWith(IResource resource);

    /**
     * Query method to list all restrictions which match a given set of criteria.
     *
     * The query criteria are represented by a specification param, which may contain
     * either a specific role, resource and capability, or wildcards for any of them.
     * The includeInherited param denotes whether to include only restrcitions created
     * directly on the specified role (if the specification contains one), or if the
     * roles hierarchy should be considered. In the latter case, the results will include
     * matching restrictions on any role granted to the role from the specification.
     *
     * So the broadest query that can be asked is ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY
     * and the most specific will include a named role, resource and capability and will
     * not includeInherited.
     *
     * @param specification query params representing role, resource and capability, any of
     *                      which may be a wildcard.
     * @param includeInherited whether to consider the fully resolved set of granted roles
     *                         or include only those restrictions directly referencing
     *                         the named role (if not a wildcard).
     * @return set of restrictions matching the query criteria. May be empty but should never
     * be null.
     */
    ImmutableSet<Restriction> listRestrictions(Restriction.Specification specification, boolean includeInherited);

    /**
     * Returns a representation of the capabilities which have been blacklisted for the combination
     * of the named role and resource. The set of restricted capabilites may be empty, but must
     * never be null.
     * @param primaryRole the role for which the restrictions are defined.
     * @param resource the resource for which the restrictions are defined.
     * @return CapabilitySet containing the capabilites from any restriction defined for the
     * named role and resource. May be empty, but should never be null.
     */
    CapabilitySet getRestricted(RoleResource primaryRole, IResource resource);

    /**
     * Checks whether a capability and resource are a valid combination to be used in
     * a restriction. The system defined capabilities only apply to DataResources (so
     * for instance, a restriction cannot be created using a
     * {@link org.apache.cassandra.auth.JMXResource},
     * {@link org.apache.cassandra.auth.FunctionResource}
     * or {@link org.apache.cassandra.auth.RoleResource} currently).
     * This method is used when custom Capabilities are used, as the system has to verify whether
     * that capability can be related to the resource in question.
     * @param capability to check
     * @param resource to check
     * @return True if the capability can be used in conjunction with the resource to
     *         define a restriction, false otherwise.
     */
    boolean validateForRestriction(Capability capability, IResource resource);

    /**
     * Whether or not the implementation allows restrictions to be created, modified
     * and dropped, and whether defined restrictions are actually enforced. If false,
     * no restrictions will actually be applied to user operations.
     * @return True if the implementation enforces restrictions, false otherwise.
     */
    boolean enforcesRestrictions();

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
