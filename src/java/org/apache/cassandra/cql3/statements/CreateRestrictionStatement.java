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

package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.capability.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateRestrictionStatement extends AuthorizationStatement
{
    private final Capability capability;
    private final RoleResource role;
    private IResource resource;
    private final boolean ifNotExists;

    public CreateRestrictionStatement(Capability capability, RoleName roleName, IResource resource, boolean ifNotExists)
    {
        this.capability = capability;
        this.role = RoleResource.role(roleName.getName());
        this.resource = resource;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        try
        {
            state.ensureHasPermission(Permission.AUTHORIZE, resource);
            state.ensureHasPermission(Permission.AUTHORIZE, role);
        }
        catch (UnauthorizedException e)
        {
            // Catch and rethrow with a more friendly message
            throw new UnauthorizedException(String.format("User %s does not have sufficient privileges " +
                                                          "to perform the requested operation",
                                                          state.getUser().getName()));
        }
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        if (!DatabaseDescriptor.getCapabilityManager().enforcesRestrictions())
            throw new InvalidRequestException("The configured capability manager does not support restrictions");

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("Role %s doesn't exist", role.getRoleName()));

        resource = maybeCorrectResource(resource, state);

        Restriction.Specification spec = new Restriction.Specification(role, resource, capability);
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        if (!ifNotExists && !capabilityManager.listRestrictions(spec, false).isEmpty())
            throw new InvalidRequestException(String.format("%s already exists", spec.toString()));

        if (!Capabilities.validateForRestriction(capability, resource))
            throw new InvalidRequestException(String.format("%s cannot be used in restrictions with %s",
                                                            capability,
                                                            resource));

        // Special case for QUERY_TRACING capability as this can only be applied to ALL KEYSPACES
        // at the moment. The reason for this is that currently, a tracing session may be started
        // before the query has been parsed, so at the point when the restrictions are checked, we
        // don't yet know which keyspace/table is being interacted with.
        if (capability.equals(Capabilities.System.QUERY_TRACING) && ! resource.equals(DataResource.root()))
            throw new InvalidRequestException(String.format("Restriction of %s may only be applied to ALL KEYSPACES"));
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        Restriction.Specification spec = new Restriction.Specification(role, resource, capability);
        if (ifNotExists && !DatabaseDescriptor.getCapabilityManager()
                                               .listRestrictions(spec, false).isEmpty())
            return null;

        DatabaseDescriptor.getCapabilityManager()
                          .createRestriction(state.getUser(), new Restriction(role, resource.getName(), capability));
        return null;
    }
}
