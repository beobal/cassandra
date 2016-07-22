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

import java.util.*;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.capability.Capability;
import org.apache.cassandra.auth.capability.Restriction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListRestrictionsStatement extends AuthorizationStatement
{
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = "restrictions"; // virtual cf to use for now.
    private static final List<ColumnSpecification> metadata;

    private final RoleResource role;
    private final IResource resource;
    private final Capability capability;
    private final boolean includeInherited;

    static
    {
        List<ColumnSpecification> columns = new ArrayList<>();
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("role", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("resource", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("capability", true), UTF8Type.instance));
        metadata = Collections.unmodifiableList(columns);
    }

    public ListRestrictionsStatement()
    {
        this(new RoleName(), Restriction.Specification.ANY_RESOURCE, Restriction.Specification.ANY_CAPABILITY, false);
    }

    public ListRestrictionsStatement(RoleName role, IResource resource, Capability capability, boolean includeInherited)
    {
        this.role = role.hasName() ? RoleResource.role(role.getName()) : Restriction.Specification.ANY_ROLE;
        this.resource = resource;
        this.capability = capability;
        this.includeInherited = includeInherited;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // a check to ensure the existence of the user isn't being leaked by user existence check.
        state.ensureNotAnonymous();

        if (role != Restriction.Specification.ANY_ROLE && !DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("Role %s doesn't exist", role.getRoleName()));
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        Restriction.Specification spec = new Restriction.Specification(role, resource, capability);
        Set<Restriction> restrictions = DatabaseDescriptor.getCapabilityManager()
                                                          .listRestrictions(spec, includeInherited);
        ResultSet result = new ResultSet(metadata);
        for (Restriction r : restrictions)
        {
            result.addColumnValue(UTF8Type.instance.decompose(r.getRole().getRoleName()));
            result.addColumnValue(UTF8Type.instance.decompose(formatResourceName(r.getResourceName())));
            result.addColumnValue(UTF8Type.instance.decompose(r.getCapability().getFullName()));
        }
        return new ResultMessage.Rows(result);
    }

    private String formatResourceName(String resourceName)
    {
        // this is an ugly, but until we rework IResources to be properly extendible,
        // it's a simple way to get the user friendly string representation of a
        // resource.
        try
        {
           return Resources.fromName(resourceName).toString();
        }
        catch(IllegalArgumentException e)
        {
            // custom resource types will fall back to the full name
            return resourceName;
        }
    }
}
