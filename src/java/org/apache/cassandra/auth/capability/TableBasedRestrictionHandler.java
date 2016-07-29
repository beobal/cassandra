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

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class TableBasedRestrictionHandler implements RestrictionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TableBasedRestrictionHandler.class);

    private final AuthLookupTableSupport lookup;

    public TableBasedRestrictionHandler()
    {
        this(new AuthLookupTableSupport<>(AuthKeyspace.ROLE_CAP_RESTRICTIONS,
                                          "role",
                                          AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
                                          "resource",
                                          RoleResource::getName,
                                          IResource::getName));
    }

    @VisibleForTesting
    protected TableBasedRestrictionHandler(AuthLookupTableSupport lookup)
    {
        this.lookup = lookup;
    }

    public void modifyForRole(Restriction restriction, Operator operator)
    throws RequestExecutionException
    {
        // Adds or remove entry in the primary restrictions table
        process(String.format("UPDATE %s.%s SET capabilities = capabilities %s {'%s'} WHERE role = '%s' AND resource = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_CAP_RESTRICTIONS,
                              operator.op,
                              restriction.getCapability().getFullName(),
                              escape(restriction.getRole().getName()),
                              escape(restriction.getResourceName())));

        if (operator == Operator.ADD)
            lookup.addLookupEntry(restriction.getRole(), Resources.fromName(restriction.getResourceName()));
        else
            lookup.removeLookupEntry(restriction.getRole(), Resources.fromName(restriction.getResourceName()));
        // update the resource lookup table
//        String indexUpdateTemplate = (operator.equals("+"))
//                                     ? "INSERT INTO %s.%s (resource, role) VALUES ('%s','%s')"
//                                     : "DELETE FROM %s.%s WHERE resource = '%s' AND role = '%s'";
//        process(String.format(indexUpdateTemplate,
//                              AuthKeyspace.NAME,
//                              AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
//                              escape(restriction.getResourceName()),
//                              escape(restriction.getRole().getName())));
    }

    public void removeAllForRole(RoleResource role)
    {
        lookup.removeAllEntriesForPrimaryKey(role);
//        try
//        {
//            UntypedResultSet rows = process(String.format("SELECT resource FROM %s.%s WHERE role = '%s'",
//                                                          AuthKeyspace.NAME,
//                                                          AuthKeyspace.ROLE_CAP_RESTRICTIONS,
//                                                          escape(role.getName())));
//
//            List<ModificationStatement> statements = new ArrayList<>();
//            for (UntypedResultSet.Row row : rows)
//            {
//                statements.add((ModificationStatement)QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s' AND role = '%s'",
//                                                          AuthKeyspace.NAME,
//                                                          AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
//                                                          escape(row.getString("resource")),
//                                                          escape(role.getName())),
//                                            ClientState.forInternalCalls()).statement);
//
//            }
//
//            statements.add((ModificationStatement)QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s'",
//                                                                     AuthKeyspace.NAME,
//                                                                     AuthKeyspace.ROLE_CAP_RESTRICTIONS,
//                                                                     escape(role.getName())),
//                                                       ClientState.forInternalCalls()).statement);
//
//            executeAsBatch(statements);
//        }
//        catch (RequestExecutionException | RequestValidationException e)
//        {
//            logger.warn("Failed to revoke all restrictions of {}: {}", role.getRoleName(), e);
//        }
    }

    public void removeAllForResource(IResource resource)
    {
        lookup.removeAllEntriesForLookupKey(resource);
//        try
//        {
//            UntypedResultSet rows = process(String.format("SELECT role FROM %s.%s WHERE resource = '%s'",
//                                                          AuthKeyspace.NAME,
//                                                          AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
//                                                          escape(resource.getName())));
//
//            List<ModificationStatement> statements = new ArrayList<>();
//            for (UntypedResultSet.Row row : rows)
//            {
//                statements.add((ModificationStatement)QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
//                                                                         AuthKeyspace.NAME,
//                                                                         AuthKeyspace.ROLE_CAP_RESTRICTIONS,
//                                                                         escape(row.getString("role")),
//                                                                         escape(resource.getName())),
//                                                           ClientState.forInternalCalls()).statement);
//            }
//
//            statements.add((ModificationStatement)QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s'",
//                                                                          AuthKeyspace.NAME,
//                                                                          AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
//                                                                          escape(resource.getName())),
//                                                            ClientState.forInternalCalls()).statement);
//
//            executeAsBatch(statements);
//        }
//        catch (RequestExecutionException | RequestValidationException e)
//        {
//            logger.warn("Failed to revoke all restrictions on {}: {}", resource, e);
//        }
    }

    public ImmutableSet<Restriction> fetch(Restriction.Specification spec, boolean includeInherited)
    {
        if (spec.isAnyRole() && spec.isAnyResource())
            return readAllRestrictions(spec);

        // for queries where the resource is bound but the role is not, like
        // LIST RESTRICTIONS ON ANY ROLE USING ANY CAPABILITY WITH <resource>;
        // use the inverted index table to identify roles with any restriction
        // on the applicable resource, then read those from the primary table
        if (spec.isAnyRole() && !spec.isAnyResource())
            return readRestrictionsByResource(spec);

        // Spec must be bound to a specific role. If we're interested in the
        // full set of effective restrictions, expand the query to include
        // all roles granted to, or inherited by, the one specified
        Set<RoleResource> roles = !includeInherited
                                  ? Collections.singleton(spec.getRole())
                                  : Roles.getRoles(spec.getRole());

        // If including restrictions inherited from granted roles, modify the
        // spec to filter more permissively. Roles will be limited by the
        // query itself, so only valid roles will be retrieved anyway
        Collector collector = !includeInherited
                              ? new Collector(spec)
                              : new Collector(spec.withUpdatedRole(Restriction.Specification.ANY_ROLE));

        if (spec.isAnyResource())
            roles.forEach(role -> collector.processRows(selectByRole(role)));
        else
            roles.forEach(role -> collector.processRows(selectByRoleAndResource(role, spec.getResource())));

        return collector.getRestrictions();
    }

//    protected void executeAsBatch(List<ModificationStatement> statements)
//    throws RequestExecutionException, RequestValidationException
//    {
//        BatchStatement batch = new BatchStatement(0, BatchStatement.Type.LOGGED, statements, Attributes.none());
//        QueryProcessor.instance.processBatch(batch,
//                                             QueryState.forInternalCalls(),
//                                             BatchQueryOptions.withoutPerStatementVariables(QueryOptions.DEFAULT));
//
//    }

    private ImmutableSet<Restriction> readRestrictionsByResource(Restriction.Specification spec)
    {
        Collector collector = new Collector(spec);
        UntypedResultSet rolesWithRestriction = lookup.lookup(spec.getResource());
        rolesWithRestriction.forEach(row -> {
            RoleResource role = RoleResource.fromName(row.getString("role"));
            collector.processRows(selectByRoleAndResource(role, spec.getResource()));
        });
        return collector.getRestrictions();
    }

    private ImmutableSet<Restriction> readAllRestrictions(Restriction.Specification spec)
    {
        Collector collector = new Collector(spec);
        collector.processRows(selectAll());
        return collector.getRestrictions();
    }

    private UntypedResultSet selectAll()
    {
        return process(String.format("SELECT role, resource, capabilities FROM %s.%s",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.ROLE_CAP_RESTRICTIONS));
    }

    private UntypedResultSet selectByRole(RoleResource role)
    {
        return process(String.format("SELECT role, resource, capabilities FROM %s.%s WHERE role = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.ROLE_CAP_RESTRICTIONS,
                                     escape(role.getName())));
    }

    private UntypedResultSet selectByRoleAndResource(RoleResource role, IResource resource)
    {
        return process(String.format("SELECT role, resource, capabilities " +
                                     "FROM %s.%s " +
                                     "WHERE role = '%s' AND resource = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.ROLE_CAP_RESTRICTIONS,
                                     role.getName(),
                                     resource.getName()));
    }

//    private UntypedResultSet lookupRolesForResource(IResource resource)
//    {
//        return process(String.format("SELECT role FROM %s.%s WHERE resource = '%s'",
//                                     AuthKeyspace.NAME,
//                                     AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
//                                     resource.getName()));
//    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    protected UntypedResultSet process(String query) throws RequestExecutionException
    {
        return QueryProcessor.process(query, ConsistencyLevel.LOCAL_ONE);
    }

    private static final class Collector
    {
        private final Restriction.Specification spec;
        private final ImmutableSet.Builder<Restriction> builder;
        private Collector(Restriction.Specification spec)
        {
            this.spec = spec;
            this.builder = ImmutableSet.builder();
        }

        private void processRows(UntypedResultSet results)
        {
            results.forEach(this::processRow);
        }

        private void processRow(UntypedResultSet.Row row)
        {
            // we only deal with resource names, not IResource instances here as
            // as we don't necessarily have a factory method in the Resources helper
            // class for every implementation. Capabilities on the other hand, must
            // be registered during initialization, so we can safely assume that we
            // can get a handle to an instance, regardless of the implementation.
            RoleResource role = RoleResource.fromName(row.getString("role"));
            String resource = row.getString("resource");
            row.getSet("capabilities", UTF8Type.instance).forEach(capability -> maybeCollect(role, resource, capability));
        }

        private void maybeCollect(RoleResource role, String resource, String capabilityName)
        {
            Capability capability = Capabilities.capability(capabilityName);
            if (spec.matches(role, resource, capability))
                builder.add(new Restriction(role, resource, capability));
        }

        private ImmutableSet<Restriction> getRestrictions()
        {
            return builder.build();
        }
    }
}
