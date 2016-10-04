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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.setDatabaseDescriptorField;
import static org.apache.cassandra.auth.capability.Restriction.Specification.ANY_CAPABILITY;
import static org.apache.cassandra.auth.capability.Restriction.Specification.ANY_RESOURCE;
import static org.apache.cassandra.auth.capability.Restriction.Specification.ANY_ROLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraCapabilityManagerTest
{

    private static final String KEYSPACE_NAME = "cassandra_capability_manager_test";
    private static final String TABLE_NAME = "test_table";
    private static final DataResource KEYSPACE = DataResource.keyspace(KEYSPACE_NAME);
    private static final DataResource TABLE = DataResource.table(KEYSPACE_NAME, TABLE_NAME);
    private static final RoleResource ROLE_A = RoleResource.role("role_a");
    private static final RoleResource ROLE_B = RoleResource.role("role_b");
    private static final RoleResource ROLE_C = RoleResource.role("role_c");
    private static final RoleResource[] ROLES = { ROLE_A, ROLE_B, ROLE_C };

    // Test helpers - these classes simply extend the internal IRoleManager & ICapabilityManager
    // implementations to ensure that all access to underlying tables is made via
    // QueryProcessor.executeOnceInternal/CQLStatement.executeInternal and not StorageProxy
    // so that they can be used in a unit test environment.

    private static class LocalExecutionRoleManager extends CassandraRoleManager
    {
        protected UntypedResultSet process(String query, ConsistencyLevel cl)
        {
            return QueryProcessor.executeOnceInternal(query);
        }

        protected UntypedResultSet process(SelectStatement statement, String role)
        {
            return UntypedResultSet.create(
                statement.executeInternal(QueryState.forInternalCalls(),
                                          QueryOptions.forInternalCalls(Collections.singletonList(ByteBufferUtil.bytes(role)))).result);
        }

        protected void scheduleSetupTask(final Callable<Void> setupTask)
        {
            // skip data migration or setting up default role for tests
        }
    }

    private static class LocalExecutionTableHandler extends TableBasedRestrictionHandler
    {
        LocalExecutionTableHandler()
        {
            super(new LocalExecutionLookupSupport<>(AuthKeyspace.ROLE_CAP_RESTRICTIONS,
                                                  "role",
                                                  AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
                                                  "resource",
                                                  RoleResource::getName,
                                                  IResource::getName));
        }

        protected UntypedResultSet process(String query) throws RequestExecutionException
        {
            return QueryProcessor.executeOnceInternal(query);
        }
    }

    private static class LocalExecutionLookupSupport<P, L> extends AuthLookupTableSupport<P, L>
    {
        public LocalExecutionLookupSupport(String primaryTable,
                                           String primaryKeyName,
                                           String lookupTable,
                                           String lookupKeyName,
                                           Function<P, String> primaryAsString,
                                           Function<L, String> lookupAsString)
        {
            super(primaryTable, primaryKeyName, lookupTable, lookupKeyName, primaryAsString, lookupAsString);
        }

        protected void executeAsBatch(List<ModificationStatement> statements)
        throws RequestExecutionException, RequestValidationException
        {
            statements.forEach(stmt -> stmt.executeInternal(QueryState.forInternalCalls(),
                                                            QueryOptions.forInternalCalls(Collections.emptyList())));
        }

        protected UntypedResultSet process(String query) throws RequestExecutionException
        {
            return QueryProcessor.executeOnceInternal(query);
        }

        protected UntypedResultSet process(SelectStatement select, ByteBuffer...args)
        {
            return UntypedResultSet.create(select.executeInternal(QueryState.forInternalCalls(),
                                                                  QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                                Arrays.asList(args))).result);
        }
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_NAME, TABLE_NAME));
        // create the system_auth keyspace so the IRoleManager & ICapabilityManager can function as normal
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, CFMetaData.class));
        setupRoleManager();
        setupCapabilityManager();
    }

    @Before
    public void setup() throws Exception
    {
        // wipe out the system_auth keyspace prior to each test
        Keyspace authKeyspace = Schema.instance.getKeyspaceInstance(SchemaConstants.AUTH_KEYSPACE_NAME);
        authKeyspace.getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);

        // create the defined roles, as it's only the restrictions & the relationships
        // between roles that we want to test here
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (RoleResource role : ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());
    }

    private static void setupRoleManager()
    {
        IRoleManager roleManager = new LocalExecutionRoleManager();
        roleManager.setup();
        setDatabaseDescriptorField("roleManager", roleManager);
    }

    private static void setupCapabilityManager()
    {
        ICapabilityManager capabilityManager = new CassandraCapabilityManager(new LocalExecutionTableHandler());
        capabilityManager.setup();
        setDatabaseDescriptorField("capabilityManager", capabilityManager);
    }


    // tests of checking the restrictions for a specific role & resource combination
    // used during acces checks at execution time

    @Test
    public void getDirectRestrictions()
    {
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        Capability[] expected = new Capability[] { Capabilities.System.FILTERING,
                                                   Capabilities.System.LWT,
                                                   Capabilities.System.TRUNCATE };
        addRestrictions(ROLE_A, TABLE, expected);
        assertEquals(new CapabilitySet(expected), capabilityManager.getRestricted(ROLE_A, TABLE));
    }

    @Test
    public void getInheritedRestrictions()
    {
        addRestrictions(ROLE_B, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, TABLE, Capabilities.System.TRUNCATE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        Capability[] expected = new Capability[] { Capabilities.System.LWT,
                                                   Capabilities.System.TRUNCATE };

        assertEquals(new CapabilitySet(expected), capabilityManager.getRestricted(ROLE_A, TABLE));
    }

    @Test
    public void getDirectAndInheritedRestrictions()
    {
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, TABLE, Capabilities.System.TRUNCATE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        Capability[] expected = new Capability[] { Capabilities.System.FILTERING,
                                                   Capabilities.System.LWT,
                                                   Capabilities.System.TRUNCATE };

        assertEquals(new CapabilitySet(expected), capabilityManager.getRestricted(ROLE_A, TABLE));
    }

    // tests of the various permutations of ICapabilityManager::listPermissions
    // used by LIST RESTRICTIONS statements in CQL

    @Test
    public void listAllRestrictions()
    {
        // LIST RESTRICTIONS
        // or
        // LIST RESTRICTIONS ON ANY ROLE USING ANY CAPABILITY WITH ANY RESOURCE

        // note: includeInherited argument to listRestrictions is irrelevant here
        // as we're interested in ANY_ROLE anyway
        addMultipleRestrictions();

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY), true);

        assertEquals(12, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.NON_LWT_UPDATE);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.PARTITION_RANGE_READ);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.CL_ANY_WRITE);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_READ);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_WRITE);
    }

    @Test
    public void listAllRestrictionsOnRole()
    {
        // LIST RESTRICTIONS ON role_a

        // basically the same as listAllRestrictions but when listing we specify only ROLE_A
        // All restrictions are retrieved as ROLE_A is also granted ROLE_B and ROLE_C
        addMultipleRestrictions();
        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, ANY_RESOURCE, ANY_CAPABILITY), true);

        assertEquals(12, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.NON_LWT_UPDATE);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.PARTITION_RANGE_READ);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.CL_ANY_WRITE);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_READ);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_WRITE);
    }

    @Test
    public void listDirectRestrictionsOnRole()
    {
        // LIST RESTRICTIONS ON role_a NORECURSIVE
        addMultipleRestrictions();
        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, ANY_RESOURCE, ANY_CAPABILITY), false);

        assertEquals(4, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.NON_LWT_UPDATE);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
    }

    @Test
    public void listAllRestrictionsOnRoleAndResource()
    {
        // LIST RESTRICTIONS ON role_a USING ANY CAPABILITY WITH keyspace.table
        addMultipleRestrictions();
        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, TABLE, ANY_CAPABILITY), true);

        assertEquals(6, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
    }

    @Test
    public void listDirectRestrictionsOnRoleAndResource()
    {
        // LIST RESTRICTIONS ON role_a USING ANY CAPABILITY WITH keyspace.table NORECURSIVE
        addMultipleRestrictions();
        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, TABLE, ANY_CAPABILITY), false);

        assertEquals(2, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
    }

    @Test
    public void listAllRestrictionsOnRoleAndCapability()
    {
        // LIST RESTRICTIONS ON role_a USING lwt (WITH ANY RESOURCE)?

        // a little contrived as restricting a single capability on a table and
        // its keyspace is redundant, but it illustrates the selection
        // likewise, both ROLE_A and ROLE_B have LWT restricted on KEYSPACE, which
        // again is redundant given ROLE_A is granted ROLE_B
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_WRITE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, ANY_RESOURCE, Capabilities.System.LWT), true);

        assertEquals(3, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.LWT);
    }

    @Test
    public void listDirectRestrictionsOnRoleAndCapability()
    {
        // LIST RESTRICTIONS ON role_a USING lwt (WITH ANY RESOURCE)? NORECURSIVE

        // a little contrived as restricting a single capability on a table and
        // its keyspace is redundant, but it illustrates the selection
        // likewise, both ROLE_A and ROLE_B have LWT restricted on KEYSPACE, which
        // again is redundant given ROLE_A is granted ROLE_B
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_WRITE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, ANY_RESOURCE, Capabilities.System.LWT), false);

        assertEquals(2, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.LWT);
    }

    @Test
    public void listAllRestrictionsOnRoleAndResourceAndCapability()
    {
        // LIST RESTRICTIONS ON role_a USING lwt WITH keyspace.table

        // a little contrived as restricting a single capability on a table and
        // its keyspace is redundant, but it illustrates the selection
        // likewise, both ROLE_A and ROLE_B have LWT restricted on KEYSPACE, which
        // again is redundant given ROLE_A is granted ROLE_B
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_WRITE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ROLE_A, KEYSPACE, Capabilities.System.LWT), true);

        assertEquals(2, restrictions.size());
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.LWT);
    }

    @Test
    public void listDirectRestrictionsOnRoleAndResourceAndCapability()
    {
        // LIST RESTRICTIONS ON role_a USING lwt WITH keyspace.table NORECURSIVE

        // a little contrived as restricting a single capability on a table and
        // its keyspace is redundant, but it illustrates the selection
        // likewise, both ROLE_A and ROLE_B have LWT restricted on KEYSPACE, which
        // again is redundant given ROLE_A is granted ROLE_B
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_WRITE);

        grantRolesTo(ROLE_A, ROLE_B, ROLE_C);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
          capabilityManager.listRestrictions(spec(ROLE_A, KEYSPACE, Capabilities.System.LWT), false);

        assertEquals(1, restrictions.size());
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.LWT);
    }


    @Test
    public void listRestrictionsOnCapability()
    {
        // LIST RESTRICTIONS ON ANY ROLE USING lwt WITH ANY RESOURCE

        // note: includeInherited argument to listRestrictions is irrelevant here
        // as we're interested in ANY_ROLE anyway
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_WRITE);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, Capabilities.System.LWT), false);

        assertEquals(4, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.LWT);
    }

    @Test
    public void listRestrictionsOnResource()
    {
        // LIST RESTRICTIONS ON ANY ROLE USING ANY CAPABILITY WITH keyspace.table

        // note: includeInherited argument to listRestrictions is irrelevant here
        // as we're interested in ANY_ROLE anyway
        addMultipleRestrictions();

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, TABLE, ANY_CAPABILITY), true);

        assertEquals(6, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.FILTERING);
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
    }

    @Test
    public void listRestrictionsOnResourceAndCapability()
    {
        // LIST RESTRICTIONS ON ANY ROLE USING lwt WITH keyspace.table

        // note: includeInherited argument to listRestrictions is irrelevant here
        // as we're interested in ANY_ROLE anyway
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.CL_ALL_WRITE);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.MULTI_PARTITION_AGGREGATION);

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, TABLE, Capabilities.System.LWT), true);

        assertEquals(2, restrictions.size());
        assertPresent(restrictions, ROLE_A, TABLE, Capabilities.System.LWT);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.LWT);
    }

    @Test
    public void createAndDropMaintainLookupTable()
    {
        // add a restriction on ROLE_A + TABLE
        // add a restriction on ROLE_A + KEYSPACE
        // add a restriction on ROLE_B + KEYSPACE
        // add a restriction on ROLE_C + KEYSPACE
        // verify index table
        // drop restriction on ROLE_A + TABLE
        // verify index table
        // drop restrictions on (ROLE_B|ROLE_C) + KEYSPACE
        // verify index table
        // drop restriction on ROLE_A + KEYSPACE
        // verify index table

        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_READ);

        assertExpectedLookupValues(KEYSPACE, ROLE_A, ROLE_B, ROLE_C);
        assertExpectedLookupValues(TABLE, ROLE_A);

        dropRestriction(ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        assertExpectedLookupValues(KEYSPACE, ROLE_B, ROLE_C);
        assertExpectedLookupValues(TABLE, ROLE_A);

        dropRestriction(ROLE_B, KEYSPACE, Capabilities.System.FILTERING);
        dropRestriction(ROLE_C, KEYSPACE, Capabilities.System.CL_ALL_READ);
        assertExpectedLookupValues(KEYSPACE);
        assertExpectedLookupValues(TABLE, ROLE_A);

        dropRestriction(ROLE_A, TABLE, Capabilities.System.LWT);
        assertExpectedLookupValues(KEYSPACE);

    }

    @Test
    public void removeAllRestrictionsForResource()
    {
        addMultipleRestrictions();
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();

        // to begin there should be restrictions for both KEYSPACE & TABLE (12 in total)
        // and there will be entries in the lookup table for both resources and all 3 roles
        assertEquals(12, capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY), true).size());
        assertExpectedLookupValues(TABLE, ROLE_A, ROLE_B, ROLE_C);
        assertExpectedLookupValues(KEYSPACE, ROLE_A, ROLE_B, ROLE_C);

        // drop all the restrictions which apply directly to TABLE
        // as would happen during a DROP TABLE statement
        capabilityManager.dropAllRestrictionsWith(TABLE);

        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY), true);

        // now only the restrictions on KEYSPACE should remain
        assertEquals(6, restrictions.size());
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.NON_LWT_UPDATE);
        assertPresent(restrictions, ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.PARTITION_RANGE_READ);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.CL_ANY_WRITE);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_READ);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_WRITE);

        // and there should only be values in the lookup table for KEYSPACE (3 of them)
        assertExpectedLookupValues(TABLE);
        assertExpectedLookupValues(KEYSPACE, ROLE_A, ROLE_B, ROLE_C);
    }

    @Test
    public void removeAllRestrictionsForRole()
    {
        addMultipleRestrictions();
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();

        // to begin there should be restrictions for both KEYSPACE & TABLE (12 in total)
        // and there will be entries in the lookup table for both resources and all 3 roles
        assertEquals(12, capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY), true).size());
        assertExpectedLookupValues(TABLE, ROLE_A, ROLE_B, ROLE_C);
        assertExpectedLookupValues(KEYSPACE, ROLE_A, ROLE_B, ROLE_C);

        // drop all the restrictions on ROLE_A
        // as would happen during a DROP ROLE statement
        capabilityManager.dropAllRestrictionsOn(ROLE_A);

        ImmutableSet<Restriction> restrictions =
            capabilityManager.listRestrictions(spec(ANY_ROLE, ANY_RESOURCE, ANY_CAPABILITY), true);

        // now only the restrictions on ROLE_B & ROLE_C should remain
        assertEquals(8, restrictions.size());
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        assertPresent(restrictions, ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.PARTITION_RANGE_READ);
        assertPresent(restrictions, ROLE_B, KEYSPACE, Capabilities.System.CL_ANY_WRITE);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        assertPresent(restrictions, ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_READ);
        assertPresent(restrictions, ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_WRITE);

        // and there should only be values in the lookup table for ROLE_B & ROLE_C
        assertExpectedLookupValues(TABLE, ROLE_B, ROLE_C);
        assertExpectedLookupValues(KEYSPACE, ROLE_B, ROLE_C);
    }

    @Test
    public void deferValidationForRestrictionToResourceImpl()
    {
        final AtomicInteger delegatedCallCount = new AtomicInteger(0);
        IResource dummyResource = new IResource()
        {
            public String getName() { return null; }
            public IResource getParent() { return null; }
            public boolean hasParent() { return false; }
            public boolean exists() { return false; }
            public Set<Permission> applicablePermissions() { return null; }

            public boolean validForCapabilityRestriction(Capability capability)
            {
                delegatedCallCount.incrementAndGet();
                return false;
            }
        };

        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        // The implementation of validateForRestriction in CassandraCapabilityManager
        // simply delegates to the IResource instance
        for (Capability capability : Capabilities.System.ALL)
           capabilityManager.validateForRestriction(capability, dummyResource);

        assertEquals(Capabilities.System.ALL.length, delegatedCallCount.get());
    }

    private static void grantRolesTo(RoleResource grantee, RoleResource...granted)
    {
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for(RoleResource toGrant : granted)
            roleManager.grantRole(AuthenticatedUser.ANONYMOUS_USER, toGrant, grantee);
    }

    private static void addMultipleRestrictions()
    {
        addRestrictions(ROLE_A, TABLE, Capabilities.System.FILTERING);
        addRestrictions(ROLE_A, TABLE, Capabilities.System.LWT);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.NON_LWT_UPDATE);
        addRestrictions(ROLE_A, KEYSPACE, Capabilities.System.TRUNCATE);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_READ);
        addRestrictions(ROLE_B, TABLE, Capabilities.System.MULTI_PARTITION_AGGREGATION);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.PARTITION_RANGE_READ);
        addRestrictions(ROLE_B, KEYSPACE, Capabilities.System.CL_ANY_WRITE);
        addRestrictions(ROLE_C, TABLE, Capabilities.System.CL_ALL_READ);
        addRestrictions(ROLE_C, TABLE, Capabilities.System.CL_ALL_WRITE);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_READ);
        addRestrictions(ROLE_C, KEYSPACE, Capabilities.System.CL_ONE_WRITE);
    }

    private static void addRestrictions(RoleResource role, IResource resource, Capability...capabilities)
    {
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        for (Capability capability : capabilities)
            capabilityManager.createRestriction(AuthenticatedUser.ANONYMOUS_USER,
                                                new Restriction(role, resource.getName(), capability));
    }

    private static void dropRestriction(RoleResource role, DataResource resource, Capability capability)
    {
        ICapabilityManager capabilityManager = DatabaseDescriptor.getCapabilityManager();
        capabilityManager.dropRestriction(AuthenticatedUser.ANONYMOUS_USER,
                                          new Restriction(role, resource.getName(), capability));
    }

    private static Restriction.Specification spec(RoleResource role, IResource resource, Capability capability)
    {
        return new Restriction.Specification(role, resource, capability);
    }

    private static void assertPresent(Set<Restriction> restrictions,
                                      RoleResource role,
                                      IResource resource,
                                      Capability capability)
    {
        assertTrue(restrictions.contains(new Restriction(role, resource.getName(), capability)));
    }

    private static void assertExpectedLookupValues(IResource resource, RoleResource...roles)
    {
        UntypedResultSet results = QueryProcessor.executeOnceInternal(
        String.format("SELECT resource, role FROM %s.%s " +
                      "WHERE resource = '%s'",
                      SchemaConstants.AUTH_KEYSPACE_NAME,
                      AuthKeyspace.RESOURCE_RESTRICTIONS_INDEX,
                      resource.getName()));
        assertEquals(roles.length, results.size());
        int i = 0;
        for (UntypedResultSet.Row row : results)
            assertEquals(roles[i++].getName(), row.getString("role"));
    }
}
