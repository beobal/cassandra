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

package org.apache.cassandra.auth;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Implements the management of simple reverse lookup tables for Auth purposes.
 *
 * There are a couple of places in the auth subsystem where inverted indexes are maintained. The
 * predominant pattern is for the data to be primarily indexed against a Role, with rows clustered
 * on IResource. In this scenario, a reverse lookup is used to find all Roles associated with a
 * given IResource.
 *
 * For example, in permissions management the relevant tables are defined as:
 *
 *   CREATE TABLE role_permissions (
 *       role text,
 *       resource text,
 *       permissions set<text>,
 *       PRIMARY KEY(role, resource)
 *   )
 *
 *   CREATE TABLE resource_role_permissons_index (
 *       resource text,
 *       role text,
 *       PRIMARY KEY(resource, role)
 *   )
 *
 * This same pattern is used by permissions and capability restrictions.
 *
 * This class encapsulates the maintenance and use of these reverse lookup tables. It makes a number of
 * assumptions:
 * 1) The keyspace is fixed to AuthKeyspace.NAME
 * 2) The key columns in both tables are text type
 * 3) The names of the columns in the primary and lookup tables match up
 *
 * @param <P> The type of object represented by keys in the primary table. Usually this is a RoleResource.
 * @param <L> The type of object represented by keys in the lookup table. Usually this is IResource.
 */
public class AuthLookupTableSupport<P, L>
{
    private static final Logger logger = LoggerFactory.getLogger(AuthLookupTableSupport.class);

    private static final String SELECT_TEMPLATE = "SELECT %s FROM %s.%s WHERE %s = ?";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s.%s (%s, %s) VALUES ('%s', '%s')";
    private static final String DELETE_ONE_TEMPLATE = "DELETE FROM %s.%s WHERE %s = '%s' AND %s = '%s'";
    private static final String DELETE_ALL_TEMPLATE = "DELETE FROM %s.%s WHERE %s = '%s'";

    // lookup statements are more freqently used so are prepared while
    // insert and deletes are comparatively rare, so those are just
    // done by constructing the appropriate CQL string and executing
    private SelectStatement getLookupKeys;
    private SelectStatement getPrimaryKeys;

    private final String primaryTable;
    private final String lookupTable;

    private final String lookupKeyName;
    private final String primaryKeyName;

    private final Function<P, String> primaryAsString;
    private final Function<L, String> lookupAsString;

    public AuthLookupTableSupport(String primaryTable,
                                  String primaryKeyName,
                                  String lookupTable,
                                  String lookupKeyName,
                                  Function<P, String> primaryAsString,
                                  Function<L, String> lookupAsString)
    {
        this.primaryTable = primaryTable;
        this.primaryKeyName = primaryKeyName;
        this.primaryAsString = primaryAsString;
        this.lookupTable = lookupTable;
        this.lookupKeyName = lookupKeyName;
        this.lookupAsString = lookupAsString;

    }

    public UntypedResultSet lookup(L lookupValue)
    {
        prepareStatements();
        return process(getPrimaryKeys, ByteBufferUtil.bytes(escape(lookupAsString(lookupValue))));
    }

    public void removeLookupEntry(P primaryValue, L lookupValue)
    {
        process(makeDeleteOneFromLookup(primaryAsString(primaryValue), lookupAsString(lookupValue)));
    }

    public void addLookupEntry(P primaryValue, L lookupValue)
    {
        process(makeInsertEntry(primaryValue, lookupValue));
    }

    public void removeAllEntriesForPrimaryKey(P primaryValue)
    {
        try
        {
            prepareStatements();

            List<ModificationStatement> statements = new ArrayList<>();

            // For the given row in the primary table, add a delete statement for each lookup entry
            UntypedResultSet rows = process(getLookupKeys, ByteBufferUtil.bytes(escape(primaryAsString(primaryValue))));
            for (UntypedResultSet.Row row : rows)
            {
                String cql = makeDeleteOneFromLookup(primaryAsString(primaryValue), row.getString(lookupKeyName));
                statements.add(makeModification(cql));
            }

            // Also, a delete for all rows in the primary table for this primary key
            ModificationStatement deleteFromPrimary = makeModification(makeDeleteAllFromPrimary(primaryValue));
            statements.add(deleteFromPrimary);

            executeAsBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("Failed to clean up auth lookup table for {}: {}", primaryAsString(primaryValue), e);
        }
    }

    public void removeAllEntriesForLookupKey(L lookupValue)
    {
        try
        {
            prepareStatements();

            List<ModificationStatement> statements = new ArrayList<>();

            // For a given key in the lookup table, remove the rows in the primary table
            UntypedResultSet rows = process(getPrimaryKeys, ByteBufferUtil.bytes(escape(lookupAsString(lookupValue))));
            for (UntypedResultSet.Row row : rows)
            {
                String cql = makeDeleteOneFromPrimary(row.getString(primaryKeyName), lookupValue);
                statements.add(makeModification(cql));
            }

            // Also a delete of all rows in the lookup table for this lookup key
            ModificationStatement deleteFromLookup = makeModification(makeDeleteAllFromLookup(lookupValue));
            statements.add(deleteFromLookup);

            executeAsBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("Failed to clean up auth lookup table for {}: {}", lookupAsString(lookupValue), e);
        }
    }

    // May be overridden in tests to have statements executed locally, not via StorageProxy
    protected void executeAsBatch(List<ModificationStatement> statements)
    throws RequestExecutionException, RequestValidationException
    {
        BatchStatement batch = new BatchStatement(0, BatchStatement.Type.LOGGED, statements, Attributes.none());
        QueryProcessor.instance.processBatch(batch,
                                             QueryState.forInternalCalls(),
                                             BatchQueryOptions.withoutPerStatementVariables(QueryOptions.DEFAULT),
                                             System.nanoTime());

    }

    // May be overridden in tests to have statements executed locally, not via StorageProxy
    protected UntypedResultSet process(String query) throws RequestExecutionException
    {
        return QueryProcessor.process(query, ConsistencyLevel.LOCAL_ONE);
    }

    // May be overridden in tests to have statements executed locally, not via StorageProxy
    protected UntypedResultSet process(SelectStatement select, ByteBuffer...args)
    {
        return UntypedResultSet.create(select.execute(QueryState.forInternalCalls(),
                                                      QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                    Arrays.asList(args)),
                                                      System.nanoTime()).result);
    }

    private String primaryAsString(P p)
    {
        return primaryAsString.apply(p);
    }

    private String lookupAsString(L l)
    {
        return lookupAsString.apply(l);
    }

    private void prepareStatements()
    {
        if (getLookupKeys == null)
            getLookupKeys = makeSelect(primaryTable);
        if (getPrimaryKeys == null)
            getPrimaryKeys = makeSelect(lookupTable);
    }

    /**
     * Used to produce prepared Select statements on both the primary and lookup tables
     * @param table the name of the table the select is for (primary or lookup table name)
     * @return A prepared select statement to execute a query on the primary or lookup table
     */
    private SelectStatement makeSelect(String table)
    {
        // depending on which table the statement is for, will produce:
        // SELECT lookup_key FROM system_auth.primary_table WHERE primary_key = ?;
        // SELECT primary_key FROM system_auth.lookup_table WHERE lookup_key = ?;

        String selecting = table.equals(lookupTable) ? primaryKeyName : lookupKeyName;
        String byKey = table.equals(lookupTable) ? lookupKeyName : primaryKeyName;

        String cql = String.format(SELECT_TEMPLATE, selecting, SchemaConstants.AUTH_KEYSPACE_NAME, table, byKey);
        return (SelectStatement) QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement;
    }

    /**
     * Used when adding an entry in the lookup table
     * @param primaryValue representing a key in the primary table
     * @param lookupValue representing a key in the lookup table
     * @return CQL String in the form:
     *      INSERT INTO system_auth.lookup_table (lookup_key, primary_key)
     *      VALUES (lookup_value, primary_value);
     */
    private String makeInsertEntry(P primaryValue, L lookupValue)
    {
        return String.format(INSERT_TEMPLATE,
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             lookupTable,
                             lookupKeyName,
                             primaryKeyName,
                             escape(lookupAsString(lookupValue)),
                             escape(primaryAsString(primaryValue)));
    }

    /**
     * Used when removing an entry from the lookup table
     * and when building a batch statement to bulk delete lookup entries
     * @param primaryValue representing a key in the primary table
     * @param lookupValue representing a key in the lookup table
     * @return CQL String in the form:
     *      DELETE FROM system_auth.lookup_table
     *      WHERE lookup_key = lookup_value
     *      AND primary_key = primary_value;
     */
    private String makeDeleteOneFromLookup(String primaryValue, String lookupValue)
    {
        return String.format(DELETE_ONE_TEMPLATE,
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             lookupTable,
                             lookupKeyName,
                             escape(lookupValue),
                             primaryKeyName,
                             escape(primaryValue));
    }

    /**
     * Used when bulk removing all lookup entries for a lookup key
     * we compile a set of statements with all the deletes of the
     * primary table rows, combine with this one to delete all the
     * lookup entries, then execute as a logged batch
     * @param lookupValue representing the key in the lookup table
     * @return CQL String in the form:
     *     DELETE FROM system_auth.lookup_table where lookup_key = lookup_value;
     */
    private String makeDeleteAllFromLookup(L lookupValue)
    {
        return String.format(DELETE_ALL_TEMPLATE,
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             lookupTable,
                             lookupKeyName,
                             escape(lookupAsString(lookupValue)));
    }


    /**
     * Used when bulk removing rows in the primary table pointed at by a
     * lookup key. We compile a set of statements with all the deletes
     * and execute as a logged batch
     * @param primaryValue representing the key in the primary table
     * @param lookupValue representing the key in the lookup table
     * @return CQL String in the form:
     *   DELETE FROM system_auth.primary_table
     *   WHERE primary_key = primary_value
     *   AND lookup_key = lookup_value;
     */
    private String makeDeleteOneFromPrimary(String primaryValue, L lookupValue)
    {
        return String.format(DELETE_ONE_TEMPLATE,
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             primaryTable,
                             primaryKeyName,
                             escape(primaryValue),
                             lookupKeyName,
                             escape(lookupAsString(lookupValue)));
    }

    /**
     * Used when bulk removing all lookup entries for a primary key
     * we compile a set of statements with all the deletes and execute
     * as a logged batch
     * @param primaryValue representing the key in the primary table
     * @return CQL String in the form:
     *  DELETE FROM system_auth.primary_table WHERE primary_key = primary_value;
     */
    private String makeDeleteAllFromPrimary(P primaryValue)
    {
        return String.format(DELETE_ALL_TEMPLATE,
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             primaryTable,
                             primaryKeyName,
                             escape(primaryAsString(primaryValue)));
    }

    private ModificationStatement makeModification(String cql)
    {
        return (ModificationStatement) QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement;
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }
}
