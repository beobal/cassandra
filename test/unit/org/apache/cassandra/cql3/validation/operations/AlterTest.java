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
package org.apache.cassandra.cql3.validation.operations;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class AlterTest extends CQLTester
{
    @Test
    public void testAddList() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        execute("ALTER TABLE %s ADD myCollection list<text>;");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", list("first element")));
    }

    @Test
    public void testDropList() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test"));
    }

    @Test
    public void testAddMap() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        execute("ALTER TABLE %s ADD myCollection map<text, text>;");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'});");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", map("1", "first element")));
    }

    @Test
    public void testDropMap() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection map<text, text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'});");
        execute("ALTER TABLE %s DROP myCollection;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test"));
    }

    @Test
    public void testDropListAndAddListWithSameName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");
        execute("ALTER TABLE %s ADD myCollection list<text>;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", null));
        execute("UPDATE %s set myCollection = ['second element'] WHERE id = 'test';");
        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", list("second element")));
    }

    @Test
    public void testDropListAndAddMapWithSameName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");

        assertInvalid("ALTER TABLE %s ADD myCollection map<int, int>;");
    }

    @Test
    public void testDropWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop int, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop` will get discarded during
        // alter statement
        flush(true);
        execute("ALTER TABLE %s DROP todrop USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop int;");
        execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000L);
        assertRows(execute("SELECT id, c1, v1, todrop FROM %s"),
                   row(1, 0, 0, null),
                   row(1, 1, 1, null),
                   row(1, 2, 2, null),
                   row(1, 3, 3, 3),
                   row(1, 4, 4, 4),
                   row(1, 100, 100, 100));
    }

    @Test
    public void testDropStaticWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop int static, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop` will get discarded during
        // alter statement
        flush(true);
        execute("ALTER TABLE %s DROP todrop USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop int static;");
        execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000L);
        // static column value with largest timestmap will be available again
        assertRows(execute("SELECT id, c1, v1, todrop FROM %s"),
                   row(1, 0, 0, 4),
                   row(1, 1, 1, 4),
                   row(1, 2, 2, 4),
                   row(1, 3, 3, 4),
                   row(1, 4, 4, 4),
                   row(1, 100, 100, 4));
    }

    @Test
    public void testDropMultipleWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop1 int, todrop2 int, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop1` and `todrop2` will get discarded during
        // alter statement
        flush(true);
        execute("ALTER TABLE %s DROP (todrop1, todrop2) USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop1 int;");
        execute("ALTER TABLE %s ADD todrop2 int;");

        execute("INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 100, 40000L);
        assertRows(execute("SELECT id, c1, v1, todrop1, todrop2 FROM %s"),
                   row(1, 0, 0, null, null),
                   row(1, 1, 1, null, null),
                   row(1, 2, 2, null, null),
                   row(1, 3, 3, 3, 3),
                   row(1, 4, 4, 4, 4),
                   row(1, 100, 100, 100, 100));
    }


    @Test
    public void testChangeStrategyWithUnquotedAgrument() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");

        assertInvalidSyntaxMessage("no viable alternative at input '}'",
                                   "ALTER TABLE %s WITH caching = {'keys' : 'all', 'rows_per_partition' : ALL};");
    }

    @Test
    // tests CASSANDRA-7976
    public void testAlterIndexInterval() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (id uuid, album text, artist text, data blob, PRIMARY KEY (id))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        alterTable("ALTER TABLE %s WITH min_index_interval=256 AND max_index_interval=512");
        assertEquals(256, cfs.metadata().params.minIndexInterval);
        assertEquals(512, cfs.metadata().params.maxIndexInterval);

        alterTable("ALTER TABLE %s WITH caching = {}");
        assertEquals(256, cfs.metadata().params.minIndexInterval);
        assertEquals(512, cfs.metadata().params.maxIndexInterval);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.create_alter_options_test()
     */
    @Test
    public void testCreateAlterKeyspaces() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE ks1");
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }");

        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String ks2 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes FROM system_schema.keyspaces"),
                   row(KEYSPACE, true),
                   row(KEYSPACE_PER_TEST, true),
                   row(ks1, true),
                   row(ks2, false));

        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 1 } AND durable_writes=False");
        schemaChange("ALTER KEYSPACE " + ks2 + " WITH durable_writes=true");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                   row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row(ks1, false, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER, "1")),
                   row(ks2, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")));

        execute("USE " + ks1);

        assertInvalidThrow(ConfigurationException.class, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }");

        execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }");
        assertRows(execute("SELECT table_name, compaction FROM system_schema.tables WHERE keyspace_name='" + ks1 + "'"),
                   row("cf1", map("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                                  "min_threshold", "7",
                                  "max_threshold", "32")));
    }

    /**
     * Test {@link ConfigurationException} thrown on alter keyspace to no DC option in replication configuration.
     */
    @Test
    public void testAlterKeyspaceWithNoOptionThrowsConfigurationException() throws Throwable
    {
        // Create keyspaces
        execute("CREATE KEYSPACE testABC WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 3 }");
        execute("CREATE KEYSPACE testXYZ WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

        // Try to alter the created keyspace without any option
        assertInvalidThrow(ConfigurationException.class, "ALTER KEYSPACE testABC WITH replication={ 'class' : 'NetworkTopologyStrategy' }");
        assertInvalidThrow(ConfigurationException.class, "ALTER KEYSPACE testXYZ WITH replication={ 'class' : 'SimpleStrategy' }");

        // Make sure that the alter works as expected
        execute("ALTER KEYSPACE testABC WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 }");
        execute("ALTER KEYSPACE testXYZ WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");

        // clean up
        execute("DROP KEYSPACE IF EXISTS testABC");
        execute("DROP KEYSPACE IF EXISTS testXYZ");
    }

    /**
     * Test {@link ConfigurationException} thrown when altering a keyspace to invalid DC option in replication configuration.
     */
    @Test
    public void testAlterKeyspaceWithNTSOnlyAcceptsConfiguredDataCenterNames() throws Throwable
    {
        // Create a keyspace with expected DC name.
        execute("CREATE KEYSPACE testABC WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 }");

        // try modifying the keyspace
        assertInvalidThrow(ConfigurationException.class, "ALTER KEYSPACE testABC WITH replication = { 'class' : 'NetworkTopologyStrategy', 'INVALID_DC' : 2 }");
        execute("ALTER KEYSPACE testABC WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 3 }");

        // Mix valid and invalid, should throw an exception
        assertInvalidThrow(ConfigurationException.class, "ALTER KEYSPACE testABC WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 , 'INVALID_DC': 1}");

        // clean-up
        execute("DROP KEYSPACE IF EXISTS testABC");
    }

    /**
     * Test {@link ConfigurationException} thrown when removing a DC with active cassandra instances from replication configuration of system_auth keyspace.
     */
    @Test
     public void testAlterKeyspaceSystem_AuthWithNTSOnlyAcceptsConfiguredDataCenterNames() throws Throwable
     {
         // Add a peer
         StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddress.getByName("127.0.0.2"));
         // Register an Endpoint snitch which returns fixed value for data center.
         String DATA_CENTER2 = "datacenter2";
         DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
         {
            public String getRack(InetAddress endpoint) { return RACK1; }
            public String getDatacenter(InetAddress endpoint) {
                if(endpoint.getHostAddress().equalsIgnoreCase("127.0.0.2"))
                    return DATA_CENTER2;
                return DATA_CENTER;
            }
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) { return 0; }
         });

         // Create a keyspace with expected DC name.
         execute(String.format("CREATE KEYSPACE %s WITH replication = {" +
                               "  'class' : 'NetworkTopologyStrategy', " +
                               "  '%s' : 2 , " +
                               "  '%s' : 2 " +
                               "}",
                               SchemaConstants.AUTH_KEYSPACE_NAME,
                               DATA_CENTER,
                               DATA_CENTER2));

         // Try modifying the system_auth keyspace without second DC which has an active node. ConfigurationException is expected.
         assertInvalidThrow(ConfigurationException.class,
                            String.format("ALTER KEYSPACE %s WITH replication = { " +
                                          "  'class' : 'NetworkTopologyStrategy', " +
                                          "  '%s' : 2 }",
                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                          DATA_CENTER));

         // Make sure that altering system_auth keyspace with correct DC names is successful
         execute(String.format("ALTER KEYSPACE %s WITH replication = {" +
                               "  'class' : 'NetworkTopologyStrategy', " +
                               "  '%s' : 1 , " +
                               "  '%s' : 1 }",
                               SchemaConstants.AUTH_KEYSPACE_NAME,
                               DATA_CENTER,
                               DATA_CENTER2));
     }

    /**
     * Test for bug of 5232,
     * migrated from cql_tests.py:TestCQL.alter_bug_test()
     */
    @Test
    public void testAlterStatementWithAdd() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, t text)");

        execute("UPDATE %s SET t = '111' WHERE id = 1");

        execute("ALTER TABLE %s ADD l list<text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, "111"));

        execute("ALTER TABLE %s ADD m map<int, text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, null, "111"));
    }

    /**
     * Test for 7744,
     * migrated from cql_tests.py:TestCQL.downgrade_to_compact_bug_test()
     */
    @Test
    public void testDowngradeToCompact() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>)");
        execute("insert into %s (k, v) VALUES (0, {'f'})");
        flush();
        execute("alter table %s drop v");
        execute("alter table %s add v int");
    }

    @Test
    // tests CASSANDRA-9565
    public void testDoubleWith() throws Throwable
    {
        String[] stmts = { "ALTER KEYSPACE WITH WITH DURABLE_WRITES = true",
                           "ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true" };

        for (String stmt : stmts) {
            assertInvalidSyntaxMessage("no viable alternative at input 'WITH'", stmt);
        }
    }

    @Test
    public void testAlterTableWithCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.LZ4Compressor")));

        execute("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        execute("ALTER TABLE %s WITH compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 64 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.LZ4Compressor")));

        execute("ALTER TABLE %s WITH compression = { 'sstable_compression' : '', 'chunk_length_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

        execute("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
        execute("ALTER TABLE %s WITH compression = { 'enabled' : 'false'};");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

        assertThrowsConfigurationException("Missing sub-option 'class' for the 'compression' option.",
                                           "ALTER TABLE %s WITH  compression = {'chunk_length_in_kb' : 32};");

        assertThrowsConfigurationException("The 'class' option must not be empty. To disable compression use 'enabled' : false",
                                           "ALTER TABLE %s WITH  compression = { 'class' : ''};");

        assertThrowsConfigurationException("If the 'enabled' option is set to false no other options must be specified",
                                           "ALTER TABLE %s WITH compression = { 'enabled' : 'false', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("The 'sstable_compression' option must not be used if the compression algorithm is already specified by the 'class' option",
                                           "ALTER TABLE %s WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("The 'chunk_length_kb' option must not be used if the chunk length is already specified by the 'chunk_length_in_kb' option",
                                           "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_kb' : 32 , 'chunk_length_in_kb' : 32 };");
    }

    private void assertThrowsConfigurationException(String errorMsg, String alterStmt) throws Throwable
    {
        try
        {
            execute(alterStmt);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + alterStmt);
        }
        catch (ConfigurationException e)
        {
            assertEquals(errorMsg, e.getMessage());
        }
    }
}
