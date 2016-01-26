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
package org.apache.cassandra.index.sasi;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.index.sasi.memory.IndexMemtable;
import org.apache.cassandra.index.sasi.plan.QueryController;
import org.apache.cassandra.index.sasi.plan.QueryPlan;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import junit.framework.Assert;

import org.junit.*;

public class SASIIndexTest
{
    private static final IPartitioner PARTITIONER = new Murmur3Partitioner();

    private static final String KS_NAME = "sasi";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME = "clustering_test_cf";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        SchemaLoader.loadSchema();
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(KS_NAME,
                                                                     KeyspaceParams.simpleTransient(1),
                                                                     Tables.of(SchemaLoader.sasiCFMD(KS_NAME, CF_NAME),
                                                                               SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME))));
    }

    @After
    public void cleanUp()
    {
        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
    }

    @Test
    public void testSingleExpressionQueries() throws Exception
    {
        testSingleExpressionQueries(false);
        cleanupData();
        testSingleExpressionQueries(true);
    }

    private void testSingleExpressionQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
            put("key2", Pair.create("Pavel", 26));
            put("key3", Pair.create("Pavel", 27));
            put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("av")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("as")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("aw")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("avel")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("n")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key3", "key4"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(13)));
        Assert.assertEquals(rows.toString(), 0, rows.size());
    }

    @Test
    public void testEmptyTokenizedResults() throws Exception
    {
        testEmptyTokenizedResults(false);
        cleanupData();
        testEmptyTokenizedResults(true);
    }

    private void testEmptyTokenizedResults(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("  ", 14));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        Set<String> rows= getIndexed(store, 10, buildExpression(UTF8Type.instance.decompose("first_name"), Operator.LIKE_MATCHES, UTF8Type.instance.decompose("doesntmatter")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{}, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testMultiExpressionQueries() throws Exception
    {
        testMultiExpressionQueries(false);
        cleanupData();
        testMultiExpressionQueries(true);
    }

    public void testMultiExpressionQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;
        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(14)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key1", "key2"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GT, Int32Type.instance.decompose(14)),
                         buildExpression(age, Operator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GT, Int32Type.instance.decompose(12)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GTE, Int32Type.instance.decompose(13)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.GTE, Int32Type.instance.decompose(16)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LT, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LTE, Int32Type.instance.decompose(29)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                         buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("avel")),
                                     buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("n")),
                                     buildExpression(age, Operator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.isEmpty());

    }

    @Test
    public void testCrossSSTableQueries() throws Exception
    {
        testCrossSSTableQueries(false);
        cleanupData();
        testCrossSSTableQueries(true);

    }

    private void testCrossSSTableQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", 43));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create("Josephine", 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
            }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
            }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", 28));
            }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows;
        rows = getIndexed(store, 10, buildExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("Fiona")),
                                     buildExpression(age, Operator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(27)),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ie")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(43)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key10" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key12", "key13", "key3", "key4", "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(33)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testQueriesThatShouldBeTokenized() throws Exception
    {
        testQueriesThatShouldBeTokenized(false);
        cleanupData();
        testQueriesThatShouldBeTokenized(true);
    }

    private void testQueriesThatShouldBeTokenized(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("If you can dream it, you can do it.", 43));
                put("key1", Pair.create("What you get by achieving your goals is not " +
                        "as important as what you become by achieving your goals, do it.", 33));
                put("key2", Pair.create("Keep your face always toward the sunshine " +
                        "- and shadows will fall behind you.", 43));
                put("key3", Pair.create("We can't help everyone, but everyone can " +
                        "help someone.", 27));
            }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                buildExpression(firstName, Operator.LIKE_CONTAINS,
                        UTF8Type.instance.decompose("What you get by achieving your goals")),
                buildExpression(age, Operator.GT, Int32Type.instance.decompose(32)));

        Assert.assertEquals(rows.toString(), Collections.singleton("key1"), rows);

        rows = getIndexed(store, 10,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("do it.")));

        Assert.assertEquals(rows.toString(), Arrays.asList("key0", "key1"), Lists.newArrayList(rows));
    }

    @Test
    public void testMultiExpressionQueriesWhereRowSplitBetweenSSTables() throws Exception
    {
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(false);
        cleanupData();
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(true);
    }

    private void testMultiExpressionQueriesWhereRowSplitBetweenSSTables(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String)null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create((String)null, 28));
        }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create((String)null, 43));
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", -1));
                put("key2", Pair.create("Josephine", -1));
        }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      buildExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      buildExpression(age, Operator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(27)),
                          buildExpression(age, Operator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key12", Pair.create((String)null, 12));
                put("key14", Pair.create("Demario", 42));
                put("key2", Pair.create("Frank", -1));
        }};

        store = loadData(part4, forceFlush);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Susana")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(13)),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key12" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Demario")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("Josephine")));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          buildExpression(firstName, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ie")),
                          buildExpression(age, Operator.LTE, Int32Type.instance.decompose(43)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key1", "key10" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testPagination() throws Exception
    {
        testPagination(false);
        cleanupData();
        testPagination(true);
    }

    private void testPagination(boolean forceFlush) throws Exception
    {
        // split data into 3 distinct SSTables to test paging with overlapping token intervals.

        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        loadData(part2, forceFlush);
        loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<DecoratedKey> uniqueKeys = getPaged(store, 4,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                buildExpression(age, Operator.GTE, Int32Type.instance.decompose(21)));


        List<String> expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test a single equals condition

        uniqueKeys = getPaged(store, 4, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key18");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test something which is smaller than a single page
        uniqueKeys = getPaged(store, 4,
                              buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                              buildExpression(age, Operator.EQ, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key22");
                add("key08");
                add("key07");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // the same but with the page size of 2 to test minimal pagination windows

        uniqueKeys = getPaged(store, 2,
                              buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                              buildExpression(age, Operator.EQ, Int32Type.instance.decompose(36)));

        Assert.assertEquals(expected, convert(uniqueKeys));

        // and last but not least, test age range query with pagination
        uniqueKeys = getPaged(store, 4,
                buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                buildExpression(age, Operator.GT, Int32Type.instance.decompose(20)),
                buildExpression(age, Operator.LTE, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key08");
                add("key07");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        Set<String> rows;

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' limit 10 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key03", "key04", "key09", "key13", "key14", "key16", "key20", "key22", "key24", "key25" }, rows.toArray(new String[rows.size()])));

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and token(id) >= token('key14') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key03", "key04", "key14", "key16", "key24" }, rows.toArray(new String[rows.size()])));

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and token(id) >= token('key14') and token(id) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key16", "key24" }, rows.toArray(new String[rows.size()])));

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name LIKE '%%a%%' and age > 30 and token(id) >= token('key14') and token(id) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name like '%%ie' limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key07", "key20", "key24" }, rows.toArray(new String[rows.size()])));

        rows = executeCQLWithKeys(String.format("SELECT * FROM %s.%s WHERE first_name like '%%ie' AND token(id) > token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key07", "key24" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testColumnNamesWithSlashes() throws Exception
    {
        testColumnNamesWithSlashes(false);
        cleanupData();
        testColumnNamesWithSlashes(true);
    }

    private void testColumnNamesWithSlashes(boolean forceFlush) throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        Mutation rm1 = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose("key1")));
        rm1.add(PartitionUpdate.singleRowUpdate(store.metadata,
                                                rm1.key(),
                                                buildRow(buildCell(store.metadata,
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("jason"),
                                                                   System.currentTimeMillis()))));

        Mutation rm2 = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose("key2")));
        rm2.add(PartitionUpdate.singleRowUpdate(store.metadata,
                                                rm2.key(),
                                                buildRow(buildCell(store.metadata,
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("pavel"),
                                                                   System.currentTimeMillis()))));

        Mutation rm3 = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose("key3")));
        rm3.add(PartitionUpdate.singleRowUpdate(store.metadata,
                                                rm3.key(),
                                                buildRow(buildCell(store.metadata,
                                                                   UTF8Type.instance.decompose("/data/output/id"),
                                                                   AsciiType.instance.decompose("Aleksey"),
                                                                   System.currentTimeMillis()))));

        rm1.apply();
        rm2.apply();
        rm3.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        final ByteBuffer dataOutputId = UTF8Type.instance.decompose("/data/output/id");

        Set<String> rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key3" }, rows.toArray(new String[rows.size()])));

        // doesn't really make sense to rebuild index for in-memory data
        if (!forceFlush)
            return;

        store.indexManager.invalidateAllIndexesBlocking();

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        // now let's trigger index rebuild and check if we got the data back
        store.indexManager.buildIndexBlocking(store.indexManager.getIndexByName("data_output_id"));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        // also let's try to build an index for column which has no data to make sure that doesn't fail
        store.indexManager.buildIndexBlocking(store.indexManager.getIndexByName("first_name"));
        store.indexManager.buildIndexBlocking(store.indexManager.getIndexByName("data_output_id"));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(dataOutputId, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("el")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testInvalidate() throws Exception
    {
        testInvalidate(false);
        cleanupData();
        testInvalidate(true);
    }

    private void testInvalidate(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String) null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key0", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));

        store.indexManager.invalidateAllIndexesBlocking();

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), rows.isEmpty());


        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Fred", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create("Dino", 28));
        }};

        loadData(part2, forceFlush);

        rows = getIndexed(store, 10, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key6", "key7" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(age, Operator.EQ, Int32Type.instance.decompose(40)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key9" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testTruncate()
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, 1000, true);

        loadData(part2, 2000, true);
        loadData(part3, 3000, true);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        // make sure we don't prematurely delete anything
        store.indexManager.truncateAllIndexesBlocking(500);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        store.indexManager.truncateAllIndexesBlocking(1500);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 10, rows.size());

        store.indexManager.truncateAllIndexesBlocking(2500);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 6, rows.size());

        store.indexManager.truncateAllIndexesBlocking(3500);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        // add back in some data just to make sure it all still works
        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key40", Pair.create("Tianna", 31));
                put("key41", Pair.create("Dennis", 32));
        }};

        loadData(part4, 4000, true);

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 1, rows.size());
    }


    @Test
    public void testConcurrentMemtableReadsAndWrites() throws Exception
    {
        final ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        ExecutorService scheduler = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        final int writeCount = 10000;
        final AtomicInteger updates = new AtomicInteger(0);

        for (int i = 0; i < writeCount; i++)
        {
            final String key = "key" + i;
            final String firstName = "first_name#" + i;
            final String lastName = "last_name#" + i;

            scheduler.submit((Runnable) () -> {
                try
                {
                    newMutation(key, firstName, lastName, 26, System.currentTimeMillis()).apply();
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS); // back up a bit to do more reads
                }
                finally
                {
                    updates.incrementAndGet();
                }
            });
        }

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        int previousCount = 0;

        do
        {
            // this loop figures out if number of search results monotonically increasing
            // to make sure that concurrent updates don't interfere with reads, uses first_name and age
            // indexes to test correctness of both Trie and SkipList ColumnIndex implementations.

            Set<DecoratedKey> rows = getPaged(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                                          buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));

            Assert.assertTrue(previousCount <= rows.size());
            previousCount = rows.size();
        }
        while (updates.get() < writeCount);

        // to make sure that after all of the right are done we can read all "count" worth of rows
        Set<DecoratedKey> rows = getPaged(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(26)));

        Assert.assertEquals(writeCount, rows.size());
    }

    @Test
    public void testSameKeyInMemtableAndSSTables()
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        Map<String, Pair<String, Integer>> data2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 28));
        }};

        loadData(data2, true);

        Map<String, Pair<String, Integer>> data3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 15));
                put("key4", Pair.create("Jason", 29));
        }};

        loadData(data3, false);

        Set<String> rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(15)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(29)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 100, buildExpression(firstName, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("a")),
                                      buildExpression(age, Operator.EQ, Int32Type.instance.decompose(27)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key2", "key3"}, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testInsertingIncorrectValuesIntoAgeIndex()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Mutation rm = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose("key1")));
        update(rm, new ArrayList<Cell>()
        {{
            add(buildCell(firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis()));
            add(buildCell(age, LongType.instance.decompose(26L), System.currentTimeMillis()));
        }});
        rm.apply();

        store.forceBlockingFlush();

        Set<String> rows = getIndexed(store, 10, buildExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                 buildExpression(age, Operator.GTE, Int32Type.instance.decompose(26)));

        // index is expected to have 0 results because age value was of wrong type
        Assert.assertEquals(0, rows.size());
    }


    @Test
    public void testUnicodeSupport()
    {
        testUnicodeSupport(false);
        cleanupData();
        testUnicodeSupport(true);
    }

    private void testUnicodeSupport(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment");

        Mutation rm = new Mutation(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾⒶⓁ ⒞⒣⒜⒭⒮ and normal ones"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("龍馭鬱"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("インディアナ"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key4"));
        update(rm, comment, UTF8Type.instance.decompose("レストラン"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key5"));
        update(rm, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), System.currentTimeMillis());
        rm.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("normal")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("レストラ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("インディ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4", "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("レストラン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testUnicodeSuffixModeNoSplits()
    {
        testUnicodeSuffixModeNoSplits(false);
        cleanupData();
        testUnicodeSuffixModeNoSplits(true);
    }

    private void testUnicodeSuffixModeNoSplits(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        Mutation rm = new Mutation(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("龍馭鬱"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("インディアナ"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("レストラン"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key4"));
        update(rm, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), System.currentTimeMillis());
        rm.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("トラン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ディア")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("ン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_SUFFIX, UTF8Type.instance.decompose("ン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("ベンジャミン ウエスト")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testThatTooBigValueIsRejected()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        for (int i = 0; i < 10; i++)
        {
            byte[] randomBytes = new byte[ThreadLocalRandom.current().nextInt(OnDiskIndexBuilder.MAX_TERM_SIZE, 5 * OnDiskIndexBuilder.MAX_TERM_SIZE)];
            ThreadLocalRandom.current().nextBytes(randomBytes);

            final ByteBuffer bigValue = UTF8Type.instance.decompose(new String(randomBytes));

            Mutation rm = new Mutation(KS_NAME, decoratedKey("key1"));
            update(rm, comment, bigValue, System.currentTimeMillis());
            rm.apply();

            Set<String> rows;

            rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());

            store.forceBlockingFlush();

            rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_MATCHES, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSearchTimeouts() throws Exception
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        RowFilter filter = RowFilter.create();
        filter.add(store.metadata.getColumnDefinition(firstName), Operator.LIKE_CONTAINS, AsciiType.instance.fromString("a"));

        ReadCommand command = new PartitionRangeReadCommand(store.metadata,
                                                            FBUtilities.nowInSeconds(),
                                                            ColumnFilter.all(store.metadata),
                                                            filter,
                                                            DataLimits.NONE,
                                                            DataRange.allData(store.metadata.partitioner),
                                                            Optional.empty(),
                                                            ReadCommand.PostReconciliationProcessor.DEFAULT);

        try
        {
            new QueryPlan(store, command, 0).execute(ReadExecutionController.empty());
            Assert.fail();
        }
        catch (TimeQuotaExceededException e)
        {
            // correct behavior
        }
        catch (Exception e)
        {
            Assert.fail();
            e.printStackTrace();
        }

        // to make sure that query doesn't fail in normal conditions

        Set<String> rows = getKeys(new QueryPlan(store, command, DatabaseDescriptor.getRangeRpcTimeout()).execute(ReadExecutionController.empty()));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testLowerCaseAnalyzer()
    {
        testLowerCaseAnalyzer(false);
        cleanupData();
        testLowerCaseAnalyzer(true);
    }

    @Test
    public void testChinesePrefixSearch()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer fullName = UTF8Type.instance.decompose("/output/full-name/");

        Mutation rm = new Mutation(KS_NAME, decoratedKey("key1"));
        update(rm, fullName, UTF8Type.instance.decompose("美加 八田"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key2"));
        update(rm, fullName, UTF8Type.instance.decompose("仁美 瀧澤"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key3"));
        update(rm, fullName, UTF8Type.instance.decompose("晃宏 高須"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key4"));
        update(rm, fullName, UTF8Type.instance.decompose("弘孝 大竹"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key5"));
        update(rm, fullName, UTF8Type.instance.decompose("満枝 榎本"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key6"));
        update(rm, fullName, UTF8Type.instance.decompose("飛鳥 上原"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key7"));
        update(rm, fullName, UTF8Type.instance.decompose("大輝 鎌田"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key8"));
        update(rm, fullName, UTF8Type.instance.decompose("利久 寺地"), System.currentTimeMillis());
        rm.apply();

        store.forceBlockingFlush();


        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.EQ, UTF8Type.instance.decompose("美加 八田")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("美加")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.EQ, UTF8Type.instance.decompose("晃宏 高須")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(fullName, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("大輝")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key7" }, rows.toArray(new String[rows.size()])));
    }

    public void testLowerCaseAnalyzer(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("address");

        Mutation rm = new Mutation(KS_NAME, decoratedKey("key1"));
        update(rm, comment, UTF8Type.instance.decompose("577 Rogahn Valleys Apt. 178"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key2"));
        update(rm, comment, UTF8Type.instance.decompose("89809 Beverly Course Suite 089"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key3"));
        update(rm, comment, UTF8Type.instance.decompose("165 clydie oval apt. 399"), System.currentTimeMillis());
        rm.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 Rogahn Valleys")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 ROgAhn VallEYs")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 rogahn valleys")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("577 rogahn")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("57")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 Beverly Course")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 BEVERly COURSE")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 beverly course")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("89809 Beverly")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("8980")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdie OvAl APT. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 Clydie Oval Apt. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 clydie oval apt. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdie OvA")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165 ClYdi")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(comment, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("165")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testPrefixSSTableLookup()
    {
        // This test coverts particular case which interval lookup can return invalid results
        // when queried on the prefix e.g. "j".
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer name = UTF8Type.instance.decompose("first_name_prefix");

        Mutation rm;

        rm = new Mutation(KS_NAME, decoratedKey("key1"));
        update(rm, name, UTF8Type.instance.decompose("Pavel"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key2"));
        update(rm, name, UTF8Type.instance.decompose("Jordan"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key3"));
        update(rm, name, UTF8Type.instance.decompose("Mikhail"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key4"));
        update(rm, name, UTF8Type.instance.decompose("Michael"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key5"));
        update(rm, name, UTF8Type.instance.decompose("Johnny"), System.currentTimeMillis());
        rm.apply();

        // first flush would make interval for name - 'johnny' -> 'pavel'
        store.forceBlockingFlush();

        rm = new Mutation(KS_NAME, decoratedKey("key6"));
        update(rm, name, UTF8Type.instance.decompose("Jason"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key7"));
        update(rm, name, UTF8Type.instance.decompose("Vijay"), System.currentTimeMillis());
        rm.apply();

        rm = new Mutation(KS_NAME, decoratedKey("key8")); // this name is going to be tokenized
        update(rm, name, UTF8Type.instance.decompose("Jean-Claude"), System.currentTimeMillis());
        rm.apply();

        // this flush is going to produce range - 'jason' -> 'vijay'
        store.forceBlockingFlush();

        // make sure that overlap of the prefixes is properly handled across sstables
        // since simple interval tree lookup is not going to cover it, prefix lookup actually required.

        Set<String> rows;

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("J")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key5", "key6", "key8"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("j")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key5", "key6", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("m")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("v")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key7" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("p")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("j")),
                                     buildExpression(name, Operator.NEQ, UTF8Type.instance.decompose("joh")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key6", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("pavel")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Pave")));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Pavel")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("JeAn")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("claUde")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Jean")));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, buildExpression(name, Operator.EQ, UTF8Type.instance.decompose("Jean-Claude")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key8" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testSettingIsLiteralOption()
    {

        // special type which is UTF-8 but is only on the inside
        AbstractType<?> stringType = new AbstractType<String>(AbstractType.ComparisonType.CUSTOM)
        {
            public ByteBuffer fromString(String source) throws MarshalException
            {
                return UTF8Type.instance.fromString(source);
            }

            public Term fromJSONObject(Object parsed) throws MarshalException
            {
                throw new UnsupportedOperationException();
            }

            public TypeSerializer<String> getSerializer()
            {
                return UTF8Type.instance.getSerializer();
            }

            public int compareCustom(ByteBuffer a, ByteBuffer b)
            {
                return UTF8Type.instance.compare(a, b);
            }
        };

        // first let's check that we get 'false' for 'isLiteral' if we don't set the option with special comparator
        ColumnDefinition columnA = ColumnDefinition.regularDef(KS_NAME, CF_NAME, "special-A", stringType);

        ColumnIndex indexA = new ColumnIndex(UTF8Type.instance, columnA, IndexMetadata.fromSchemaMetadata("special-index-A", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertEquals(true,  indexA.isIndexed());
        Assert.assertEquals(false, indexA.isLiteral());

        // now let's double-check that we do get 'true' when we set it
        ColumnDefinition columnB = ColumnDefinition.regularDef(KS_NAME, CF_NAME, "special-B", stringType);

        ColumnIndex indexB = new ColumnIndex(UTF8Type.instance, columnB, IndexMetadata.fromSchemaMetadata("special-index-B", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put("is_literal", "true");
        }}));

        Assert.assertEquals(true, indexB.isIndexed());
        Assert.assertEquals(true, indexB.isLiteral());

        // and finally we should also get a 'true' if it's built-in UTF-8/ASCII comparator
        ColumnDefinition columnC = ColumnDefinition.regularDef(KS_NAME, CF_NAME, "special-C", UTF8Type.instance);

        ColumnIndex indexC = new ColumnIndex(UTF8Type.instance, columnC, IndexMetadata.fromSchemaMetadata("special-index-C", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertEquals(true, indexC.isIndexed());
        Assert.assertEquals(true, indexC.isLiteral());

        ColumnDefinition columnD = ColumnDefinition.regularDef(KS_NAME, CF_NAME, "special-D", AsciiType.instance);

        ColumnIndex indexD = new ColumnIndex(UTF8Type.instance, columnD, IndexMetadata.fromSchemaMetadata("special-index-D", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
        }}));

        Assert.assertEquals(true, indexD.isIndexed());
        Assert.assertEquals(true, indexD.isLiteral());

        // and option should supersedes the comparator type
        ColumnDefinition columnE = ColumnDefinition.regularDef(KS_NAME, CF_NAME, "special-E", UTF8Type.instance);

        ColumnIndex indexE = new ColumnIndex(UTF8Type.instance, columnE, IndexMetadata.fromSchemaMetadata("special-index-E", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put("is_literal", "false");
        }}));

        Assert.assertEquals(true,  indexE.isIndexed());
        Assert.assertEquals(false, indexE.isLiteral());
    }

    @Test
    public void testClusteringIndexes() throws Exception
    {
        testClusteringIndexes(false);
        cleanupData();
        testClusteringIndexes(true);
    }

    public void testClusteringIndexes(boolean forceFlush) throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME);

        executeCQL("INSERT INTO %s.%s (name, location, age, height, score) VALUES (?, ?, ?, ?, ?)", "Pavel", "US", 27, 183, 1.0);
        executeCQL("INSERT INTO %s.%s (name, location, age, height, score) VALUES (?, ?, ?, ?, ?)", "Pavel", "BY", 28, 182, 2.0);
        executeCQL("INSERT INTO %s.%s (name, location, age, height, score) VALUES (?, ?, ?, ?, ?)", "Jordan", "US", 27, 182, 1.0);

        if (forceFlush)
            store.forceBlockingFlush();

        UntypedResultSet results;

        results = executeCQL("SELECT * FROM %s.%s WHERE location = ? ALLOW FILTERING", "US");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE age >= ? AND height = ? ALLOW FILTERING", 27, 182);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE age = ? AND height = ? ALLOW FILTERING", 28, 182);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE age >= ? AND height = ? AND score >= ? ALLOW FILTERING", 27, 182, 1.0);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE age >= ? AND height = ? AND score = ? ALLOW FILTERING", 27, 182, 1.0);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location = ? AND age >= ? ALLOW FILTERING", "US", 27);
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location = ? ALLOW FILTERING", "BY");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location LIKE 'U%%' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location LIKE 'U%%' AND height >= 183 ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location LIKE 'US%%' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        results = executeCQL("SELECT * FROM %s.%s WHERE location LIKE 'US' ALLOW FILTERING");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        try
        {
            executeCQL("SELECT * FROM %s.%s WHERE location LIKE '%%U' ALLOW FILTERING");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage().contains("only supported"));
            // expected
        }

        try
        {
            executeCQL("SELECT * FROM %s.%s WHERE location LIKE '%%' ALLOW FILTERING");
            Assert.fail();
        }
        catch (SyntaxException e)
        {
            Assert.assertTrue(e.getMessage().contains("empty"));
            // expected
        }

        try
        {
            executeCQL("SELECT * FROM %s.%s WHERE location LIKE '%%%%' ALLOW FILTERING");
            Assert.fail();
        }
        catch (SyntaxException e)
        {
            Assert.assertTrue(e.getMessage().contains("empty"));
            // expected
        }
    }

    @Test
    public void testInvalidIndexOptions()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        try
        {
            // invalid index mode
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "address"); put("mode", "NORMAL"); }},
                                      store.metadata);
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("Incorrect index mode"));
        }

        try
        {
            // invalid SPARSE on the literal index
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "address"); put("mode", "SPARSE"); }},
                                      store.metadata);
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("non-literal"));
        }

        try
        {
            // invalid SPARSE on the explicitly literal index
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "height"); put("mode", "SPARSE"); put("is_literal", "true"); }},
                    store.metadata);
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("non-literal"));
        }

        try
        {
            //  SPARSE with analyzer
            SASIIndex.validateOptions(new HashMap<String, String>()
                                      {{ put("target", "height"); put("mode", "SPARSE"); put("analyzed", "true"); }},
                                      store.metadata);
            Assert.fail();
        }
        catch (ConfigurationException e)
        {
            Assert.assertTrue(e.getMessage().contains("doesn't support analyzers"));
        }
    }

    @Test
    public void testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes()
    {
        String containsTable = "sasi_like_contains_test";
        String prefixTable = "sasi_like_prefix_test";
        String analyzedPrefixTable = "sasi_like_analyzed_prefix_test";

        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, containsTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, prefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int primary key, v text);", KS_NAME, analyzedPrefixTable));

        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'CONTAINS' };", KS_NAME, containsTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'PREFIX' };", KS_NAME, prefixTable));
        QueryProcessor.executeOnceInternal(String.format("CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s(v) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'PREFIX', 'analyzed': 'true' };", KS_NAME, analyzedPrefixTable));

        testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(containsTable, prefixTable, analyzedPrefixTable, false);
        testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(containsTable, prefixTable, analyzedPrefixTable, true);
    }

    private void testLIKEAndEQSemanticsWithDifferenceKindsOfIndexes(String containsTable,
                                                                    String prefixTable,
                                                                    String analyzedPrefixTable,
                                                                    boolean forceFlush)
    {
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, containsTable), 0, "Pavel");
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, prefixTable), 0, "Jean-Claude");
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?);", KS_NAME, analyzedPrefixTable), 0, "Jean-Claude");

        if (forceFlush)
        {
            Keyspace keyspace = Keyspace.open(KS_NAME);
            for (String table : Arrays.asList(containsTable, prefixTable, analyzedPrefixTable))
                keyspace.getColumnFamilyStore(table).forceBlockingFlush();
        }

        UntypedResultSet results;

        // CONTAINS

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pavel';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Pav';", KS_NAME, containsTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since CONTAINS indexes only support LIKE
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Pav%%';", KS_NAME, containsTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since CONTAINS indexes only support LIKE '%<term>' and LIKE '%<term>%'
        }

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Pav';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Pav%%';", KS_NAME, containsTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // PREFIX

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean-Claude';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jea';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jea%%';", KS_NAME, prefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jea';", KS_NAME, prefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%'
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jea%%';", KS_NAME, prefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%'
        }

        // PREFIX + analyzer

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v = 'Jean';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support EQ without tokenization
        }

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Claude';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean-Claude';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Jean%%';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        results = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE 'Claude%%';", KS_NAME, analyzedPrefixTable));
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Jean';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%' and LIKE '<term>'
        }

        try
        {
            QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE v LIKE '%%Claude%%';", KS_NAME, analyzedPrefixTable));
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            // expected since PREFIX indexes only support LIKE '<term>%' and LIKE '<term>'
        }

        for (String table : Arrays.asList(containsTable, prefixTable, analyzedPrefixTable))
            QueryProcessor.executeOnceInternal(String.format("TRUNCATE TABLE %s.%s", KS_NAME, table));
    }

    @Test
    public void testIndexMemtableSwitching()
    {
        // write some data but don't flush
        ColumnFamilyStore store = loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
        }}, false);

        ColumnIndex index = ((SASIIndex) store.indexManager.getIndexByName("first_name")).getIndex();
        IndexMemtable beforeFlushMemtable = index.getCurrentMemtable();

        PartitionRangeReadCommand command = new PartitionRangeReadCommand(store.metadata,
                                                                          FBUtilities.nowInSeconds(),
                                                                          ColumnFilter.all(store.metadata),
                                                                          RowFilter.NONE,
                                                                          DataLimits.NONE,
                                                                          DataRange.allData(store.getPartitioner()),
                                                                          Optional.empty(),
                                                                          ReadCommand.PostReconciliationProcessor.DEFAULT);

        QueryController controller = new QueryController(store, command, Integer.MAX_VALUE);
        org.apache.cassandra.index.sasi.plan.Expression expression =
                new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                                                    .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Pavel"));

        Assert.assertTrue(beforeFlushMemtable.search(expression).getCount() > 0);

        store.forceBlockingFlush();

        IndexMemtable afterFlushMemtable = index.getCurrentMemtable();

        Assert.assertNotSame(afterFlushMemtable, beforeFlushMemtable);
        Assert.assertNull(afterFlushMemtable.search(expression));
        Assert.assertEquals(0, index.getPendingMemtables().size());

        loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key2", Pair.create("Sam", 15));
        }}, false);

        expression = new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                        .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Sam"));

        beforeFlushMemtable = index.getCurrentMemtable();
        Assert.assertTrue(beforeFlushMemtable.search(expression).getCount() > 0);

        // let's emulate switching memtable and see if we can still read-data in "pending"
        index.switchMemtable(store.getTracker().getView().getCurrentMemtable());

        Assert.assertNotSame(index.getCurrentMemtable(), beforeFlushMemtable);
        Assert.assertEquals(1, index.getPendingMemtables().size());

        Assert.assertTrue(index.searchMemtable(expression).getCount() > 0);

        // emulate "everything is flushed" notification
        index.discardMemtable(store.getTracker().getView().getCurrentMemtable());

        Assert.assertEquals(0, index.getPendingMemtables().size());
        Assert.assertNull(index.searchMemtable(expression));

        // test discarding data from memtable
        loadData(new HashMap<String, Pair<String, Integer>>()
        {{
            put("key3", Pair.create("Jonathan", 16));
        }}, false);

        expression = new org.apache.cassandra.index.sasi.plan.Expression(controller, index)
                .add(Operator.LIKE_MATCHES, UTF8Type.instance.fromString("Jonathan"));

        Assert.assertTrue(index.searchMemtable(expression).getCount() > 0);

        index.switchMemtable();
        Assert.assertNull(index.searchMemtable(expression));
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, boolean forceFlush)
    {
        return loadData(data, System.currentTimeMillis(), forceFlush);
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, long timestamp, boolean forceFlush)
    {
        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
            newMutation(e.getKey(), e.getValue().left, null, e.getValue().right, timestamp).apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        if (forceFlush)
            store.forceBlockingFlush();

        return store;
    }

    private void cleanupData()
    {
        Keyspace ks = Keyspace.open(KS_NAME);
        ks.getColumnFamilyStore(CF_NAME).truncateBlocking();
        ks.getColumnFamilyStore(CLUSTERING_CF_NAME).truncateBlocking();
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, int maxResults, Expression... expressions)
    {
        return getIndexed(store, ColumnFilter.all(store.metadata), maxResults, expressions);
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, ColumnFilter columnFilter, int maxResults, Expression... expressions)
    {
        return getKeys(getIndexed(store, columnFilter, null, maxResults, expressions));
    }

    private static Set<DecoratedKey> getPaged(ColumnFamilyStore store, int pageSize, Expression... expressions)
    {
        UnfilteredPartitionIterator currentPage;
        Set<DecoratedKey> uniqueKeys = new TreeSet<>();

        DecoratedKey lastKey = null;

        int count;
        do
        {
            count = 0;
            currentPage = getIndexed(store, ColumnFilter.all(store.metadata), lastKey, pageSize, expressions);
            if (currentPage == null)
                break;

            while (currentPage.hasNext())
            {
                try (UnfilteredRowIterator row = currentPage.next())
                {
                    uniqueKeys.add(row.partitionKey());
                    lastKey = row.partitionKey();
                    count++;
                }
            }

            currentPage.close();
        }
        while (count == pageSize);

        return uniqueKeys;
    }

    private static UnfilteredPartitionIterator getIndexed(ColumnFamilyStore store, ColumnFilter columnFilter, DecoratedKey startKey, int maxResults, Expression... expressions)
    {
        DataRange range = (startKey == null)
                            ? DataRange.allData(PARTITIONER)
                            : DataRange.forKeyRange(new Range<>(startKey, PARTITIONER.getMinimumToken().maxKeyBound()));

        RowFilter filter = RowFilter.create();
        for (Expression e : expressions)
            filter.add(store.metadata.getColumnDefinition(e.name), e.op, e.value);

        ReadCommand command = new PartitionRangeReadCommand(store.metadata,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            filter,
                                                            DataLimits.thriftLimits(maxResults, DataLimits.NO_LIMIT),
                                                            range,
                                                            Optional.empty(),
                                                            ReadCommand.PostReconciliationProcessor.DEFAULT);

        return command.executeLocally(command.executionController());
    }

    private static Mutation newMutation(String key, String firstName, String lastName, int age, long timestamp)
    {
        Mutation rm = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose(key)));
        List<Cell> cells = new ArrayList<>(3);

        if (age >= 0)
            cells.add(buildCell(ByteBufferUtil.bytes("age"), Int32Type.instance.decompose(age), timestamp));
        if (firstName != null)
            cells.add(buildCell(ByteBufferUtil.bytes("first_name"), UTF8Type.instance.decompose(firstName), timestamp));
        if (lastName != null)
            cells.add(buildCell(ByteBufferUtil.bytes("last_name"), UTF8Type.instance.decompose(lastName), timestamp));

        update(rm, cells);
        return rm;
    }

    private static Set<String> getKeys(final UnfilteredPartitionIterator rows)
    {
        try
        {
            return new TreeSet<String>()
            {{
                while (rows.hasNext())
                {
                    try (UnfilteredRowIterator row = rows.next())
                    {
                        if (!row.isEmpty())
                            add(AsciiType.instance.compose(row.partitionKey().getKey()));
                    }
                }
            }};
        }
        finally
        {
            rows.close();
        }
    }

    private static List<String> convert(final Set<DecoratedKey> keys)
    {
        return new ArrayList<String>()
        {{
            for (DecoratedKey key : keys)
                add(AsciiType.instance.getString(key.getKey()));
        }};
    }

    private UntypedResultSet executeCQL(String query, Object... values)
    {
        return QueryProcessor.executeOnceInternal(String.format(query, KS_NAME, CLUSTERING_CF_NAME), values);
    }

    private Set<String> executeCQLWithKeys(String rawStatement) throws Exception
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(rawStatement).prepare().statement;
        ResultMessage.Rows cqlRows = statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT);

        Set<String> results = new TreeSet<>();
        for (CqlRow row : cqlRows.toThriftResult().getRows())
        {
            for (org.apache.cassandra.thrift.Column col : row.columns)
            {
                String columnName = UTF8Type.instance.getString(col.bufferForName());
                if (columnName.equals("id"))
                    results.add(AsciiType.instance.getString(col.bufferForValue()));
            }
        }

        return results;
    }

    private static DecoratedKey decoratedKey(ByteBuffer key)
    {
        return PARTITIONER.decorateKey(key);
    }

    private static DecoratedKey decoratedKey(String key)
    {
        return decoratedKey(AsciiType.instance.fromString(key));
    }

    private static Row buildRow(Collection<Cell> cells)
    {
        return buildRow(cells.toArray(new Cell[cells.size()]));
    }

    private static Row buildRow(Cell... cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(Clustering.EMPTY);
        for (Cell c : cells)
            rowBuilder.addCell(c);
        return rowBuilder.build();
    }

    private static Cell buildCell(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        CFMetaData cfm = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata;
        return BufferCell.live(cfm, cfm.getColumnDefinition(name), timestamp, value);
    }

    private static Cell buildCell(CFMetaData cfm, ByteBuffer name, ByteBuffer value, long timestamp)
    {
        ColumnDefinition column = cfm.getColumnDefinition(name);
        assert column != null;
        return BufferCell.live(cfm, column, timestamp, value);
    }

    private static Expression buildExpression(ByteBuffer name, Operator op, ByteBuffer value)
    {
        return new Expression(name, op, value);
    }

    private static void update(Mutation rm, ByteBuffer name, ByteBuffer value, long timestamp)
    {
        CFMetaData metadata = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata;
        rm.add(PartitionUpdate.singleRowUpdate(metadata, rm.key(), buildRow(buildCell(metadata, name, value, timestamp))));
    }


    private static void update(Mutation rm, List<Cell> cells)
    {
        CFMetaData metadata = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata;
        rm.add(PartitionUpdate.singleRowUpdate(metadata, rm.key(), buildRow(cells)));
    }

    private static class Expression
    {
        public final ByteBuffer name;
        public final Operator op;
        public final ByteBuffer value;

        public Expression(ByteBuffer name, Operator op, ByteBuffer value)
        {
            this.name = name;
            this.op = op;
            this.value = value;
        }
    }
}
