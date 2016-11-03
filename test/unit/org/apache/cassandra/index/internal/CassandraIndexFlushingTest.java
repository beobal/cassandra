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

package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

@RunWith(BMUnitRunner.class)
public class CassandraIndexFlushingTest extends CQLTester
{
    // control the invocation of the byteman rules, we only want to start
    // pausing/blocking once the test setup is complete
    static final AtomicBoolean testStarted = new AtomicBoolean(false);

    // controls the execution of the memtable flush writer, the test uses it
    // to pause execution between flushing the base and index memtables
    static volatile CountDownLatch startFlushLatch = new CountDownLatch(1);
    static volatile CountDownLatch finishFlushLatch = new CountDownLatch(1);

    // counts down after the memory allocated to the base table memtable has been freed
    static volatile CountDownLatch reclaimLatch = new CountDownLatch(1);

    // XXX temp
    static volatile boolean result = false;

    static final String TABLE_NAME = "index_flush_test_tbl";
    static final String INDEX_NAME = "index_flush_test_idx";

    @Test
    @BMRules(rules = {
         @BMRule(name = "Signal base table memory reclaimed",
                 targetClass = "Memtable",
                 targetMethod = "setDiscarded",
                 targetLocation = "AT EXIT",
                 condition = "$this.cfs.metadata.cfName.equals(\"" + TABLE_NAME + "\") ",
                 action = "org.apache.cassandra.index.internal.CassandraIndexFlushingTest.signalReclaimed()"),

         @BMRule(name = "Pause flush writer",
                 targetClass="ColumnFamilyStore$Flush",
                 targetMethod="flushMemtable",
                 targetLocation="AT LINE 1153",
                 condition = "$memtable.cfs.metadata.cfName.equals(\"" + TABLE_NAME + '.' + INDEX_NAME + "\")",
                 action = "org.apache.cassandra.index.internal.CassandraIndexFlushingTest.coordinateIndexFlush()")
    })
    public void reproCorruption() throws Throwable
    {
        createTable(String.format("CREATE TABLE %s.%s (a int, b text, c text, PRIMARY KEY (a, b))", KEYSPACE, TABLE_NAME));
        execute(String.format("CREATE INDEX %s ON %s.%s(c)", INDEX_NAME, KEYSPACE, TABLE_NAME));
        waitForIndex(KEYSPACE, TABLE_NAME, INDEX_NAME);

        System.out.println("Marking test started");
        testStarted.set(true);

        execute(String.format("INSERT INTO %s.%s (a, b, c) VALUES (0, 'unindexed value', 'indexed value')", KEYSPACE, TABLE_NAME));
        System.out.println("====================================");

        ColumnFamilyStore baseCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_NAME);
        ColumnFamilyStore indexCfs = baseCfs.indexManager.getIndexByName(INDEX_NAME).getBackingTable().get();

        // grab the index memtables prior to flush, so we can verify that we pause
        // the flush thread before they're switched

        System.out.println(String.format("Base table has %s live and %s flushing memtables",
                                         baseCfs.getTracker().getView().liveMemtables.size(),
                                         baseCfs.getTracker().getView().flushingMemtables.size()));
        System.out.println(String.format("Index table has %s live and %s flushing memtables",
                                         indexCfs.getTracker().getView().liveMemtables.size(),
                                         indexCfs.getTracker().getView().flushingMemtables.size()));
        Memtable baseMemtable = baseCfs.getTracker().getView().getCurrentMemtable();
        Memtable indexMemtable = indexCfs.getTracker().getView().getCurrentMemtable();

        // Flush the base table (and so the index too). The flush itself is blocking, but the
        // reclaimation of memory from the memtables is performed asyncronously on a separate
        // executor. The first byteman rule ("Signal memory reclaimed") signals when this
        // reclaimation has happened for the main table's memtable.
        Thread flushThread = new Thread(baseCfs::forceBlockingFlush);
        flushThread.start();

        // The byteman rule "Pause flush writer" tells the flushwriter to trigger a latch just
        // before it switches the memtable for the index. We also need a second latch, to signal
        // that the memory allocated to the base memtable has been reclaimed.
        startFlushLatch.await();
        reclaimLatch.await();

        // At this point, the base memtable has been switched and the memory allocated to it
        // freed, but the index memtable is still in the flushing state - and so will be read
        // when answering queries.
        System.out.println("Flush blocked = " + result);
        System.out.println(String.format("Base table has %s live and %s flushing memtables",
                                         baseCfs.getTracker().getView().liveMemtables.size(),
                                         baseCfs.getTracker().getView().flushingMemtables.size()));
        System.out.println(String.format("Index table has %s live and %s flushing memtables",
                                         indexCfs.getTracker().getView().liveMemtables.size(),
                                         indexCfs.getTracker().getView().flushingMemtables.size()));
        // do queries etc to repro #12590

        Iterator<PartitionPosition> keys = indexMemtable.allPartitionKeys();
        while (keys.hasNext())
        {
            DecoratedKey key = (DecoratedKey)keys.next();
            Token t = key.getToken();
            Object o = key.getToken().getTokenValue();
            ByteBuffer b = (ByteBuffer)o;
            System.out.println(String.format("Index token (%s) == %s", t.getClass().getName(), o.toString()));
            System.out.println(String.format("IsDirect = %s (%s)", b.isDirect(), b.getClass().getName()));
            System.out.println(String.format("Token value = %s", t.toString()));
            Partition p = indexMemtable.getPartition(key);
            try (UnfilteredRowIterator rows = p.unfilteredIterator())
            {
                if (rows.isEmpty())
                {
                    System.out.println("Index partition contains no rows");
                }
                else
                {
                    Row row = (Row) rows.next();
                    System.out.println(String.format("Index row: %s", row.toString(indexCfs.metadata)));
                }
            }
        }

        System.out.println("Executing index queries");
        for (int i = 0; i < 10; i++)
        {
            System.out.println(String.format("Executing index query (%s)", i));
            execute(String.format("SELECT * FROM %s.%s WHERE c='indexed value'", KEYSPACE, TABLE_NAME));
        }

        // Now we can go ahead and flip the latch to let the flush writer finish switching
        // out the index memtable.
        System.out.println("Counting down latch");
        finishFlushLatch.countDown();
        System.out.println("Flush blocked = " + result);
    }

    public static boolean testStarted()
    {
        return testStarted.get();
    }

    public static void signalReclaimed()
    {
        if (!testStarted())
        {
            System.out.println("BASE MT: Test not started, not waiting");
            return;
        }

        System.out.println("BASE MT: Signalling base memtable memory freed");
        reclaimLatch.countDown();
    }

    public static void coordinateIndexFlush()
    {
        if (!testStarted())
        {
            System.out.println("INDEX: Test not started, not waiting");
            return;
        }

        System.out.println("INDEX: Waiting before proceeding with index memtable flush");
        try
        {
            startFlushLatch.countDown();
            result = true;
            finishFlushLatch.await();
            result = false;
        }
        catch (InterruptedException e)
        {
        }
        System.out.println("INDEX: Done waiting");
    }
}

