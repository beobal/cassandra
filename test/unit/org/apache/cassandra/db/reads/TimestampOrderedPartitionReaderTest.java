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

package org.apache.cassandra.db.reads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimestampOrderedPartitionReaderTest
{

    private static final String KEYSPACE = "TimestampOrderedPartitionReaderTest";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        TableMetadata.Builder table = TableMetadata.builder(KEYSPACE, CF)
                                                   .addPartitionKeyColumn("key", BytesType.instance)
                                                   .addClusteringColumn("col", AsciiType.instance)
                                                   .addRegularColumn("a", AsciiType.instance)
                                                   .addRegularColumn("b", AsciiType.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    table);
    }

    @Test
    public void testSSTablesReadToSatisfyColumnFilter()
    {
        // with repaired data tracking enabled we need to read all sstables for the
        // key as the repaired tables have to be merged to generate a digest
        // with tracking not enabled, once all the requested columns are read, the
        // read can return
        DecoratedKey key = Util.dk("key");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, key.getKey())
                            .clustering("cc")
                            .add("a", ByteBufferUtil.bytes("abcd"))
                            .build()
                            .apply();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 1, key.getKey())
                            .clustering("cc")
                            .add("a", ByteBufferUtil.bytes("wxyz"))
                            .build()
                            .apply();
        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        // mark tables repaired so that the repaired data info has something to track
        sstables.forEach(this::markRepaired);

        // without repaired data tracking, the reader can return after the 1st sstable
        assertReadResultAndSSTableReadCount(cfs, key, null, true, 1);

        // with tracking, we must touch both sstables but the query results obviously remain the same
        TestRepairedDataInfo repairedDataInfo = new TestRepairedDataInfo();
        assertReadResultAndSSTableReadCount(cfs, key, repairedDataInfo, true, 2);
        // repaired data info isn't marked inconclusive as we didn't skip any sstables
        assertFalse(repairedDataInfo.markedInconclusive);
    }

    @Test
    public void testSSTablesReadWithPartitionDeletions()
    {
        // when partition deletions shadow data from sstables, the reader may return
        // early as no further data can be read from sstables with earlier max timestamps
        // with repaired data tracking enabled we mark the repaired data info as
        // inconclusive to avoid false positives between replicas
        DecoratedKey key = Util.dk("key");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata(), 0, key.getKey())
                            .clustering("cc")
                            .add("a", ByteBufferUtil.bytes("abcd"))
                            .build()
                            .apply();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata(), 1, key.getKey())
                            .clustering("cc")
                            .add("a", ByteBufferUtil.bytes("wxyz"))
                            .build()
                            .apply();
        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        // mark tables repaired so that the repaired data info has something to track
        sstables.forEach(this::markRepaired);

        // Add a partition deletion, first to the memtable so that no sstables are read
        new Mutation(PartitionUpdate.simpleBuilder(cfs.metadata(), key.getKey())
                                    .delete()
                                    .build()).apply();
        assertReadResultAndSSTableReadCount(cfs, key, null, false,  0);

        // same behaviour with repaired data tracking, but mark the info inconclusive
        TestRepairedDataInfo repairedDataInfo = new TestRepairedDataInfo();
        assertReadResultAndSSTableReadCount(cfs, key, repairedDataInfo, false, 0);
        assertTrue(repairedDataInfo.markedInconclusive);

        // Now flush, so that only the most recent sstable gets touched
        cfs.forceBlockingFlush();
        assertReadResultAndSSTableReadCount(cfs, key, null, false, 1);

        // same behaviour with repaired data tracking, but mark the info inconclusive
        repairedDataInfo = new TestRepairedDataInfo();
        assertReadResultAndSSTableReadCount(cfs, key, repairedDataInfo, false, 1);
        assertTrue(repairedDataInfo.markedInconclusive);

    }

    private void markRepaired(SSTableReader sstable)
    {
        long repairedAt = 1;  // !ActiveRepairService.UNREPAIRED_SSTABLE
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, null);
            sstable.reloadSSTableMetadata();
        }
        catch (IOException e)
        {
            fail("Unexpected IOException mutating repaired status of table " + sstable);
        }
    }

    private void assertReadResultAndSSTableReadCount(ColumnFamilyStore cfs,
                                                     DecoratedKey key,
                                                     RepairedDataInfo repairedDataInfo,
                                                     boolean expectData,
                                                     int expectedSSTableCount)
    {
        TestSSTableReadsListener listener = new TestSSTableReadsListener();
        ReadCommand readCommand = Util.cmd(cfs, key).includeRow("cc").columns("a").build();
        ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter)readCommand.clusteringIndexFilter(key);
        ColumnFilter columnFilter = readCommand.columnFilter();

        TimestampOrderedPartitionReader.Builder builder = new TimestampOrderedPartitionReader.Builder(cfs, key, filter, columnFilter, listener);
        if (repairedDataInfo != null)
            builder = builder.withRepairedDataTracking(repairedDataInfo);

        try (UnfilteredRowIterator partition = builder.build()
                                                      .readPartition()
                                                      .unfilteredIterator(columnFilter, Slices.ALL, false))
        {

            // if some data is expected, regardless of whether repaired data tracking is enabled the result should be the same
            if (expectData)
            {
                assertTrue(partition.hasNext());
                Unfiltered u = partition.next();
                assertTrue(u.isRow());
                assertEquals(((Row) u).getCell(cfs.metadata().regularColumns().getSimple(0)).value(), ByteBufferUtil.bytes("wxyz"));
                assertFalse(partition.hasNext());
            }
            else
            {
                assertFalse(partition.partitionLevelDeletion().isLive());
            }

            // if repaired data tracking is enabled, expect to read both sstables
            assertEquals(expectedSSTableCount, listener.count);
        }
    }

    private static class TestSSTableReadsListener implements SSTableReadsListener
    {
        int count = 0;
        public void onSSTableSelected(SSTableReader sstable, RowIndexEntry<?> indexEntry, SelectionReason reason)
        {
            count++;
        }
    }

    private static class TestRepairedDataInfo implements RepairedDataInfo
    {
        boolean markedInconclusive = false;
        public void markInconclusive()
        {
            markedInconclusive = true;
        }
    }
}
