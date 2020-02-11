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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageProxy;

import static junit.framework.TestCase.fail;

public class RepairDigestTrackingTest extends DistributedTestBase implements Serializable
{

    @Test
    public void testInconsistenciesFound() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT, c INT, v INT, PRIMARY KEY (k,c)) with read_repair='NONE'");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl")));
            // Mark everything repaired on node2
            cluster.get(2).runOnInstance(() -> markAllRepaired("tbl"));
            cluster.get(2).runOnInstance(() -> assertRepaired("tbl"));
            // now overwrite on node1 only to generate digest mismatches
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 55);
            cluster.get(1).runOnInstance(() -> assertNotRepaired("tbl"));

            Object[][] expected = new Object[20][];
            for (int i=0; i<20; i++)
                expected[i] = new Object[] { 0, i, i };
            expected[5] = new Object[] { 0, 5, 55 };

            // Execute a range read and assert inconsistency is detected (as nothing is repaired on node1)
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL), expected);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should differ by 1 after range read", ccBefore + 1, ccAfter);

            // Re-introduce a mismatch and run again as a single partition read
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 555);
            expected[5] = new Object[] { 0, 5, 555 };
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0", ConsistencyLevel.ALL), expected);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should differ by 1 after partition read", ccBefore + 2, ccAfter);
        }
    }

    @Test
    public void testPurgeableTombstonesAreIgnored() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl2 (k INT, c INT, v1 INT, v2 INT, PRIMARY KEY (k,c)) WITH gc_grace_seconds=0");
            // on node1 only insert some tombstones, then flush
            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("DELETE v1 FROM " + KEYSPACE + ".tbl2 USING TIMESTAMP 0 WHERE k=0 and c=? ", i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            // insert data on both nodes and flush
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (0, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl2")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl2")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl2")));

            // now overwrite on node2 only to generate digest mismatches, but don't flush so the repaired dataset is not affected
            for (int i = 0; i < 10; i++)
            {
                cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (0, ?, ?) USING TIMESTAMP 2", i, i * 2);
            }

            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl2");
            // Unfortunately we need to sleep here to ensure that nowInSec > the local deletion time of the tombstones
            TimeUnit.SECONDS.sleep(2);
            Object[][] expected = new Object[10][];
            for (int i=0; i<10; i++)
                expected[i] = new Object[] { 0, i, null, i * 2};

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl2", ConsistencyLevel.ALL), expected);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl2");
            Assert.assertEquals("No repaired data inconsistencies should be detected", ccBefore, ccAfter);
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialUnderread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read less repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl3 (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl3")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl3")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl3")));

            // Add some unrepaired data to both nodes
            for (int i=20; i<30; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            // And some more unrepaired data to node2 only. This causes node2 to read less repaired data than node1
            // when satisfying the limits of the read. So node2 needs to overread more repaired data than node1 when
            // calculating the repaired data digest.
            cluster.get(2).executeInternal("INSERT INTO "  + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?) ", 30, 30);

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl3");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl3 WHERE k=1 LIMIT 20", ConsistencyLevel.ALL),
                       rows(1, 30, 11));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1), "tbl3");

            // Recreate a mismatch in unrepaired data and verify partition range read
            cluster.get(2).executeInternal("INSERT INTO "  + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?)", 31, 31);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl3 LIMIT 30", ConsistencyLevel.ALL),
                       rows(1, 31, 2));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1), "tbl3");

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialOverread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read more repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl4 (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 0",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl4")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl4")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl4")));

            // Add some unrepaired data to both nodes
            for (int i=10; i<13; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // And some row deletions on node2 only which cover data in the repaired set
            // This will cause node2 to read more repaired data in satisfying the limit of the read request
            // so it should overread less than node1 (in fact, it should not overread at all) in order to
            // calculate the repaired data digest.
            for (int i=7; i<10; i++)
            {
                cluster.get(2).executeInternal("DELETE FROM " + KEYSPACE + ".tbl4 USING TIMESTAMP 2 WHERE k = 0 AND c = ?", i);
                cluster.get(2).executeInternal("DELETE FROM " + KEYSPACE + ".tbl4 USING TIMESTAMP 2 WHERE k = 1 AND c = ?", i);
            }

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl4");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl4 WHERE k=0 LIMIT 5", ConsistencyLevel.ALL),
                       rows(rows(0, 12, 10), rows(0, 6, 5)));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1), "tbl4");

            // Verify partition range read
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl4 LIMIT 11", ConsistencyLevel.ALL),
                       rows(rows(1, 12, 10), rows(1, 6, 0), rows(0, 12, 12)));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1), "tbl4");

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    private Object[][] rows(Object[][] head, Object[][]...tail)
    {
        return Stream.concat(Stream.of(head),
                             Stream.of(tail).flatMap(Stream::of))
                     .toArray(Object[][]::new);
    }

    private Object[][] rows(int partitionKey, int start, int end)
    {
        if (start == end)
            return new Object[][] { new Object[] { partitionKey, start, end } };

        IntStream clusterings = start > end
                              ? IntStream.range(end -1, start).map(i -> start - i + end - 1)
                              : IntStream.range(start, end);

        return clusterings.mapToObj(i -> new Object[] {partitionKey, i, i}).toArray(Object[][]::new);
    }

    private void assertNotRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(reader -> Assert.assertEquals("repaired at is set for sstable: " + reader.descriptor,
                                                       getRepairedAt(reader),
                                                       ActiveRepairService.UNREPAIRED_SSTABLE));
    }

    private void assertRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(reader -> Assert.assertTrue("repaired at is not set for sstable: " + reader.descriptor, getRepairedAt(reader) > 0));
    }

    private void markAllRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(this::markRepaired);
    }

    private long getRepairedAt(SSTableReader reader)
    {
        Descriptor descriptor = reader.descriptor;
        try
        {
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer()
                                                                      .deserialize(descriptor, EnumSet.of(MetadataType.STATS));

            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            return stats.repairedAt;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void markRepaired(SSTableReader reader) {
        Descriptor descriptor = reader.descriptor;
        try
        {
            descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, System.currentTimeMillis(), null, false);
            reader.reloadSSTableMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private long getConfirmedInconsistencies(IInvokableInstance instance, String table)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(table)
                                                     .metric
                                                     .confirmedRepairedInconsistencies
                                                     .table
                                                     .getCount());
    }

}
