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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;

public class SSTableUpgradeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableUpgradeTest.class);

    private static final String KEYSPACE = "legacy_tables";

    private static final String COMPACT_TABLE = "legacy_ka_compact_multi_block_rt";
    private static final String COMPACT_DDL = String.format("CREATE TABLE %s.%s (k int, c1 int, c2 int, v1 blob, v2 blob, PRIMARY KEY (k, c1, c2))", KEYSPACE, COMPACT_TABLE);

    private static final String FLUSH_TABLE = "legacy_ka_flush_multi_block_rt";
    private static final String FLUSH_DDL = String.format("CREATE TABLE %s.%s (k int, c1 int, c2 int, v1 blob, v2 blob, PRIMARY KEY (k, c1, c2))", KEYSPACE, FLUSH_TABLE);

    private static final Random random = new Random(0);

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        KSMetaData ksm = KSMetaData.testMetadata(KEYSPACE, SimpleStrategy.class, KSMetaData.optsWithRF(1),
                                                 CFMetaData.compile(COMPACT_DDL, KEYSPACE),
                                                 CFMetaData.compile(FLUSH_DDL, KEYSPACE)
        );
        MigrationManager.announceNewKeyspace(ksm);
    }

    private ByteBuffer createBuffer(int size)
    {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static void copySSTables(ColumnFamilyStore cfs, String destination) throws IOException
    {
        cfs.forceBlockingFlush();
        File dstDir = new File(destination);
        if (dstDir.exists())
            FileUtils.deleteRecursive(dstDir);
        dstDir.mkdirs();

        for (File srcDir : cfs.directories.getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                if (!file.isFile())
                    continue;
                File dstFile = new File(dstDir, file.getName());
                Files.copy(file.toPath(), dstFile.toPath());
            }
        }
    }

    private static String destination(String tableName)
    {
        return "/Users/blakeeggleston/code/cassandra-3/test/data/legacy-sstables/ka/legacy_tables/" + tableName;
    }

    @Test
    public void exportCompactedMultiBlock() throws Exception
    {
        String dir = destination(COMPACT_TABLE);
        logger.info(System.getProperties().toString());
        int k = 100;

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(COMPACT_TABLE);
        cfs.disableAutoCompaction();
        String insert = String.format("INSERT INTO %s.%s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", KEYSPACE, COMPACT_TABLE);
        String delete = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE k=100 AND c1 = ?", KEYSPACE, COMPACT_TABLE);
        int size = DatabaseDescriptor.getColumnIndexSize();
        // insert row(0:0) & row(1:1) - these are small
        QueryProcessor.executeOnceInternal(insert, k, 0, 0, createBuffer(100), createBuffer(100), 100L);
        QueryProcessor.executeOnceInternal(insert, k, 1, 1, createBuffer(100), createBuffer(100), 100L);

        // now an RT covering all of the row(2,*) range [2:_, 2:!]
        QueryProcessor.executeOnceInternal(delete, 200L, 2);

        String pointDelete = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE k=100 AND c1 = ? AND c2 = ?", KEYSPACE, COMPACT_TABLE);
        QueryProcessor.executeOnceInternal(pointDelete, 150L, 2, 0);

        // now some large rows with c1 = 2 which push that RT across multiple index blocks
        QueryProcessor.executeOnceInternal(insert, k, 2, 0, createBuffer(size+1), createBuffer(size+1), 300L);
        QueryProcessor.executeOnceInternal(insert, k, 2, 1, createBuffer(100), createBuffer(100), 300L);
        QueryProcessor.executeOnceInternal(insert, k, 2, 2, createBuffer(size+1), createBuffer(size+1), 300L);

        // another small row(3:0) which follows the large RT, so it can be closed
        QueryProcessor.executeOnceInternal(insert, k, 3, 0, createBuffer(100), createBuffer(100), 300L);
        // finally, another RT, so that the last block ends with an RT (related to the second part of the problem,
        // generating an unbounded RT bound because we don't fully exhaus OldFormatDeserializer - not necessary for
        // the main issue)
        QueryProcessor.executeOnceInternal(delete, 100L, 4);
        cfs.forceBlockingFlush();
        QueryProcessor.executeOnceInternal(insert, k, 2, 3, createBuffer(100), createBuffer(100), 300L);
        cfs.forceBlockingFlush();
        Assert.assertEquals(2, cfs.getLiveSSTableCount());
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable: cfs.getSSTables())
        {
            descriptors.add(sstable.descriptor);
        }
        CompactionManager.instance.submitUserDefined(cfs,descriptors, Integer.MIN_VALUE).get();

        Assert.assertEquals(1, cfs.getLiveSSTableCount());

        copySSTables(cfs, dir);
    }

    @Test
    public void exportFlushedMultiBlock() throws Exception
    {
        String dir = destination(FLUSH_TABLE);
        logger.info(System.getProperties().toString());
        int k = 100;

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(FLUSH_TABLE);
        cfs.disableAutoCompaction();
        String insert = String.format("INSERT INTO %s.%s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", KEYSPACE, FLUSH_TABLE);
        String delete = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE k=100 AND c1 = ?", KEYSPACE, FLUSH_TABLE);
        int size = DatabaseDescriptor.getColumnIndexSize();
        // insert row(0:0) & row(1:1) - these are small
        QueryProcessor.executeOnceInternal(insert, k, 0, 0, createBuffer(100), createBuffer(100), 100L);
        QueryProcessor.executeOnceInternal(insert, k, 1, 1, createBuffer(100), createBuffer(100), 100L);

        // now an RT covering all of the row(2,*) range [2:_, 2:!]
        QueryProcessor.executeOnceInternal(delete, 200L, 2);

        String pointDelete = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE k=100 AND c1 = ? AND c2 = ?", KEYSPACE, FLUSH_TABLE);
        QueryProcessor.executeOnceInternal(pointDelete, 150L, 2, 0);

        // now some large rows with c1 = 2 which push that RT across multiple index blocks
        QueryProcessor.executeOnceInternal(insert, k, 2, 0, createBuffer(size+1), createBuffer(size+1), 300L);
        QueryProcessor.executeOnceInternal(insert, k, 2, 1, createBuffer(100), createBuffer(100), 300L);
        QueryProcessor.executeOnceInternal(insert, k, 2, 2, createBuffer(size+1), createBuffer(size+1), 300L);

        // another small row(3:0) which follows the large RT, so it can be closed
        QueryProcessor.executeOnceInternal(insert, k, 3, 0, createBuffer(100), createBuffer(100), 300L);
        // finally, another RT, so that the last block ends with an RT (related to the second part of the problem,
        // generating an unbounded RT bound because we don't fully exhaus OldFormatDeserializer - not necessary for
        // the main issue)
        QueryProcessor.executeOnceInternal(delete, 100L, 4);

        copySSTables(cfs, dir);
    }

}
