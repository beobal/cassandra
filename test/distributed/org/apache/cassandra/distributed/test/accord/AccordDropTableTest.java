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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.service.accord.AccordTestUtils.wrapInTxn;

public class AccordDropTableTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withoutVNodes()
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start())
        {
            fixDistributedSchemas(cluster);
            init(cluster);

            int examples = 10;
            int steps = 100;
            for (int i = 0; i < examples; i++)
            {
                try
                {
                    addChaos(cluster, i);
                    createTable(cluster);
                    for (int j = 0; j < steps; j++)
                        doTxn(cluster, j);
                    dropTable(cluster);
                }
                catch (Throwable t)
                {
                    throw new AssertionError("Error at example " + i, t);
                }
            }
        }
    }

    private static void addChaos(Cluster cluster, int example)
    {
        cluster.filters().reset();
        cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).from(1).to(3).drop();
//        cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).from(2).to(1).drop();
//        cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).from(3).to(2).drop();
    }

    private static void doTxn(Cluster cluster, int step)
    {
        int stepId = step % 3;
        int partitionId = step % 10;
        ICoordinator coordinator = cluster.coordinator(stepId + 1);
        switch (stepId)
        {
            case 0: // insert
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("INSERT INTO %s.tbl(pk, v) VALUES (?, ?);")), ConsistencyLevel.ANY, partitionId, step));
                break;
            case 1: // insert + read
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("UPDATE %s.tbl SET v+=1 WHERE pk=?;")), ConsistencyLevel.ANY, partitionId));
                break;
            case 2: // read
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("SELECT * FROM %s.tbl WHERE pk=?")), ConsistencyLevel.ANY, partitionId));
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static void retry(int maxAttempts, Runnable fn)
    {
        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                fn.run();
            }
            catch (Throwable t)
            {
                if (i == (maxAttempts - 1))
                    throw t;
            }
        }
    }

    private static void createTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int PRIMARY KEY, v int) WITH transactional_mode='full'"));
    }

    private static void dropTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl"));
    }
}
