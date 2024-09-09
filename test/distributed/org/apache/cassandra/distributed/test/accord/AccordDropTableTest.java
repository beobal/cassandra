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
import java.util.UUID;

import org.junit.Test;

import accord.api.Key;
import accord.local.CommandStores;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;

import static org.apache.cassandra.service.accord.AccordTestUtils.wrapInTxn;

public class AccordDropTableTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withoutVNodes()
                                      .withConfig(c -> c.with(Feature.values())
                                                       .set("accord.progress_log_schedule_delay", "10s"))
                                      .start())
        {
            fixDistributedSchemas(cluster);
            init(cluster);

            int examples = 10;
            int steps = 10;
            for (int i = 0; i < examples; i++)
            {
                int j = 0;
                try
                {
                    addChaos(cluster, i);
                    TableId id = createTable(cluster);
                    for (j = 0; j < steps; j++)
                        doTxn(cluster, j);
                    dropTable(cluster);
                    validateAccord(cluster, id);
                }
                catch (Throwable t)
                {
                    throw new AssertionError("Error at example " + i + ", " + j, t);
                }
            }
        }
    }

    private static void addChaos(Cluster cluster, int example)
    {
        cluster.filters().reset();
        cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).from(1).to(3).drop();
    }

    private static void doTxn(Cluster cluster, int step)
    {
        int stepId = step % 3;
        int partitionId = step % 10;
        int coordinatorId = (step % 2) + 1; // avoid node3 as it can't get applies from node1, so leads to user errors
        ICoordinator coordinator = cluster.coordinator(coordinatorId);
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

    private static TableId createTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int PRIMARY KEY, v int) WITH transactional_mode='full'"));
        return ClusterUtils.tableId(cluster, KEYSPACE, "tbl");
    }

    private static void dropTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl"));
    }

    private static void validateAccord(Cluster cluster, TableId id)
    {
        String s = id.toString();
        for (IInvokableInstance inst : cluster)
        {
            inst.runOnInstance(() -> {
                TableId tableId = TableId.fromUUID(UUID.fromString(s));
                AccordService accord = (AccordService) AccordService.instance();
                PreLoadContext ctx = PreLoadContext.contextFor(Ranges.single(TokenRange.fullRange(tableId)), KeyHistory.COMMANDS);
                CommandStores stores = accord.node().commandStores();
                for (int storeId : stores.ids())
                {
                    AccordCommandStore store = (AccordCommandStore) stores.forId(storeId);
                    AsyncResult<?> result = store.submit(ctx, input -> {
                        AccordSafeCommandStore safe = (AccordSafeCommandStore) input;
                        for (Key key : safe.commandsForKeysKeys())
                        {
                            CommandsForKey cfk = safe.maybeCommandsForKey(key).current();
                            CommandsForKey.TxnInfo minUndecided = cfk.minUndecided();
                            if (minUndecided != null)
                                throw new AssertionError("Undecided txn: " + minUndecided);
                            TxnId next = cfk.nextWaitingToApply();
                            if (next != null)
                                throw new AssertionError("Unapplied txn: " + next);
                        }
                        return null;
                    }).beginAsResult();
                    AsyncChains.getUnchecked(result);
                }
            });
        }
    }
}
