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
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

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
            init(cluster);
            fixDistributedSchemas(cluster);

            for (int i = 0; i < 100; i++)
            {
                createTable(cluster);
//                for (int j = 0; j < 100; j++)
//                    doTxn(cluster, j);
                dropTable(cluster);
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
