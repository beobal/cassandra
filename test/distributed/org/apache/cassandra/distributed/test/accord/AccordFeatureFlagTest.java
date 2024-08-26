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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.AssertionUtils;
import org.assertj.core.api.Assertions;

public class AccordFeatureFlagTest extends TestBaseImpl
{
    @Test
    public void shouldHideAccordTransactions()  throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK).set("accord.enabled", "false"))
                                           .start()))
        {
            Assertions.assertThatThrownBy(() -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'"))
                    .has(AssertionUtils.isThrowableInstanceof(InvalidRequestException.class))
                    .hasMessageContaining("accord.enabled");
        }
    }
}