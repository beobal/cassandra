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

import org.junit.Test;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;

public class DropTest extends CQLTester
{
    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage("Cannot drop non existing table", "DROP TABLE " + KEYSPACE + ".table_does_not_exist");
        assertInvalidMessage("Cannot drop table in unknown keyspace", "DROP TABLE keyspace_does_not_exist.table_does_not_exist");

        execute("DROP TABLE IF EXISTS " + KEYSPACE + ".table_does_not_exist");
        execute("DROP TABLE IF EXISTS keyspace_does_not_exist.table_does_not_exist");
    }

    @Test
     public void testDroppingSystemKeyspacesIsNotAllowed() throws Throwable
     {
            assertInvalidMessage("system keyspace is not user-modifiable", "DROP KEYSPACE " + SchemaConstants.SYSTEM_KEYSPACE_NAME);
            assertInvalidMessage("Keyspace " + SchemaConstants.AUTH_KEYSPACE_NAME + " can not be dropped by a user", "DROP KEYSPACE " + SchemaConstants.AUTH_KEYSPACE_NAME);
        }
}
