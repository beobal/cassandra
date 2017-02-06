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

import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.junit.Test;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;

import java.util.Optional;

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
        // Uses CQLTester::executeNet under the hood to behave like a real client,
        // specifically to have ClientState validate access to the keyspace
        assertUnauthorized("DROP KEYSPACE " + SchemaConstants.SYSTEM_KEYSPACE_NAME,
                           "system keyspace is not user-modifiable");
        assertUnauthorized("DROP KEYSPACE " + SchemaConstants.AUTH_KEYSPACE_NAME,
                           "Cannot DROP <keyspace system_auth>");
    }

    private void assertUnauthorized(String statement, String message) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT), message, UnauthorizedException.class, statement);
    }
}
