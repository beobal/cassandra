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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface RepairedDataInfo
{
    default ByteBuffer getRepairedDataDigest()
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    default boolean isConclusive()
    {
        return true;
    }

    default void markInconclusive()
    {
    }

    default void trackPartitionKey(DecoratedKey key)
    {
    }

    default void trackDeletion(DeletionTime deletion)
    {
    }

    default void trackRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
    }

    default void trackRow(Row row)
    {
    }
}
