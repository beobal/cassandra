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
package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.ColumnData;

/**
 * Observer for events in the lifecycle of writing out an sstable.
 */
public interface SSTableFlushObserver
{
    /**
     * Called before writing any data to the sstable.
     */
    void begin();

    /**
     * Called when a new partition in being written to the sstable,
     * but before any cells are processed (see {@link #nextCell(ColumnData)}).
     *
     * @param key The key being appended to SSTable.
     * @param indexPosition The position of the key in the SSTable PRIMARY_INDEX file.
     */
    void startPartition(DecoratedKey key, long indexPosition);

    /**
     * Called after the cell is written to the sstable.
     * Will be preceded by a call to {@code startRow(DecoratedKey, long)},
     * and the cell should be assumed to belong to that row.
     *
     * @param cell The cell being added to the row.
     */
    void nextCell(ColumnData cell);

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();
}