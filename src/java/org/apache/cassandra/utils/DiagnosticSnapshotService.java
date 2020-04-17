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

package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class DiagnosticSnapshotService
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticSnapshotService.class);

    public static final DiagnosticSnapshotService instance =
        new DiagnosticSnapshotService(Executors.newSingleThreadExecutor(new NamedThreadFactory("DiagnosticSnapshot")));

    public static final String DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX = "DuplicateRows-";

    private final Executor executor;

    private DiagnosticSnapshotService(Executor executor)
    {
        this.executor = executor;
    }

    // Issue at most 1 snapshot request per minute for any given table.
    // Replicas will only create one snapshot per day, but this stops us
    // from swamping the network.
    // Overridable via system property for testing.
    private final long SNAPSHOT_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private final ConcurrentHashMap<UUID, AtomicLong> LAST_SNAPSHOT_TIMES = new ConcurrentHashMap<>();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;

    public static void duplicateRows(CFMetaData metadata, Iterable<InetAddress> replicas)
    {
        instance.maybeTriggerSnapshot(metadata, DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX, replicas);
    }

    public static boolean isDiagnosticSnapshotRequest(SnapshotCommand command)
    {
        return command.snapshot_name.startsWith(DUPLICATE_ROWS_DETECTED_SNAPSHOT_PREFIX);
    }

    public static void snapshot(SnapshotCommand command, InetAddress initiator)
    {
        Preconditions.checkArgument(isDiagnosticSnapshotRequest(command));
        instance.maybeSnapshot(command, initiator);
    }

    public static String getSnapshotName(String prefix)
    {
        return String.format("%s%s", prefix, DATE_FORMAT.format(LocalDate.now()));
    }

    private void maybeTriggerSnapshot(CFMetaData metadata, String prefix, Iterable<InetAddress> endpoints)
    {
        long now = System.nanoTime();
        AtomicLong cached = LAST_SNAPSHOT_TIMES.computeIfAbsent(metadata.cfId, u -> new AtomicLong(0));
        long last = cached.get();
        long interval = Long.getLong("cassandra.diagnostic_snapshot_interval_nanos", SNAPSHOT_INTERVAL_NANOS);
        if (now - last > interval && cached.compareAndSet(last, now))
        {
            MessageOut<?> msg = new SnapshotCommand(metadata.ksName,
                                                    metadata.cfName,
                                                    getSnapshotName(prefix),
                                                    false).createMessage();
            for (InetAddress replica : endpoints)
                MessagingService.instance().sendOneWay(msg, replica);
        }
        logger.debug("Diagnostic snapshot request dropped due to throttling");
    }

    private void maybeSnapshot(SnapshotCommand command, InetAddress initiator)
    {
        executor.execute(new DiagnosticSnapshotTask(command, initiator));
    }

    private static class DiagnosticSnapshotTask implements Runnable
    {
        final SnapshotCommand command;
        final InetAddress from;

        DiagnosticSnapshotTask(SnapshotCommand command, InetAddress from)
        {
            this.command = command;
            this.from = from;
        }

        public void run()
        {
            try
            {
                Keyspace ks = Keyspace.open(command.keyspace);
                if (ks == null)
                {
                    logger.info("Snapshot request received from {} for {}.{} but keyspace not found",
                                from,
                                command.keyspace,
                                command.column_family);
                    return;
                }

                ColumnFamilyStore cfs = ks.getColumnFamilyStore(command.column_family);
                if (cfs.snapshotExists(command.snapshot_name))
                {
                    logger.info("Received diagnostic snapshot request from {} for {}.{}, " +
                                "but snapshot with tag {} already exists",
                                from,
                                command.keyspace,
                                command.column_family,
                                command.snapshot_name);
                    return;
                }
                logger.info("Creating snapshot requested by {} of {}.{} tag: {}",
                            from,
                            command.keyspace,
                            command.column_family,
                            command.snapshot_name);
                cfs.snapshot(command.snapshot_name);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Snapshot request received from {} for {}.{} but CFS not found",
                            from,
                            command.keyspace,
                            command.column_family);
            }
        }
    }
}
