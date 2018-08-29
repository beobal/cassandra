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

package org.apache.cassandra.service.reads.repair;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.utils.NoSpamLogger;

public interface RepairedDataVerifier
{
    public void verify(RepairedDataTracker tracker);

    static RepairedDataVerifier simple(ReadCommand command)
    {
        return new SimpleVerifier(command);
    }

    static class SimpleVerifier implements RepairedDataVerifier
    {
        private static final Logger logger = LoggerFactory.getLogger(SimpleVerifier.class);
        private final ReadCommand command;

        private static final String INCONSISTENCY_WARNING = "Detected mismatch between repaired datasets for table {}.{} during read of {}. {}";

        SimpleVerifier(ReadCommand command)
        {
            this.command = command;
        }

        @Override
        public void verify(RepairedDataTracker tracker)
        {
            // some mismatch occurred between the repaired datasets on the replicas
            if (tracker.digests.keySet().size() > 1)
            {
                TableMetrics metrics = ColumnFamilyStore.metricsFor(command.metadata().id);
                // if there weren't any pending repair sessions which had not yet been committed
                // mark the inconsistency as confirmed, otherwise it may be due to the sessions
                // being committed at different times on different replicas so mark it unconfirmed
                if (tracker.inconclusiveDigests.isEmpty())
                    metrics.confirmedRepairedInconsistencies.mark();
                else
                    metrics.unconfirmedRepairedInconsistencies.mark();

                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                 INCONSISTENCY_WARNING, command.metadata().keyspace,
                                 command.metadata().name, getCommandString(), tracker);
            }
        }

        private String getCommandString()
        {
            return command instanceof SinglePartitionReadCommand
                   ? ((SinglePartitionReadCommand)command).partitionKey().toString()
                   : ((PartitionRangeReadCommand)command).dataRange().keyRange().getString(command.metadata().partitionKeyType);

        }
    }
}
