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

package org.apache.cassandra.auth.cache;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.metrics.GenCacheMetrics;

public class GenerationalCacheController
{
    private static final Logger logger = LoggerFactory.getLogger(GenerationalCacheController.class);

    // interval between retry attempts if a generation increment should fail
    private static final Long GEN_INCR_RETRY_INTERVAL = Long.getLong("cassandra.gen_cache_incr_retry_interval_ms", 1000);

    private final String identifier;
    private final AtomicLong currentGeneration = new AtomicLong(-1);
    private final AtomicBoolean refreshInFlight = new AtomicBoolean(false);
    private final AtomicBoolean isPaused = new AtomicBoolean(true);
    private final ExecutorService refreshExecutor;
    private final Runnable refreshCallback;
    private final int refreshInterval;

    private final GenCacheMetrics metrics;

    GenerationalCacheController(String identifier,
                                int refreshInterval,
                                Runnable refreshCallback,
                                ExecutorService refreshExecutor)
    {
        this.identifier = identifier;
        this.refreshInterval = refreshInterval;
        this.refreshExecutor = refreshExecutor;
        this.refreshCallback = refreshCallback;
        metrics = new GenCacheMetrics(identifier);
    }

    public void init()
    {
        long current = readGeneration();
        while (current < 0)
        {
            logger.debug("No distributed cache metadata for {}, attempting to initialize", identifier);
            String query = String.format("INSERT INTO %s.%s (id, generation) VALUES ('%s', 0) IF NOT EXISTS",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.CACHE_METADATA,
                                         identifier);
            UntypedResultSet results = process(query, ConsistencyLevel.LOCAL_QUORUM);
            if (!results.isEmpty() && results.one().has("[applied]"))
            {
                UntypedResultSet.Row row = results.one();
                boolean success = row.getBoolean("[applied]");
                if (success)
                {
                    current = 0;
                    logger.debug("Distributed cache metadata successfully initialized");
                }
                else if (row.has("generation"))
                {
                    current = row.getLong("generation");
                    logger.debug("Distributed cache metadata already initialized");
                }
                // shouldn't be able to get to this point, but if we do we'll just retry
            }
        }
        currentGeneration.set(current);

        // schedule a task to check whether a reload is required & execute the callback if so
        long initialDelay = (new Random().nextInt(10) + 1) * 1000;
        RefreshTrigger trigger = new RefreshTrigger();
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(trigger,
                                                                 initialDelay,
                                                                 refreshInterval,
                                                                 TimeUnit.MILLISECONDS);

        isPaused.set(false);
        logger.debug("Initialized cache generation watcher with generation {}", current);
    }

    public long getLocalGeneration()
    {
        return currentGeneration.get();
    }

    public void forceLocalGeneration(long newGeneration)
    {
        currentGeneration.set(newGeneration);
    }

    void forceRefresh()
    {
        refreshCallback.run();
    }

    void pauseRefresh()
    {
        isPaused.set(true);
    }

    void resumeRefresh()
    {
        isPaused.set(false);
    }

    public void notifyUpdate()
    {
        incrementGeneration();
    }

    private long readGeneration()
    {
        try (Timer.Context ctx = metrics.genCheckLatency.time())
        {
            String query = String.format("SELECT generation FROM %s.%s WHERE id = '%s'",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.CACHE_METADATA,
                                         identifier);

            UntypedResultSet results = process(query, ConsistencyLevel.LOCAL_SERIAL);
            if (!results.isEmpty() && results.one().has("generation"))
                return results.one().getLong("generation");

            return currentGeneration.get();
        }
    }

    private void incrementGeneration()
    {
        metrics.localGenUpdates.mark();
        long current = currentGeneration.get();
        long incremented = current + 1;
        try (Timer.Context ctx = metrics.genUpdateLatency.time())
        {
            String query = String.format("UPDATE %s.%s SET generation = %s WHERE id = '%s' IF generation = %s",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.CACHE_METADATA,
                                         incremented,
                                         identifier,
                                         current);
            UntypedResultSet results = process(query, ConsistencyLevel.LOCAL_ONE);
            if (!results.isEmpty() && results.one().has("[applied]"))
            {
                UntypedResultSet.Row row = results.one();
                boolean success = row.getBoolean("[applied]");
                if (!success)
                {
                    // If somebody else already updated the generation, this isn't a
                    // problem as we would detect the change next time we poll and so
                    // schedule an update. We can optimise by pre-emptively doing so now.
                    maybeSubmitRefreshTask(row.getLong("generation"));
                }
            }
        }
        catch (Exception e)
        {
            // if an unexpected error is encountered when bumping the generation,
            // the underlying data will have changed but if no further updates occur
            // (& successfully increment the generation), peers will receive no
            // notification to refresh their own local snapshots. So, submit a task
            // to re-try the generation update. This will keep retrying until a
            // success, which isn't harmful, at worst it's slightly wasteful
            metrics.localGenUpdateErrors.mark();
            logger.warn("Updating distributed cache metadata for {} following local update failed. " +
                        "Scheduling retrying in {}ms",
                        identifier, GEN_INCR_RETRY_INTERVAL);
            ScheduledExecutors.optionalTasks.schedule(this::incrementGeneration,
                                                      GEN_INCR_RETRY_INTERVAL,
                                                      TimeUnit.MILLISECONDS);
        }
    }

    private void maybeSubmitRefreshTask(long newGeneration)
    {
        if (refreshInFlight.compareAndSet(false, true))
        {
            refreshExecutor.submit(new RefreshTask(newGeneration));
            metrics.refreshTaskSubmissions.mark();
        }
        else
        {
            logger.debug("Cache {} already has a refresh task in flight, not submitting another", identifier);
            metrics.refreshTasksDiscarded.mark();
        }
    }

    // protected so it can be overridden in subclasses with a version that
    // executes purely locally, which is useful for testing
    protected UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    private final class RefreshTrigger implements Runnable
    {
        public void run()
        {
            if (isPaused.get())
                return;

            logger.trace("Checking cache generation for {}", identifier);
            try
            {
                long newGeneration = readGeneration();
                long current = currentGeneration.get();
                if (newGeneration != current)
                {
                    logger.debug("Distributed cache generation updated for {} ({} -> {})", identifier, current, newGeneration);
                    maybeSubmitRefreshTask(newGeneration);
                }
            }
            catch (Exception e)
            {
                // swallow any exceptions so as not to suppress future executions
                logger.debug("Encountered error checking cache metadata for {}", identifier, e);
                metrics.genCheckErrors.mark();
            }
        }
    }

    private final class RefreshTask implements Runnable
    {
        private final long newGeneration;

        RefreshTask(long newGeneration)
        {
            this.newGeneration = newGeneration;
        }

        public void run()
        {
            try (Timer.Context ctx = metrics.refreshLatency.time())
            {
                logger.debug("Performing background cache refresh for {}", identifier);
                try
                {
                    refreshCallback.run();
                    currentGeneration.set(newGeneration);
                }
                catch (Exception e)
                {
                    logger.info("Encountered error during background cache refresh for {}", identifier, e);
                    metrics.refreshTaskErrors.mark();
                }
                finally
                {
                    refreshInFlight.set(false);
                    logger.debug("Done performing background cache refresh for {}", identifier);
                }
            }
        }
    }
}
