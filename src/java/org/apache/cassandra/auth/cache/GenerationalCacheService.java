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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;

public class GenerationalCacheService implements GenerationalCacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(GenerationalCacheService.class);

    private static final String MBEAN_NAME = "org.apache.cassandra.db:type=GenerationalCacheService";
    public static GenerationalCacheService instance = new GenerationalCacheService();

    private final ThreadPoolExecutor executor;
    private final ConcurrentHashMap<String, GenerationalCacheController> controllers = new ConcurrentHashMap<>();

    public GenerationalCacheService()
    {
        // start off single threaded and increase max threads as we register new caches.
        // use a SynchronousQueue as we don't want to actually enqueue refresh tasks,
        // just hand them off to worker threads.
        executor = new JMXEnabledThreadPoolExecutor(1,
                                                    1,
                                                    1,
                                                    TimeUnit.MINUTES,
                                                    new SynchronousQueue<>(),
                                                    new NamedThreadFactory("GenCacheRefresh", Thread.MIN_PRIORITY),
                                                    "internal");
    }

    public void registerMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public GenerationalCacheController registerCache(String identifier, int refreshInterval, Runnable refreshCallback)
    {
        logger.info("Registering cache {} with service", identifier);
        // this is slightly wasteful if multiple registrations with the same
        // identifier are attempted in error, but constructing a new cache
        // controller is fairly cheap. Plus, registration is not any hot path
        // and in fact, should only happen during system initialization.
        GenerationalCacheController newCache = new GenerationalCacheController(identifier,
                                                                               refreshInterval,
                                                                               refreshCallback,
                                                                               executor);
        GenerationalCacheController existing = controllers.putIfAbsent(identifier, newCache);

        if (existing != null)
            throw new IllegalArgumentException(String.format("Duplicate registration attempt for cache " +
                                                             "with identifier %s", identifier));

        // bump the max pool size to equal the number of registered caches to handle worst-case concurrency
        executor.setCorePoolSize(controllers.size());

        logger.info("Registration of cache {} complete", identifier);
        return newCache;
    }

    public List<String> listCaches()
    {
        List<String> caches = new ArrayList<>();
        caches.addAll(controllers.keySet());
        return caches;
    }

    public long getLocalGeneration(String identifier)
    {
        if (!controllers.containsKey(identifier))
            throw new IllegalArgumentException(String.format("%s is not a registered cache", identifier));

        return controllers.get(identifier).getLocalGeneration();
    }

    public void forceLocalGeneration(String identifier, long generation)
    {
        if (!controllers.containsKey(identifier))
            throw new IllegalArgumentException(String.format("%s is not a registered cache", identifier));

        controllers.get(identifier).forceLocalGeneration(generation);
    }

    public void forceRefresh(String identifier)
    {
        if (!controllers.containsKey(identifier))
            throw new IllegalArgumentException(String.format("%s is not a registered cache", identifier));

        logger.debug("Forcing refresh of {}", identifier);
        controllers.get(identifier).forceRefresh();
    }

    public void pauseRefresh(String identifier)
    {
        if (!controllers.containsKey(identifier))
            throw new IllegalArgumentException(String.format("%s is not a registered cache", identifier));

        logger.debug("Pausing cache refreshing for {}", identifier);
        controllers.get(identifier).pauseRefresh();
    }

    public void resumeRefresh(String identifier)
    {
        if (!controllers.containsKey(identifier))
            throw new IllegalArgumentException(String.format("%s is not a registered cache", identifier));

        logger.debug("Resuming cache refreshing for {}", identifier);
        controllers.get(identifier).resumeRefresh();
    }

    public void forceRefreshAll()
    {
        controllers.values().forEach(GenerationalCacheController::forceRefresh);
    }

    public void pauseRefreshAll()
    {
        controllers.values().forEach(GenerationalCacheController::pauseRefresh);
    }

    public void resumeRefreshAll()
    {
        controllers.values().forEach(GenerationalCacheController::resumeRefresh);
    }
}
