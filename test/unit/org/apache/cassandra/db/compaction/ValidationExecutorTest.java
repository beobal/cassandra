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

package org.apache.cassandra.db.compaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationExecutorTest
{

    CompactionManager.ValidationExecutor validationExecutor;

    @Before
    public void setup()
    {
        DatabaseDescriptor.clientInitialization();
        // required for static initialization of CompactionManager
        DatabaseDescriptor.setConcurrentCompactors(2);
        DatabaseDescriptor.setConcurrentValidations(2);

        // shutdown the singleton CompactionManager to ensure MBeans are unregistered
        CompactionManager.instance.forceShutdown();
    }

    @After
    public void tearDown()
    {
        if (null != validationExecutor)
            validationExecutor.shutdownNow();
    }

    @Test
    public void testQueueOnValidationSubmission() throws InterruptedException
    {
        Condition taskBlocked = new SimpleCondition();
        AtomicInteger threadsAvailable = new AtomicInteger(DatabaseDescriptor.getConcurrentValidations());
        CountDownLatch taskComplete = new CountDownLatch(5);
        validationExecutor = new CompactionManager.ValidationExecutor(Config.ValidationPoolFullStrategy.queue);

        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        for (int i=0; i< 5; i++)
            testExecutor.submit(() -> {
                threadsAvailable.decrementAndGet();
                validationExecutor.submit(new Task(taskBlocked, taskComplete));
            });

        // wait for all tasks to be submitted & check that the excess ones were queued
        while (threadsAvailable.get() > 0)
            TimeUnit.MILLISECONDS.sleep(10);

        assertEquals(2, validationExecutor.getActiveTaskCount());
        assertEquals(3, validationExecutor.getPendingTaskCount());

        taskBlocked.signalAll();
        taskComplete.await(10, TimeUnit.SECONDS);
        validationExecutor.shutdownNow();
    }

    @Test
    public void testBlockOnValidationSubmission() throws InterruptedException
    {
        Condition taskBlocked = new SimpleCondition();
        CountDownLatch tasksToComplete = new CountDownLatch(3);
        validationExecutor = new CompactionManager.ValidationExecutor(Config.ValidationPoolFullStrategy.block);

        Condition poolFull = new SimpleCondition();

        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        Future<?> f1 = testExecutor.submit(() -> validationExecutor.submit(new Task(taskBlocked, tasksToComplete)));
        Future<?> f2 = testExecutor.submit(() -> validationExecutor.submit(new Task(taskBlocked, tasksToComplete)));
        Future<?> f3 = testExecutor.submit(() -> {
            // pool should be filled by the prior submissions, so signal we're ready to check that
            poolFull.signalAll();
            validationExecutor.submit(new Task(taskBlocked, tasksToComplete));
        });

        // wait for the submissions to fill the pool so that we start blocking
        poolFull.await();

        // submission of the third task should be blocked until the previous tasks complete
        assertTrue(f1.isDone());
        assertTrue(f2.isDone());
        assertFalse(f3.isDone());
        // neither of the two tasks are complete
        assertEquals(3, tasksToComplete.getCount());
        // unblock the tasks and wait for all 3 tasks to complete
        taskBlocked.signalAll();
        tasksToComplete.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void testAdjustPoolSize()
    {
        // if the pool full strategy is queue, adjusting the pool size should dynamically
        // set core and max pool size to DatabaseDescriptor::getConcurrentValidations

        validationExecutor = new CompactionManager.ValidationExecutor(Config.ValidationPoolFullStrategy.queue);

        int corePoolSize = validationExecutor.getCorePoolSize();
        int maxPoolSize = validationExecutor.getMaximumPoolSize();

        DatabaseDescriptor.setConcurrentValidations(corePoolSize * 2);
        validationExecutor.adjustPoolSize();
        assertThat(validationExecutor.getCorePoolSize()).isEqualTo(corePoolSize * 2);
        assertThat(validationExecutor.getMaximumPoolSize()).isEqualTo(maxPoolSize * 2);
        validationExecutor.shutdownNow();

        // if the strategy is to block, then adjustPool size has no effect as the
        // use of SynchronousQueue will allow core threads to be created when necessary
        validationExecutor = new CompactionManager.ValidationExecutor(Config.ValidationPoolFullStrategy.block);
        assertThat(validationExecutor.getCorePoolSize()).isEqualTo(1);
        validationExecutor.adjustPoolSize();
        assertThat(validationExecutor.getCorePoolSize()).isEqualTo(1);
    }

    private static class Task implements Runnable
    {
        private final Condition blocked;
        private final CountDownLatch complete;

        Task(Condition blocked, CountDownLatch complete)
        {
            this.blocked = blocked;
            this.complete = complete;
        }

        public void run()
        {
            Uninterruptibles.awaitUninterruptibly(blocked, 10, TimeUnit.SECONDS);
            complete.countDown();
        }
    }
}
