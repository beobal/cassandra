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
package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */

public class NamedThreadFactory implements ThreadFactory
{
    private static volatile String globalPrefix;
    public static String globalPrefix() { return globalPrefix;}
    public static void setGlobalPrefix(String prefix) { globalPrefix = prefix; }

    public static class MetaFactory
    {
        protected ClassLoader contextClassLoader;
        protected ThreadGroup threadGroup;
        protected Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

        public MetaFactory(ClassLoader contextClassLoader, ThreadGroup threadGroup, Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            this.contextClassLoader = contextClassLoader;
            if (threadGroup == null)
            {
                threadGroup = Thread.currentThread().getThreadGroup();
                while (threadGroup.getParent() != null)
                    threadGroup = threadGroup.getParent();
            }
            this.threadGroup = threadGroup;
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        }

        NamedThreadFactory newThreadFactory(String name)
        {
            return newThreadFactory(name, Thread.NORM_PRIORITY);
        }

        NamedThreadFactory newThreadFactory(String name, int threadPriority)
        {
            // We create a unique thread group for each factory, so that e.g. executors can determine which threads are members of the executor
            ThreadGroup threadGroup = new ThreadGroup(this.threadGroup, name);
            return new NamedThreadFactory(name, threadPriority, contextClassLoader, threadGroup, uncaughtExceptionHandler);
        }
    }

    public final String id;
    private final int priority;
    private final ClassLoader contextClassLoader;
    public final ThreadGroup threadGroup;
    protected final AtomicInteger n = new AtomicInteger(1);
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    public NamedThreadFactory(String id)
    {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority)
    {
        this(id, priority, null, null, JVMStabilityInspector::uncaughtException);
    }

    public NamedThreadFactory(String id, ClassLoader contextClassLoader, ThreadGroup threadGroup)
    {
        this(id, Thread.NORM_PRIORITY, contextClassLoader, threadGroup, JVMStabilityInspector::uncaughtException);
    }

    public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup)
    {
        this(id, priority, contextClassLoader, threadGroup, JVMStabilityInspector::uncaughtException);
    }
    public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup, Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
    {
        this.id = id;
        this.priority = priority;
        this.contextClassLoader = contextClassLoader;
        this.threadGroup = threadGroup;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public Thread newThread(Runnable runnable)
    {
        String name = id + ':' + n.getAndIncrement();
        String prefix = globalPrefix;
        Thread thread = newThreadRaw(threadGroup, runnable, prefix != null ? prefix + name : name);
        thread.setPriority(priority);
        if (contextClassLoader != null)
            thread.setContextClassLoader(contextClassLoader);
        if (uncaughtExceptionHandler != null)
            thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }

    protected Thread newThreadRaw(ThreadGroup threadGroup, Runnable runnable, String name)
    {
        return createThread(threadGroup, runnable, name, true);
    }

    private static final AtomicInteger threadCounter = new AtomicInteger();

    @VisibleForTesting
    public static Thread createThread(Runnable runnable)
    {
        return createThread(null, runnable, "anonymous-" + threadCounter.incrementAndGet());
    }

    public static Thread createThread(Runnable runnable, String name)
    {
        return createThread(null, runnable, name);
    }

    public static Thread createThread(Runnable runnable, String name, boolean daemon)
    {
        return createThread(null, runnable, name, daemon);
    }

    public static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name)
    {
        return createThread(threadGroup, runnable, name, false);
    }

    private static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name, boolean daemon)
    {
        String prefix = globalPrefix;
        Thread thread = new FastThreadLocalThread(threadGroup, runnable, prefix != null ? prefix + name : name);
        thread.setDaemon(daemon);
        return thread;
    }

    @Override
    public String toString()
    {
        return id;
    }
}
