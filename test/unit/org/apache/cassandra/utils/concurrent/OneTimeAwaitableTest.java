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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

public abstract class OneTimeAwaitableTest<A extends Awaitable> extends AbstractTestAwaitable<A>
{
    protected void testOneSuccess(A awaitable, Consumer<A> signal)
    {
        Async async = new Async();
        //noinspection Convert2MethodRef
        async.success(awaitable, a -> a.await(), awaitable);
        async.success(awaitable, a -> a.awaitUninterruptibly(), awaitable);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(), awaitable);
        async.success(awaitable, a -> a.await(1L, TimeUnit.SECONDS), true);
        async.success(awaitable, a -> a.awaitUninterruptibly(1L, TimeUnit.SECONDS), true);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(1L, TimeUnit.SECONDS), true);
        async.success(awaitable, a -> a.awaitUntil(Long.MAX_VALUE), true);
        async.success(awaitable, a -> a.awaitUntilUninterruptibly(Long.MAX_VALUE), true);
        async.success(awaitable, a -> a.awaitUntilThrowUncheckedOnInterrupt(Long.MAX_VALUE), true);
        signal.accept(awaitable);
        async.verify();
    }

    public void testOneTimeout(A awaitable)
    {
        Async async = new Async();
        async.success(awaitable, a -> a.await(1L, TimeUnit.MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitUninterruptibly(1L, TimeUnit.MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(1L, TimeUnit.MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1L)), false);
        async.success(awaitable, a -> a.awaitUntilUninterruptibly(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1L)), false);
        async.success(awaitable, a -> a.awaitUntilThrowUncheckedOnInterrupt(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1L)), false);
        Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
        async.verify();
    }

    public void testOneInterrupt(A awaitable)
    {
        Async async = new Async();
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.await(); }, InterruptedException.class);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.await(1L, TimeUnit.SECONDS); }, InterruptedException.class);
        async.success(awaitable, a -> { Thread.currentThread().interrupt(); return a.awaitUninterruptibly(1L, TimeUnit.SECONDS); }, false);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitThrowUncheckedOnInterrupt(1L, TimeUnit.SECONDS); }, UncheckedInterruptedException.class);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitUntil(System.nanoTime() + TimeUnit.SECONDS.toNanos(1L)); }, InterruptedException.class);
        async.success(awaitable, a -> { Thread.currentThread().interrupt(); return a.awaitUntilUninterruptibly(System.nanoTime() + TimeUnit.SECONDS.toNanos(1L)); }, false);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitUntilThrowUncheckedOnInterrupt(System.nanoTime() + TimeUnit.SECONDS.toNanos(1L)); }, UncheckedInterruptedException.class);
        Uninterruptibles.sleepUninterruptibly(2L, TimeUnit.SECONDS);
        async.verify();
    }

}
