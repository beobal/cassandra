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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for the generational caches managed by {@link org.apache.cassandra.auth.cache.GenerationalCacheService}
 */
public class GenCacheMetrics
{

    public final Meter localGenUpdates;

    public final Meter localGenUpdateErrors;

    public final Meter refreshTaskSubmissions;

    public final Meter refreshTasksDiscarded;

    public final Meter refreshTaskErrors;

    public final Meter genCheckErrors;

    public final Timer genCheckLatency;

    public final Timer refreshLatency;

    public final Timer genUpdateLatency;


    public GenCacheMetrics(String identifier)
    {
        MetricNameFactory factory = new DefaultNameFactory("GenCache", identifier);

        localGenUpdates = Metrics.meter(factory.createMetricName("localGenerationUpdates"));
        localGenUpdateErrors = Metrics.meter(factory.createMetricName("localGenerationUpdateErrors"));
        refreshTaskSubmissions = Metrics.meter(factory.createMetricName("refreshTaskSubmissions"));
        refreshTasksDiscarded = Metrics.meter(factory.createMetricName("refreshTasksDiscarded"));
        refreshTaskErrors = Metrics.meter(factory.createMetricName("refreshTaskErrors"));
        genCheckErrors = Metrics.meter(factory.createMetricName("generationCheckErrors"));
        genCheckLatency = Metrics.timer(factory.createMetricName("generationCheckLatency"));
        refreshLatency = Metrics.timer(factory.createMetricName("refreshLatency"));
        genUpdateLatency = Metrics.timer(factory.createMetricName("generationUpdateLatency"));
    }

}
