package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics about authentication
 */
public class AuthMetrics
{
    /** Counts the number of successes */
    protected final Counter success;

    /** Counts the number of failures */
    protected final Counter failure;

    protected final MetricNameFactory factory;

    /**
     * Create AuthMetrics with given group, type and scope.
     *
     * @param scope Scope of metrics
     */
    public AuthMetrics(String scope)
    {
        this(new DefaultNameFactory("Auth", scope));
    }

    /**
     * Create AuthMetrics with given group, type and scope.
     *
     * @param factory MetricName factory to use
     */
    private AuthMetrics(MetricNameFactory factory)
    {
        this.factory = factory;

        success = Metrics.counter(factory.createMetricName("success"));
        failure = Metrics.counter(factory.createMetricName("failure"));
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("success"));
        Metrics.remove(factory.createMetricName("failure"));
    }

    public void markSuccess()
    {
        success.inc();
    }

    public void markFailure()
    {
        failure.inc();
    }
}