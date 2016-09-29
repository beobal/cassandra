package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * Metrics about authentication & authorization
 */
public class AuthMetrics
{
    public static final AuthMetrics instance = new AuthMetrics();

    public static void init()
    {
        // no-op, just used to force instance creation
    }

    /** Number and rate of successful logins */
    private final Meter authSuccess;

    /** Number and rate of login failures */
    private final Meter authFailure;

    /** Number and rate of operations being rejected due to a capability restriction **/
    private final Meter capRestrictionEnforced;

    /** Latency of capability restriction lookups **/
    private final Timer capRestrictionCheckLatency;
    private final Counter capRestrictionCheckTotalLatencyMicros;

    private AuthMetrics()
    {
        authSuccess = ClientMetrics.instance.addMeter("AuthSuccess");
        authFailure = ClientMetrics.instance.addMeter("AuthFailure");
        capRestrictionEnforced = ClientMetrics.instance.addMeter("CapRestrictionEnforced");
        capRestrictionCheckLatency = ClientMetrics.instance.addTimer("CapRestrictionCheckLatency");
        capRestrictionCheckTotalLatencyMicros = ClientMetrics.instance.addCounter("CapRestrictionTotalCheckLatencyMicros");
    }

    public void markSuccess()
    {
        authSuccess.mark();
    }

    public void markFailure()
    {
        authFailure.mark();
    }

    public void markRestricted()
    {
        capRestrictionEnforced.mark();
    }

    public void updateRestrictionCheckLatencyNanos(long latency)
    {
        capRestrictionCheckLatency.update(latency, TimeUnit.NANOSECONDS);
        capRestrictionCheckTotalLatencyMicros.inc(TimeUnit.MICROSECONDS.convert(latency, TimeUnit.NANOSECONDS));
    }
}
