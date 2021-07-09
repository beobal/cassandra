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

package org.apache.cassandra.distributed.shared;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Futures;
import org.junit.Assert;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utilities for working with jvm-dtest clusters.
 *
 * This class should never be called from within the cluster, always in the App ClassLoader.
 */
public class ClusterUtils
{
    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, Consumer<WithProperties> fn)
    {
        return start(inst, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, BiConsumer<I, WithProperties> fn)
    {
        try (WithProperties properties = new WithProperties())
        {
            fn.accept(inst, properties);
            inst.startup();
            return inst;
        }
    }

    /**
     * Stop an instance in a blocking manner.
     *
     * The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     */
    public static void stopUnchecked(IInstance i)
    {
        Futures.getUnchecked(i.shutdown());
    }

    /**
     * Stops an instance abruptly.  This is done by blocking all messages to/from so all other instances are unable
     * to communicate, then stopping the instance gracefully.
     *
     * The assumption is that hard stopping inbound and outbound messages will apear to the cluster as if the instance
     * was stopped via kill -9; this does not hold true if the instance is restarted as it knows it was properly shutdown.
     *
     * @param cluster to filter messages to
     * @param inst to shut down
     */
    public static <I extends IInstance> void stopAbrupt(ICluster<I> cluster, I inst)
    {
        // block all messages to/from the node going down to make sure a clean shutdown doesn't happen
        IMessageFilters.Filter to = cluster.filters().allVerbs().to(inst.config().num()).drop();
        IMessageFilters.Filter from = cluster.filters().allVerbs().from(inst.config().num()).drop();
        try
        {
            stopUnchecked(inst);
        }
        finally
        {
            from.off();
            to.off();
        }
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param other config to copy from
     * @param fn function to add to the config before starting
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      IInstanceConfig other,
                                                      Consumer<IInstanceConfig> fn)
    {
        return addInstance(cluster, other.localDatacenter(), other.localRack(), fn);
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param dc the instance should be in
     * @param rack the instance should be in
     * @param fn function to add to the config before starting
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc, String rack,
                                                      Consumer<IInstanceConfig> fn)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");

        InstanceConfig config = cluster.newInstanceConfig();
        //TODO adding new instances should be cleaner, currently requires you create the cluster with all
        // instances known about (at least to NetworkTopology and TokenStategy)
        // this is very hidden, so should be more explicit
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));

        fn.accept(config);

        return cluster.bootstrap(config);
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param fn lambda to add additional properties
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster,
                                                              IInstance toReplace,
                                                              Consumer<WithProperties> fn)
    {
        return replaceHostAndStart(cluster, toReplace, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param fn lambda to add additional properties or modify instance
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster,
                                                              IInstance toReplace,
                                                              BiConsumer<I, WithProperties> fn)
    {
        IInstanceConfig toReplaceConf = toReplace.config();
        I inst = addInstance(cluster, toReplaceConf, c -> c.set("auto_bootstrap", true));

        return start(inst, properties -> {
            // lower this so the replacement waits less time
            properties.setProperty("cassandra.broadcast_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(30)));
            // default is 30s, lowering as it should be faster
            properties.setProperty("cassandra.ring_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10)));
            properties.setProperty("cassandra.schema_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10)));

            // state which node to replace
            properties.setProperty("cassandra.replace_address_first_boot", toReplace.config().broadcastAddress().getAddress().getHostAddress());

            fn.accept(inst, properties);
        });
    }

    public static void runAndWaitForLogs(Cluster cluster, Runnable r, String waitString) throws TimeoutException
    {
        long [] marks = new long[cluster.size()];
        for (int i = 0; i < cluster.size(); i++)
            marks[i] = cluster.get(i + 1).logs().mark();
        r.run();
        for (int i = 0; i < cluster.size(); i++)
            cluster.get(i + 1).logs().watchFor(marks[i], waitString);
    }

    /**
     * Get the ring from the perspective of the instance.
     */
    public static List<RingInstanceDetails> ring(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }

    /**
     * Make sure the target instance's gossip state matches on the source instance
     *
     * @param instance instance to check on
     * @param expectedInRing instance expected in the ring
     * @param state expected gossip state
     * @return the ring (if target is present and has expected state)
     */
    public static List<RingInstanceDetails> assertRingState(IInstance instance, IInstance expectedInRing, String state)
    {
        String targetAddress = getBroadcastAddressHostString(expectedInRing);
        List<RingInstanceDetails> ring = ring(instance);
        List<RingInstanceDetails> match = ring.stream()
                                              .filter(d -> d.address.equals(targetAddress))
                                              .collect(Collectors.toList());
        assertThat(match)
        .isNotEmpty()
        .as("State was expected to be %s but was not", state)
        .anyMatch(r -> r.state.equals(state));
        return ring;
    }

    private static List<RingInstanceDetails> awaitRing(IInstance src, String errorMessage, Predicate<List<RingInstanceDetails>> fn)
    {
        List<RingInstanceDetails> ring = null;
        for (int i = 0; i < 100; i++)
        {
            ring = ring(src);
            if (fn.test(ring))
            {
                return ring;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new AssertionError(errorMessage + "\n" + ring);
    }

    /**
     * Wait for the target to be in the ring as seen by the source instance.
     *
     * @param instance instance to check on
     * @param expectedInRing instance to wait for
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingJoin(IInstance instance, IInstance expectedInRing)
    {
        return awaitRingJoin(instance, expectedInRing.broadcastAddress().getAddress().getHostAddress());
    }

    /**
     * Wait for the target to be in the ring as seen by the source instance.
     *
     * @param instance instance to check on
     * @param expectedInRing instance address to wait for
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingJoin(IInstance instance, String expectedInRing)
    {
        return awaitRing(instance, "Node " + expectedInRing + " did not join the ring...", ring -> {
            Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(expectedInRing)).findFirst();
            if (match.isPresent())
            {
                RingInstanceDetails details = match.get();
                return details.status.equals("Up") && details.state.equals("Normal");
            }
            return false;
        });
    }

    /**
     * Wait for the ring to only have instances that are Up and Normal.
     *
     * @param src instance to check on
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingHealthy(IInstance src)
    {
        return awaitRing(src, "Timeout waiting for ring to become healthy",
                         ring ->
                         ring.stream().allMatch(ClusterUtils::isRingInstanceDetailsHealthy));
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedInRing expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, IInstance... expectedInRing)
    {
        return assertRingIs(instance, Arrays.asList(expectedInRing));
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedInRing expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, Collection<? extends IInstance> expectedInRing)
    {
        Set<String> expectedRingAddresses = expectedInRing.stream()
                                                          .map(i -> i.config().broadcastAddress().getAddress().getHostAddress())
                                                          .collect(Collectors.toSet());
        return assertRingIs(instance, expectedRingAddresses);
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedRingAddresses expected instances addresses in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, Set<String> expectedRingAddresses)
    {
        List<RingInstanceDetails> ring = ring(instance);
        Set<String> ringAddresses = ring.stream().map(d -> d.address).collect(Collectors.toSet());
        assertThat(ringAddresses)
        .as("Ring addreses did not match for instance %s", instance)
        .isEqualTo(expectedRingAddresses);
        return ring;
    }

    private static boolean isRingInstanceDetailsHealthy(RingInstanceDetails details)
    {
        return details.status.equals("Up") && details.state.equals("Normal");
    }

    private static List<RingInstanceDetails> parseRing(String str)
    {
        // 127.0.0.3  rack0       Up     Normal  46.21 KB        100.00%             -1
        // /127.0.0.1:7012  Unknown     ?      Normal  ?               100.00%             -3074457345618258603
        Pattern pattern = Pattern.compile("^(/?[0-9.:]+)\\s+(\\w+|\\?)\\s+(\\w+|\\?)\\s+(\\w+|\\?).*?(-?\\d+)\\s*$");
        List<RingInstanceDetails> details = new ArrayList<>();
        String[] lines = str.split("\n");
        for (String line : lines)
        {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find())
            {
                continue;
            }
            details.add(new RingInstanceDetails(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5)));
        }

        return details;
    }

    /**
     * Get the broadcast address host address only (ex. 127.0.0.1)
     */
    public static String getBroadcastAddressHostString(IInstance target)
    {
        return target.config().broadcastAddress().getAddress().getHostAddress();
    }

    public static final class RingInstanceDetails
    {
        private final String address;
        private final String rack;
        private final String status;
        private final String state;
        private final String token;

        private RingInstanceDetails(String address, String rack, String status, String state, String token)
        {
            this.address = address;
            this.rack = rack;
            this.status = status;
            this.state = state;
            this.token = token;
        }

        public String getAddress()
        {
            return address;
        }

        public String getRack()
        {
            return rack;
        }

        public String getStatus()
        {
            return status;
        }

        public String getState()
        {
            return state;
        }

        public String getToken()
        {
            return token;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RingInstanceDetails that = (RingInstanceDetails) o;
            return Objects.equals(address, that.address) &&
                   Objects.equals(rack, that.rack) &&
                   Objects.equals(status, that.status) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(token, that.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, rack, status, state, token);
        }

        public String toString()
        {
            return Arrays.asList(address, rack, status, state, token).toString();
        }
    }
}
