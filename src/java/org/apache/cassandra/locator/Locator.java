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

package org.apache.cassandra.locator;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.RegistrationStateCallbacks;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

/**
 *
 */
// TODO rename
public class Locator implements RegistrationStateCallbacks
{
    private static final Logger logger = LoggerFactory.getLogger(Locator.class);

    private enum State {INITIAL, UNREGISTERED, REGISTERED};
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    private final InetAddressAndPort localEndpoint;
    private final Location initializationLocation;
    private final AtomicReference<VersionedLocation> local;     //  not initialized until ClusterMetadata is available

    private static class VersionedLocation
    {
        final Epoch epoch;
        final Location location;
        private VersionedLocation(Epoch epoch, Location location)
        {
            this.epoch = epoch;
            this.location = location;
        }
    }

    public Locator(InetAddressAndPort localEndpoint, InitialLocationProvider provider)
    {
        this.localEndpoint = localEndpoint;
        this.initializationLocation = provider.initialLocation();
        this.local = new AtomicReference<>(new VersionedLocation(Epoch.EMPTY, Location.UNKNOWN));
    }

    public static Locator forClients()
    {
        return new Locator(FBUtilities.getBroadcastAddressAndPort(), () -> Location.UNKNOWN);
    }

    @VisibleForTesting
    public void resetState()
    {
        state.set(State.INITIAL);
    }

    @Override
    public void onInitialized()
    {
        logger.info("Node is initialized, moving snitch adapter PREREGISTED state");
        if (!state.compareAndSet(State.INITIAL, State.UNREGISTERED))
            throw new IllegalStateException(String.format("Cannot move snitch adapter to UNREGISTERED state (%s)", state.get()));
    }

    @Override
    public void onRegistration()
    {
        // This may have been done already if the metadata log replay at start up included our registration
        State current = state.get();
        if (current == State.REGISTERED)
            return;

        logger.info("Node is registered, interrupting any previously established peer connections");
        state.getAndSet(State.REGISTERED);
        MessagingService.instance().channelManagers.keySet().forEach(MessagingService.instance()::interruptOutbound);
    }

    @Override
    public void onPeerRegistration(InetAddressAndPort endpoint)
    {
        logger.info("Peer has registered, interrupting any previously established connections");
        MessagingService.instance().interruptOutbound(endpoint);
    }

    public Location location(InetAddressAndPort endpoint)
    {
        switch (state.get())
        {
            case INITIAL:
                return endpoint.equals(localEndpoint) ? initializationLocation : Location.UNKNOWN;
            case UNREGISTERED:
                return endpoint.equals(localEndpoint) ? initializationLocation : fromClusterMetadata(endpoint);
            default:
                return fromClusterMetadata(endpoint);
        }
    }

    public Location local()
    {
        switch (state.get())
        {
            case INITIAL:
            case UNREGISTERED:
                return initializationLocation;
            default:
                // For now, local location is immutable and once registered with cluster metadata, it cannot be
                // changed. Revisit this if that assumption changes.
                VersionedLocation location = local.get();
                if (location.epoch.isAfter(Epoch.EMPTY))
                    return location.location;

                // We should never get to this point
                // TODO maybe replace with an assertion
                ClusterMetadata metadata = ClusterMetadata.current();
                Location registered = metadata.directory.location(metadata.myNodeId());
                local.set(new VersionedLocation(metadata.epoch, registered));
                return registered;
        }
    }

    private Location fromClusterMetadata(InetAddressAndPort endpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return Location.UNKNOWN;
        NodeId nodeId = metadata.directory.peerId(endpoint);
        return nodeId != null ? metadata.directory.location(nodeId) : Location.UNKNOWN;
    }

}
