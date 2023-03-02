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

package org.apache.cassandra.tcm.compatibility;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionGenerator;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.gms.ApplicationState.DC;
import static org.apache.cassandra.gms.ApplicationState.HOST_ID;
import static org.apache.cassandra.gms.ApplicationState.INTERNAL_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.INTERNAL_IP;
import static org.apache.cassandra.gms.ApplicationState.NATIVE_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.RACK;
import static org.apache.cassandra.gms.ApplicationState.RPC_ADDRESS;
import static org.apache.cassandra.gms.ApplicationState.TOKENS;
import static org.apache.cassandra.gms.VersionedValue.unsafeMakeVersionedValue;
import static org.apache.cassandra.locator.InetAddressAndPort.getByName;
import static org.apache.cassandra.locator.InetAddressAndPort.getByNameOverrideDefaults;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class GossipHelper
{
    public static void updateSchemaVersionInGossip(UUID version)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA,
                                                   StorageService.instance.valueFactory.schema(version));
    }

    public static void removeFromGossip(InetAddressAndPort addr)
    {
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(addr));
    }

    /**
     * Basic idea is that we can't ever bump the generation or version for a remote node
     *
     * If the remote node is not yet known, set generation and version to 0 to make sure that we don't overwrite
     * any state generated by the remote node itself
     *
     * If the remote node is known, keep the remote generation and version and just update the versioned value in
     * place, this makes sure that if the remote node changed, those values will override anything we have here.
     */
    public static void mergeNodeToGossip(NodeId nodeId, ClusterMetadata metadata)
    {
        mergeNodeToGossip(nodeId, metadata, metadata.tokenMap.tokens(nodeId));
    }

    public static void mergeNodeToGossip(NodeId nodeId, ClusterMetadata metadata, Collection<Token> tokens)
    {
        boolean isLocal = nodeId.equals(metadata.myNodeId());
        IPartitioner partitioner = metadata.tokenMap.partitioner();
        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        Location location = metadata.directory.location(nodeId);
        InetAddressAndPort endpoint = addresses.broadcastAddress;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner,
                                                                                                     isLocal ? VersionGenerator::getNextVersion : () -> 0);
        Gossiper.runInGossipStageBlocking(() -> {
            EndpointState epstate = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epstate == null)
                epstate = new EndpointState(HeartBeatState.empty());
            Map<ApplicationState, VersionedValue> newStates = new EnumMap<>(ApplicationState.class);
            for (ApplicationState appState : ApplicationState.values())
            {
                VersionedValue value = epstate.getApplicationState(appState);
                VersionedValue newValue = null;
                switch (appState)
                {
                    case DC:
                        newValue = valueFactory.datacenter(location.datacenter);
                        break;
                    case RACK:
                        newValue = valueFactory.rack(location.rack);
                        break;
                    case RELEASE_VERSION:
                        newValue = valueFactory.releaseVersion(metadata.directory.version(nodeId).cassandraVersion.toString());
                        break;
                    case RPC_ADDRESS:
                        newValue = valueFactory.rpcaddress(endpoint.getAddress());
                        break;
                    case HOST_ID:
                        if (getBroadcastAddressAndPort().equals(addresses.broadcastAddress))
                            SystemKeyspace.setLocalHostId(nodeId.uuid);
                        newValue = valueFactory.hostId(nodeId.uuid);
                        break;
                    case TOKENS:
                        if (tokens != null)
                            newValue = valueFactory.tokens(tokens);
                        break;
                    case INTERNAL_ADDRESS_AND_PORT:
                        newValue = valueFactory.internalAddressAndPort(addresses.localAddress);
                        break;
                    case NATIVE_ADDRESS_AND_PORT:
                        newValue = valueFactory.nativeaddressAndPort(addresses.nativeAddress);
                        break;
                    case STATUS:
                        // only publish/add STATUS if there are non-upgraded hosts
                        if (metadata.directory.versions.values().stream().anyMatch(v -> !v.isUpgraded()) && tokens != null && !tokens.isEmpty())
                            newValue = nodeStateToStatus(metadata.directory.peerState(nodeId), tokens, valueFactory);
                        break;
                    case STATUS_WITH_PORT:
                        if (tokens != null && !tokens.isEmpty())
                            newValue = nodeStateToStatus(metadata.directory.peerState(nodeId), tokens, valueFactory);
                        break;
                    case INDEX_STATUS:
                        // TODO;
                    default:
                        newValue = value;
                }
                if (newValue != null)
                {
                    // note that version needs to be > -1 here, otherwise Gossiper#sendAll on generation change doesn't send it
                    if (!isLocal)
                        newValue = unsafeMakeVersionedValue(newValue.value, value == null ? 0 : value.version);
                    newStates.put(appState, newValue);
                }
            }
            HeartBeatState heartBeatState = new HeartBeatState(epstate.getHeartBeatState().getGeneration(), isLocal ? VersionGenerator.getNextVersion() : 0);
            Gossiper.instance.unsafeUpdateEpStates(endpoint, new EndpointState(heartBeatState, newStates));
        });
    }

    private static VersionedValue nodeStateToStatus(NodeState nodeState, Collection<Token> tokens, VersionedValue.VersionedValueFactory valueFactory)
    {
        VersionedValue status = null;
        switch (nodeState)
        {
            case JOINED:
                status = valueFactory.normal(tokens);
                break;
            case LEFT:
                status = valueFactory.left(tokens, Gossiper.computeExpireTime());
                break;
            case BOOTSTRAPPING:
                status = valueFactory.bootstrapping(tokens);
                break;
            case LEAVING:
                status = valueFactory.leaving(tokens);
                break;
            case MOVING:
                status = valueFactory.moving(tokens.iterator().next()); //todo
                break;
            case REGISTERED:
                break;
            default:
                throw new RuntimeException("Bad NodeState " + nodeState);
        }
        return status;
    }

    public static void mergeAllNodeStatesToGossip(ClusterMetadata metadata)
    {
        Gossiper.runInGossipStageBlocking(() -> {
            for (Map.Entry<NodeId, NodeState> entry : metadata.directory.states.entrySet())
                mergeNodeToGossip(entry.getKey(), metadata);
        });
    }

    private static Collection<Token> getTokensIn(IPartitioner partitioner, EndpointState epState)
    {
        try
        {
            if (epState == null)
                return Collections.emptyList();

            VersionedValue versionedValue = epState.getApplicationState(TOKENS);
            if (versionedValue == null)
                return Collections.emptyList();

            return TokenSerializer.deserialize(partitioner, new DataInputStream(new ByteArrayInputStream(versionedValue.toBytes())));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static NodeState toNodeState(InetAddressAndPort endpoint, EndpointState epState)
    {
        assert epState != null;

        String status = epState.getStatus();
        if (status.equals(VersionedValue.STATUS_NORMAL) ||
            status.equals(VersionedValue.SHUTDOWN))
            return NodeState.JOINED;
        if (status.equals(VersionedValue.STATUS_LEFT))
            return NodeState.LEFT;
        throw new IllegalStateException("Can't upgrade the first node when STATUS = " + status + " for node " + endpoint);
    }

    private static NodeAddresses getAddressesFromEndpointState(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (endpoint.equals(getBroadcastAddressAndPort()))
            return NodeAddresses.current();
        try
        {
            InetAddressAndPort local = getEitherState(endpoint, epState, INTERNAL_ADDRESS_AND_PORT, INTERNAL_IP, DatabaseDescriptor.getStoragePort());
            InetAddressAndPort nativeAddress = getEitherState(endpoint, epState, NATIVE_ADDRESS_AND_PORT, RPC_ADDRESS, DatabaseDescriptor.getNativeTransportPort());
            return new NodeAddresses(endpoint, local, nativeAddress);
        }
        catch (UnknownHostException e)
        {
            throw new ConfigurationException("Unknown host in epState for " + endpoint + " : " + epState, e);
        }
    }

    private static InetAddressAndPort getEitherState(InetAddressAndPort endpoint,
                                                     EndpointState epState,
                                                     ApplicationState primaryState,
                                                     ApplicationState deprecatedState,
                                                     int defaultPortForDeprecatedState) throws UnknownHostException
    {
        if (epState.getApplicationState(primaryState) != null)
        {
            return getByName(epState.getApplicationState(primaryState).value);
        }
        else if (epState.getApplicationState(deprecatedState) != null)
        {
            return getByNameOverrideDefaults(epState.getApplicationState(deprecatedState).value, defaultPortForDeprecatedState);
        }
        else
        {
            return endpoint.withPort(defaultPortForDeprecatedState);
        }
    }

    private static NodeVersion getVersionFromEndpointState(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (endpoint.equals(getBroadcastAddressAndPort()))
            return NodeVersion.CURRENT;
        CassandraVersion cassandraVersion = epState.getReleaseVersion();
        return NodeVersion.fromCassandraVersion(cassandraVersion);
    }

    public static ClusterMetadata emptyWithSchemaFromSystemTables()
    {
        return new ClusterMetadata(Epoch.UPGRADE_STARTUP,
                                   Period.EMPTY,
                                   true,
                                   DatabaseDescriptor.getPartitioner(),
                                   DistributedSchema.fromSystemTables(SchemaKeyspace.fetchNonSystemKeyspaces()),
                                   Directory.EMPTY,
                                   new TokenMap(DatabaseDescriptor.getPartitioner()),
                                   DataPlacements.empty(),
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   Collections.emptyMap());
    }

    public static ClusterMetadata fromEndpointStates(DistributedSchema schema, Map<InetAddressAndPort, EndpointState> epStates)
    {
        return fromEndpointStates(epStates, DatabaseDescriptor.getPartitioner(), schema);
    }
    @VisibleForTesting
    public static ClusterMetadata fromEndpointStates(Map<InetAddressAndPort, EndpointState> epStates, IPartitioner partitioner, DistributedSchema schema)
    {
        Directory directory = new Directory();
        TokenMap tokenMap = new TokenMap(partitioner);
        List<InetAddressAndPort> sortedEps = Lists.newArrayList(epStates.keySet());
        Collections.sort(sortedEps);
        Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions = new HashMap<>();
        for (InetAddressAndPort endpoint : sortedEps)
        {
            EndpointState epState = epStates.get(endpoint);
            String dc = epState.getApplicationState(DC).value;
            String rack = epState.getApplicationState(RACK).value;
            String hostIdString = epState.getApplicationState(HOST_ID).value;
            NodeAddresses nodeAddresses = getAddressesFromEndpointState(endpoint, epState);
            NodeVersion nodeVersion = getVersionFromEndpointState(endpoint, epState);
            assert hostIdString != null;
            directory = directory.withNonUpgradedNode(nodeAddresses,
                                                      new Location(dc, rack),
                                                      nodeVersion,
                                                      toNodeState(endpoint, epState),
                                                      UUID.fromString(hostIdString));
            NodeId nodeId = directory.peerId(endpoint);
            tokenMap = tokenMap.assignTokens(nodeId, getTokensIn(partitioner, epState));
        }

        ClusterMetadata forPlacementCalculation = new ClusterMetadata(Epoch.UPGRADE_GOSSIP,
                                                                      Period.EMPTY,
                                                                      true,
                                                                      partitioner,
                                                                      schema,
                                                                      directory,
                                                                      tokenMap,
                                                                      DataPlacements.empty(),
                                                                      LockedRanges.EMPTY,
                                                                      InProgressSequences.EMPTY,
                                                                      extensions);
        return new ClusterMetadata(Epoch.UPGRADE_GOSSIP,
                                   Period.EMPTY,
                                   true,
                                   partitioner,
                                   schema,
                                   directory,
                                   tokenMap,
                                   new UniformRangePlacement().calculatePlacements(forPlacementCalculation, schema.getKeyspaces()),
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   extensions);
    }
}
