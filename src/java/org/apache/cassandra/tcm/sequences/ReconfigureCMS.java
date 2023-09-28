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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.DataMovement;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.EntireRange;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.streaming.StreamOperation.RESTORE_REPLICA_COUNT;
import static org.apache.cassandra.tcm.ownership.EntireRange.entireRange;

public class ReconfigureCMS extends InProgressSequence<ReconfigureCMS>
{
    public static final Serializer serializer = new Serializer();
    private static final Logger logger = LoggerFactory.getLogger(ReconfigureCMS.class);

    /**
     * We store the state (lock key, diff, and active transition) in the transformation to make debugging simpler,
     * since otherwise active transition, additions, and removals would have to be taken from ClusterMetadata, and
     * `Enacted` log messages would be less useful.
     */
    public final AdvanceCMSReconfiguration next;

    public ReconfigureCMS(LockedRanges.Key lockKey, PrepareCMSReconfiguration.Diff diff, ReconfigureCMS.ActiveTransition active)
    {
        this(new AdvanceCMSReconfiguration(0, lockKey, diff, active));
    }

    public ReconfigureCMS(AdvanceCMSReconfiguration next)
    {
        this.next = next;
    }

    public static Transformation.Result executeStartAdd(ClusterMetadata prev, NodeId nodeId, BiFunction<InProgressSequences, Set<InetAddressAndPort>, InProgressSequences> updateInProgressSequenes)
    {
        InetAddressAndPort endpoint = prev.directory.endpoint(nodeId);
        Replica replica = new Replica(endpoint, entireRange, true);

        ReplicationParams metaParams = ReplicationParams.meta(prev);
        RangesByEndpoint readReplicas = prev.placements.get(metaParams).reads.byEndpoint();
        RangesByEndpoint writeReplicas = prev.placements.get(metaParams).writes.byEndpoint();

        if (readReplicas.containsKey(endpoint) || writeReplicas.containsKey(endpoint))
            return new Transformation.Rejected(INVALID, "Endpoint is already a member of CMS");

        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild()
                                                       .withWriteReplica(prev.nextEpoch(), replica);

        transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build());

        Set<InetAddressAndPort> streamCandidates = new HashSet<>();
        for (Replica r : prev.placements.get(metaParams).reads.byEndpoint().flattenValues())
        {
            if (!replica.equals(r))
                streamCandidates.add(r.endpoint());
        }

        return Transformation.success(transformer.with(updateInProgressSequenes.apply(prev.inProgressSequences, streamCandidates)),
                                      EntireRange.affectedRanges(prev));
    }

    public static Transformation.Result executeFinishAdd(ClusterMetadata prev, NodeId nodeId, Function<InProgressSequences, InProgressSequences> updateInProgressSequenes)
    {
        ReplicationParams metaParams = ReplicationParams.meta(prev);
        InetAddressAndPort endpoint = prev.directory.endpoint(nodeId);
        Replica replica = new Replica(endpoint, entireRange, true);

        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(metaParams)
                                                       .unbuild()
                                                       .withReadReplica(prev.nextEpoch(), replica);
        transformer = transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build());

        return Transformation.success(transformer.with(updateInProgressSequenes.apply(prev.inProgressSequences)),
                                      EntireRange.affectedRanges(prev));
    }

    public static Transformation.Result executeRemove(ClusterMetadata prev, NodeId nodeId, Function<InProgressSequences, InProgressSequences> updateInProgressSequenes)
    {
        ClusterMetadata.Transformer transformer = prev.transformer();
        InetAddressAndPort endpoint = prev.directory.endpoint(nodeId);
        Replica replica = new Replica(endpoint, entireRange, true);
        ReplicationParams metaParams = ReplicationParams.meta(prev);

        if (!prev.fullCMSMembers().contains(endpoint))
            return new Transformation.Rejected(INVALID, String.format("%s is not currently a CMS member, cannot remove it", endpoint));

        DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild();
        builder.reads.withoutReplica(prev.nextEpoch(), replica);
        builder.writes.withoutReplica(prev.nextEpoch(), replica);
        DataPlacement proposed = builder.build();

        if (proposed.reads.byEndpoint().isEmpty() || proposed.writes.byEndpoint().isEmpty())
            return new Transformation.Rejected(INVALID, String.format("Removing %s will leave no nodes in CMS", endpoint));

        return Transformation.success(transformer.with(prev.placements.unbuild().with(metaParams, proposed).build())
                                                 .with(updateInProgressSequenes.apply(prev.inProgressSequences)),
                                      EntireRange.affectedRanges(prev));
    }

    public static void maybeReconfigureCMS(ClusterMetadata metadata, InetAddressAndPort toRemove)
    {
        if (!metadata.fullCMSMembers().contains(toRemove))
            return;

        // We can force removal from the CMS as it doesn't alter the size of the service
        ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Simple(metadata.directory.peerId(toRemove)),
                                                 latest -> latest,
                                                 (latest, code, message) -> {
                                                     InProgressSequence<?> sequence = metadata.inProgressSequences.get(SequenceKey.instance);
                                                     if (sequence != null)
                                                         return null;

                                                     throw new IllegalStateException(String.format("Can not commit event to metadata service: \"%s\"(%s). Interrupting startup sequence.",
                                                                                                   code, message));
                                                 });

        StorageService.instance.finishInProgressSequences(SequenceKey.instance);
        if (ClusterMetadata.current().isCMSMember(toRemove))
            throw new IllegalStateException(String.format("Could not remove %s from CMS", toRemove));
    }

    private static void initiateRemoteStreaming(Replica replicaForStreaming, Set<InetAddressAndPort> streamCandidates)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        EndpointsForRange.Builder efr = EndpointsForRange.builder(entireRange);
        streamCandidates.forEach(addr -> efr.add(new Replica(addr, entireRange, true)));

        MovementMap movements = MovementMap.builder().put(ReplicationParams.meta(metadata),
                                                          new EndpointsByReplica(Collections.singletonMap(replicaForStreaming, efr.build())))
                                           .build();

        String operationId = replicaForStreaming.toString();
        DataMovements.ResponseTracker responseTracker = DataMovements.instance.registerMovements(RESTORE_REPLICA_COUNT, operationId, movements);
        movements.byEndpoint().forEach((ep, epMovements) -> {
            DataMovement msg = new DataMovement(operationId, RESTORE_REPLICA_COUNT.name(), epMovements);
            MessagingService.instance().sendWithCallback(Message.out(Verb.INITIATE_DATA_MOVEMENTS_REQ, msg), ep, response -> {
                logger.debug("Endpoint {} starting streams {}", response.from(), epMovements);
            });
        });

        try
        {
            responseTracker.await();
        }
        finally
        {
            DataMovements.instance.unregisterMovements(RESTORE_REPLICA_COUNT, operationId);
        }
    }
    public static void streamRanges(Replica replicaForStreaming, Set<InetAddressAndPort> streamCandidates) throws ExecutionException, InterruptedException
    {
        InetAddressAndPort endpoint = replicaForStreaming.endpoint();

        // Current node is the streaming target. We can pick any other live CMS node as a streaming source
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            StreamPlan streamPlan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, true, null, PreviewKind.NONE);
            Optional<InetAddressAndPort> streamingSource = streamCandidates.stream().filter(FailureDetector.instance::isAlive).findFirst();
            if (!streamingSource.isPresent())
                throw new IllegalStateException(String.format("Can not start range streaming as all candidates (%s) are down", streamCandidates));
            streamPlan.requestRanges(streamingSource.get(),
                                     SchemaConstants.METADATA_KEYSPACE_NAME,
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).add(replicaForStreaming).build(),
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).build(),
                                     DistributedMetadataLogKeyspace.TABLE_NAME);
            streamPlan.execute().get();
        }
        // Current node is a live CMS node, therefore the streaming source
        else if (streamCandidates.contains(FBUtilities.getBroadcastAddressAndPort()))
        {
            StreamPlan streamPlan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, true, null, PreviewKind.NONE);
            streamPlan.transferRanges(endpoint,
                                      SchemaConstants.METADATA_KEYSPACE_NAME,
                                      new RangesAtEndpoint.Builder(replicaForStreaming.endpoint()).add(replicaForStreaming).build(),
                                      DistributedMetadataLogKeyspace.TABLE_NAME);
            streamPlan.execute().get();
        }
        // We are neither a target, nor a source, so initiate streaming on the target
        else
        {
            initiateRemoteStreaming(replicaForStreaming, streamCandidates);
        }

    }

    static void repairPaxosTopology()
    {
        Retry.Backoff retry = new Retry.Backoff(TCMMetrics.instance.repairPaxosTopologyRetries);
        List<Supplier<Future<?>>> remaining = ActiveRepairService.instance().repairPaxosForTopologyChangeAsync(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                                                                               Collections.singletonList(entireRange),
                                                                                                               "bootstrap");

        while (!retry.reachedMax())
        {
            Map<Supplier<Future<?>>, Future<?>> tasks = new HashMap<>();
            for (Supplier<Future<?>> supplier : remaining)
                tasks.put(supplier, supplier.get());
            remaining.clear();
            logger.info("Performing paxos topology repair on:", remaining);

            for (Map.Entry<Supplier<Future<?>>, Future<?>> e : tasks.entrySet())
            {
                try
                {
                    e.getValue().get();
                }
                catch (ExecutionException t)
                {
                    logger.error("Caught an exception while repairing paxos topology.", e);
                    remaining.add(e.getKey());
                }
                catch (InterruptedException t)
                {
                    return;
                }
            }

            if (remaining.isEmpty())
                return;

            retry.maybeSleep();
        }
        logger.error(String.format("Added node as a CMS, but failed to repair paxos topology after this operation."));
    }

    public InProgressSequences.Kind kind()
    {
        return InProgressSequences.Kind.RECONFIGURE_CMS;
    }

    public ProgressBarrier barrier()
    {
        return ProgressBarrier.immediate();
    }

    public Transformation.Kind nextStep()
    {
        return Transformation.Kind.ADVANCE_CMS_RECONFIGURATION;
    }

    @Override
    public SequenceState executeNext()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        InProgressSequence<?> inProgressSequence = metadata.inProgressSequences.get(SequenceKey.instance);
        if (inProgressSequence.kind() != InProgressSequences.Kind.RECONFIGURE_CMS)
            throw new IllegalStateException(String.format("Can not advance in-progress sequence, since its kind is %s, but not %s", inProgressSequence.kind(), InProgressSequences.Kind.RECONFIGURE_CMS));
        ReconfigureCMS transitionCMS = (ReconfigureCMS) inProgressSequence;
        try
        {
            if (transitionCMS.next.activeTransition != null)
            {
                ActiveTransition activeTransition = transitionCMS.next.activeTransition;
                InetAddressAndPort endpoint = metadata.directory.endpoint(activeTransition.nodeId);
                Replica replica = new Replica(endpoint, entireRange, true);
                streamRanges(replica, transitionCMS.next.activeTransition.streamCandidates);
            }

            commit(transitionCMS.next);
            metadata = ClusterMetadata.current();
            new ProgressBarrier(metadata.epoch, metadata.directory.location(metadata.myNodeId()), EntireRange.affectedRanges(metadata)).await();
            return SequenceState.continuable();
        }
        catch (Throwable t)
        {
            logger.error("Could not finish adding the node to the metadata ownership group", t);
            return SequenceState.blocked();
        }
    }

    public ReconfigureCMS advance(Epoch waitForWatermark)
    {
        throw new IllegalStateException("TransitionCMS advances im-progress sequences manually");
    }

    protected Transformation.Kind stepFollowing(Transformation.Kind kind)
    {
        int steps = 0;
        if (next.activeTransition != null)
            steps += 1;
        if (!next.diff.additions.isEmpty())
            steps += next.diff.additions.size() * 2;
        if (!next.diff.removals.isEmpty())
            steps += next.diff.removals.size();

        if (steps > 1)
            return Transformation.Kind.ADVANCE_CMS_RECONFIGURATION;
        else
            return null;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return SequenceKey.instance;
    }

    public static class ActiveTransition
    {
        public final NodeId nodeId;
        public final Set<InetAddressAndPort> streamCandidates;

        public ActiveTransition(NodeId nodeId, Set<InetAddressAndPort> streamCandidates)
        {
            this.nodeId = nodeId;
            this.streamCandidates = Collections.unmodifiableSet(streamCandidates);
        }
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, ReconfigureCMS>
    {

        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            ReconfigureCMS transformation = (ReconfigureCMS) t;
            AdvanceCMSReconfiguration.serializer.serialize(transformation.next, out, version);
        }

        public ReconfigureCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ReconfigureCMS(AdvanceCMSReconfiguration.serializer.deserialize(in, version));
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            ReconfigureCMS transformation = (ReconfigureCMS) t;
            return AdvanceCMSReconfiguration.serializer.serializedSize(transformation.next, version);
        }
    }

    public static class SequenceKey implements InProgressSequences.SequenceKey {
        public static SequenceKey instance = new SequenceKey();
        public static Serializer serializer = new Serializer();

        private SequenceKey(){}

        public static class Serializer implements MetadataSerializer<SequenceKey>
        {

            public void serialize(SequenceKey t, DataOutputPlus out, Version version) throws IOException
            {
            }

            public SequenceKey deserialize(DataInputPlus in, Version version) throws IOException
            {
                return instance;
            }

            public long serializedSize(SequenceKey t, Version version)
            {
                return 0;
            }
        }
    }
}
