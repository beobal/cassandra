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
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.FinishAddToCMS;
import org.apache.cassandra.tcm.transformations.cms.StartAddToCMS;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.JOIN_OWNERSHIP_GROUP;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;

/**
 * Add this or another node as a member of CMS.
 * This sequence is unlike other implementations in that it has no 'prepare' phase. The real first step of the
 * logical sequence, implemented by StartAddToCMS, is always executed first before the AddToCMS sequence itself
 * is constructed. In fact, StartAddToCMS could be considered to perform the 'prepare' phase as well as the
 * first step, which is to add the candidate as a write-only member of the CMS. Only once this has succeeded
 * does the sequence start in earnest, performing streaming for the global log tables before executing the
 * second and final logical step of promoting the candidate to a full read/write member of the CMS.
 *
 * @deprecated in favour of ReconfigureCMS
 * This class along with AddToCMS & RemoveFromCMS, contain a high degree of duplication with their intended
 * replacements ReconfigureCMS and AdvanceCMSReconfiguration. This shouldn't be a big problem as the intention is to
 * remove this superceded version asap.
 */
@Deprecated
public class AddToCMS extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(AddToCMS.class);
    public static Serializer serializer = new Serializer();

    private final NodeId toAdd;
    private final Set<InetAddressAndPort> streamCandidates;
    private final FinishAddToCMS finishJoin;

    public static void initiate()
    {
        initiate(ClusterMetadata.current().myNodeId(), FBUtilities.getBroadcastAddressAndPort());
    }

    public static void initiate(NodeId nodeId, InetAddressAndPort addr)
    {
        MultiStepOperation<?> sequence = ClusterMetadataService.instance()
                                                               .commit(new StartAddToCMS(addr),
                                                                       (metadata) -> metadata.inProgressSequences.get(nodeId),
                                                                       (metadata, code, reason) -> {
                                                                           MultiStepOperation<?> sequence_ = metadata.inProgressSequences.get(nodeId);

                                                                           if (sequence_ == null)
                                                                           {
                                                                               throw new IllegalStateException("Can't join ownership group: " + reason);
                                                                           }

                                                                           // We might have discovered a startup sequence we ourselves committed but got no response for
                                                                           if (sequence_.kind() != JOIN_OWNERSHIP_GROUP)
                                                                           {
                                                                               throw new IllegalStateException(String.format("Following accepted initiation of node to CMS, " +
                                                                                                                             "an incorrect sequence %s was found in progress. %s.\nRejection reason: %s",
                                                                                                                             sequence_.kind(), sequence_, reason));
                                                                           }

                                                                           return metadata.inProgressSequences.get(nodeId);
                                                                       });

        InProgressSequences.resume(sequence);
        ReconfigureCMS.repairPaxosTopology();
    }

    public AddToCMS(Epoch latestModification,
                    NodeId toAdd,
                    Set<InetAddressAndPort> streamCandidates,
                    FinishAddToCMS join)
    {
        super(0, latestModification);
        this.toAdd = toAdd;
        this.streamCandidates = streamCandidates;
        this.finishJoin = join;
    }

    @Override
    public ProgressBarrier barrier()
    {
        return ProgressBarrier.immediate();
    }

    @Override
    public SequenceState executeNext()
    {
        try
        {
            ReconfigureCMS.streamRanges(finishJoin.replicaForStreaming(), streamCandidates);
            commit(finishJoin);
        }
        catch (Throwable t)
        {
            logger.error("Could not finish adding the node to the metadata ownership group", t);
        }

        return continuable();
    }

    @Override
    public AddToCMS advance(Epoch waitForWatermark)
    {
        throw new NoSuchElementException();
    }

    @Override
    protected InProgressSequences.SequenceKey sequenceKey()
    {
        return toAdd;
    }

    @Override
    public Kind kind()
    {
        return JOIN_OWNERSHIP_GROUP;
    }

    public Transformation.Kind nextStep()
    {
        return Transformation.Kind.FINISH_ADD_TO_CMS;
    }

    @Override
    public MetadataSerializer<? extends InProgressSequences.SequenceKey> keySerializer()
    {
        return NodeId.serializer;
    }

    @Override
    public boolean atFinalStep()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddToCMS addMember = (AddToCMS) o;
        return Objects.equals(latestModification, addMember.latestModification) &&
               Objects.equals(streamCandidates, addMember.streamCandidates) &&
               Objects.equals(finishJoin, addMember.finishJoin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, streamCandidates, finishJoin);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, AddToCMS>
    {
        @Override
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            AddToCMS seq = (AddToCMS) t;
            NodeId.serializer.serialize(seq.toAdd,out, version);
            Epoch.serializer.serialize(seq.latestModification, out, version);
            FinishAddToCMS.serializer.serialize(seq.finishJoin, out, version);
            out.writeInt(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(ep, out, version);
        }

        @Override
        public AddToCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            Epoch barrier = Epoch.serializer.deserialize(in, version);
            FinishAddToCMS finish = FinishAddToCMS.serializer.deserialize(in, version);
            int streamCandidatesSize = in.readInt();
            Set<InetAddressAndPort> streamCandidates = new HashSet<>();

            for (int i = 0; i < streamCandidatesSize; i++)
                streamCandidates.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
            return new AddToCMS(barrier, nodeId, streamCandidates, finish);
        }

        @Override
        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            AddToCMS seq = (AddToCMS) t;
            long size = NodeId.serializer.serializedSize(seq.toAdd, version);
            size += Epoch.serializer.serializedSize(seq.latestModification, version);
            size += FinishAddToCMS.serializer.serializedSize(seq.finishJoin, version);
            size += sizeof(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(ep, version);
            return size;
        }
    }
}
