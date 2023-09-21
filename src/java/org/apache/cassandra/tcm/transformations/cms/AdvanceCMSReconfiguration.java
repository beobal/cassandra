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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.EntireRange;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

/**
 * Advances current CMS Reconfiguration
 */
public class AdvanceCMSReconfiguration implements Transformation
{
    public static final Serializer serializer = new Serializer();

    //Advance transformations can be retried. Idx is used to quickly and efficiently distinguish them one from another.
    public final int idx;
    public final LockedRanges.Key lockKey;

    public final PrepareCMSReconfiguration.Diff diff;
    public final ReconfigureCMS.ActiveTransition activeTransition;

    public AdvanceCMSReconfiguration(int idx, LockedRanges.Key lockKey, PrepareCMSReconfiguration.Diff diff, ReconfigureCMS.ActiveTransition active)
    {
        this.idx = idx;
        this.lockKey = lockKey;
        this.diff = diff;
        this.activeTransition = active;
    }

    private AdvanceCMSReconfiguration next(List<NodeId> additions, List<NodeId> removals, ReconfigureCMS.ActiveTransition active)
    {
        return new AdvanceCMSReconfiguration(idx + 1, lockKey, new PrepareCMSReconfiguration.Diff(additions, removals), active);
    }

    @Override
    public Kind kind()
    {
        return Kind.ADVANCE_CMS_RECONFIGURATION;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        InProgressSequences sequences = prev.inProgressSequences;
        InProgressSequence<?> sequence = sequences.get(ReconfigureCMS.SequenceKey.instance);

        if (sequence == null)
            return new Transformation.Rejected(INVALID, "Can't advance CMS Reconfiguration as it is not present in current metadata");

        if (!(sequence instanceof ReconfigureCMS))
            return new Transformation.Rejected(INVALID, "Can't execute finish join as cluster metadata contains a sequence of a different kind");

        ReconfigureCMS reconfigureCMS = (ReconfigureCMS) sequence;
        if (reconfigureCMS.next.idx != idx)
            return new Transformation.Rejected(INVALID, String.format("This transformation (%d) has already been applied. Expected: %d", idx, reconfigureCMS.next.idx));

        if (activeTransition == null)
        {
            // We execute additions before removals to avoid shrinking service and not being able to expand it later
            if (!diff.additions.isEmpty())
            {
                NodeId addition = diff.additions.get(0);
                List<NodeId> newAdditions = new ArrayList<>(diff.additions.subList(1, diff.additions.size()));
                return ReconfigureCMS.executeStartAdd(prev,
                                                      addition,
                                                      (inProgressSequences, streamCandidcates) -> inProgressSequences.with(ReconfigureCMS.SequenceKey.instance,
                                                                                                                           new ReconfigureCMS(next(newAdditions, diff.removals, new ReconfigureCMS.ActiveTransition(addition, streamCandidcates)))));
            }
            else if (!diff.removals.isEmpty())
            {
                NodeId removal = diff.removals.get(0);
                List<NodeId> newRemovals = new ArrayList<>(diff.removals.subList(1, diff.removals.size()));
                return ReconfigureCMS.executeRemove(prev,
                                                    removal,
                                                    (inProgressSequences, ignored_) -> inProgressSequences.with(ReconfigureCMS.SequenceKey.instance,
                                                                                                                new ReconfigureCMS(next(diff.additions, newRemovals, null))));
            }
            else
            {
                return Transformation.success(prev.transformer()
                                                  .with(prev.inProgressSequences.without(ReconfigureCMS.SequenceKey.instance))
                                                  .with(prev.lockedRanges.unlock(lockKey)),
                                              EntireRange.affectedRanges(prev));
            }
        }
        else
        {
            return ReconfigureCMS.executeFinishAdd(prev,
                                                   activeTransition.nodeId,
                                                   (inProgressSequences, ignored_) -> inProgressSequences.with(ReconfigureCMS.SequenceKey.instance,
                                                                                                               new ReconfigureCMS(next(diff.additions, diff.removals, null))));
        }
    }

    public String toString()
    {
        String current;
        if (activeTransition == null)
        {
            if (!diff.additions.isEmpty())
            {
                NodeId addition = diff.additions.get(0);
                current = "StartAddToCMS(" + addition + ")";
            }
            else if (!diff.removals.isEmpty())
            {
                NodeId removal = diff.removals.get(0);
                current = "RemoveFromCMS(" + removal + ")";
            }
            else
            {
                current = "FinishReconfiguration()";
            }
        }
        else
        {
            current = "FinishCMSReconfiguration()";
        }
        return "AdvanceCMSReconfiguration{" +
               "idx=" + idx +
               ", current=" + current +
               ", diff=" + diff +
               ", activeTransition=" + activeTransition +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceCMSReconfiguration>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceCMSReconfiguration transformation = (AdvanceCMSReconfiguration) t;
            out.writeUnsignedVInt32(transformation.idx);
            LockedRanges.Key.serializer.serialize(transformation.lockKey, out, version);

            PrepareCMSReconfiguration.Diff.serializer.serialize(transformation.diff, out, version);

            out.writeBoolean(transformation.activeTransition != null);
            if (transformation.activeTransition != null)
            {
                ReconfigureCMS.ActiveTransition activeTransition = transformation.activeTransition;
                NodeId.serializer.serialize(activeTransition.nodeId, out, version);
                out.writeInt(activeTransition.streamCandidates.size());
                for (InetAddressAndPort e : activeTransition.streamCandidates)
                    InetAddressAndPort.MetadataSerializer.serializer.serialize(e, out, version);
            }
        }

        public AdvanceCMSReconfiguration deserialize(DataInputPlus in, Version version) throws IOException
        {
            int idx = in.readUnsignedVInt32();
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            PrepareCMSReconfiguration.Diff diff = PrepareCMSReconfiguration.Diff.serializer.deserialize(in, version);

            boolean hasActiveTransition = in.readBoolean();
            ReconfigureCMS.ActiveTransition activeTransition = null;
            if (hasActiveTransition)
            {
                NodeId nodeId = NodeId.serializer.deserialize(in, version);
                int streamCandidatesCount = in.readInt();
                Set<InetAddressAndPort> streamCandidates = new HashSet<>();
                for (int i = 0; i < streamCandidatesCount; i++)
                    streamCandidates.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
                activeTransition = new ReconfigureCMS.ActiveTransition(nodeId, streamCandidates);
            }

            return new AdvanceCMSReconfiguration(idx, lockKey, diff, activeTransition);
        }

        public long serializedSize(Transformation t, Version version)
        {
            AdvanceCMSReconfiguration transformation = (AdvanceCMSReconfiguration) t;
            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(transformation.idx);
            size += LockedRanges.Key.serializer.serializedSize(transformation.lockKey, version);
            size += PrepareCMSReconfiguration.Diff.serializer.serializedSize(transformation.diff, version);

            size += TypeSizes.BOOL_SIZE;
            if (transformation.activeTransition != null)
            {
                ReconfigureCMS.ActiveTransition activeTransition = transformation.activeTransition;
                size += NodeId.serializer.serializedSize(activeTransition.nodeId, version);
                size += TypeSizes.INT_SIZE;
                for (InetAddressAndPort e : activeTransition.streamCandidates)
                    size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(e, version);
            }

            return size;
        }
    }
}
