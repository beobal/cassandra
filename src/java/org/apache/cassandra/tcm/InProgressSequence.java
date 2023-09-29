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

package org.apache.cassandra.tcm;

import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.SequenceState;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;

/**
 * Represents a multi-step process performed in order to transition the cluster to some state.
 *
 * For example, in order to join, the joining node has to execute the following steps:
 *   * PrepareJoin, which introduces node's tokens, but makes no changes to range ownership, and creates BootstrapAndJoin
 *     in-progress sequence
 *   * StartJoin, which adds the bootstrapping node to the write placements for the ranges it gains
 *   * MidJoin, which adds the bootstrapping node to the read placements for the ranges it has gained, and removes
 *     owners of these ranges from the read placements
 *   * FinishJoin, which removes owners of the gained ranges from the write placements.
 *
 * A multi-step sequence necessarily holds all data required to execute the next step in sequence. All in-progress
 * sequences can be interleaved, as long as their internal logic permits. In other words, an arbitrary number of join,
 * move, leave, and replace sequences can be in flight at any point in time, as long as they operate on non-overlapping
 * ranges.
 *
 * It is assumed that the node may crash after committing any of the steps. {@link InProgressSequence#executeNext()}
 * should be implemented such that commits are done _after_ executing necessary prerequisites. For example,
 * streaming has to finish before MidJoin transformation (which adds the node to the read placements) is executed.
 */
public abstract class InProgressSequence<CONTEXT>
{
    // Represents the position in the sequence of the next step to be executed.
    public final int idx;
    // The epoch representing the last modification made by this sequence. Before executing a step in a sequence,
    // the node driving it will typically wait for this epoch to be acknowledged by the relevant peers. This helps
    // ensure step execution is coordinated across those peers.
    public final Epoch latestModification;

    protected InProgressSequence(int currentStep, Epoch latestModification)
    {
        this.idx = currentStep;
        this.latestModification = latestModification;
    }

    /**
     * Unique identifier for the type of sequence, e.g. JOIN, LEAVE, MOVE
     * @return the specific kind of this sequence
     */
    public abstract InProgressSequences.Kind kind();

    /**
     * Executes the next step in the sequence. This should usually include a Transformation to mutate ClusterMetadata
     * state, and _may_ also involve additional non-metadata operations such as streaming of SSTables to or from peers
     * (i.e. in the sequences implementing bootstrap, decommission, etc).
     *
     * Returns an indication of the sequence's state based on the outcome of executing the step.
     * This may express that the sequence can continue processing (executing further steps), report a fatal and
     * non-fatal errors, or indicate that further execution is blocked while waiting for acknowledgement of preceding
     * steps from peers.
     * @return sequence state following attempted execution
     */
    public abstract SequenceState executeNext();

    /**
     * Advance the state of in-progress sequence after successfully executing a step. Essentially, this "bumps the
     * pointer" into the list (actual or logical) of steps which comprise the sequence. It is most commonly called by
     * Transformations which represent the steps of the sequence. For example, in a sequence X comprising steps A, B, C
     * each step will typically modify ClusterMetadata such that the persisted representation of X indicates what the
     * next step to execute is. So at the start, A is the next step and X's internal state will indicate this. Part of
     * A's execution is to update that state to show that B is now the next step to run.
     *
     * The type parameter here represents an entity which can provide any state necessary to perform this advancement.
     * In cases where the entire sequence can be constructed a priori (as all the steps are known up front), this may
     * simply be the Epoch in which the step was executed. In more dynamic sequences, this parameter may express more
     * information. e.g. in the case of CMS reconfiguration, only a single step is known at a time and the paramter
     * supplied here is itself the next step.
     *
     * @param context required to move the sequence's state onto the next step
     * @return Logically this sequence, ready to execute the next step.
     */
    public abstract InProgressSequence<CONTEXT> advance(CONTEXT context);

    /**
     * When execution of steps needs to be coordinated across nodes in the cluster, ProgressBarrier enables the sequence
     * to wait for a quorum of peers to acknowledge an epoch before proceeding. For example, during a BootstrapAndJoin
     * sequence, a quorum of relevant nodes must acknowledge that the joining node has become a write replica (which is
     * an effect of the StartJoin transformation), before commencing the next step, which includes streaming existing
     * data from peers and making the joining node a read replica.
     * @return the barrier to wait on before executing the next step of the sequence.
     */
    public abstract ProgressBarrier barrier();

    /**
     * Whether or not the next step is the last in the sequence, this is useful to know when handling commit failure
     * during a sequence step (see {@link #commit(Transformation)}. This is also used to indicate if node which is
     * started in write survey mode or with -Djoin_ring=false is in a suitable state to fully join the cluster.
     * @return true if the next step is the last in the sequence, false otherwise
     */
    public abstract boolean atFinalStep();

    /**
     * Reverts any metadata changes that this sequence has made up to now. Used to cancel in flight sequences such as
     * bootstrapping. This is performed by the CancelInProgressSequence transformation, which is also responsible for
     * removing the sequence itself from the map in ClusterMetadata.
     * @param metadata current cluster metadata. Any metadata changes already committed by this sequence will be
     *                 reverted and the resulting metadata returned
     * @return the supplied metadata with this sequence's previously applied changes reverted
     */
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * The key under which this sequence is stored in {@link org.apache.cassandra.tcm.ClusterMetadata#inProgressSequences}
     * In many cases, this is a NodeId, restricting each peer to be the object of at most one bootstrap, token movement,
     * decommission, etc at a time.
     * @return the key to identify this sequence
     */
    protected abstract InProgressSequences.SequenceKey sequenceKey();

    /**
     * Provides the means to write this sequence's key as bytes. Used when serializing the map of current sequences
     * {@link org.apache.cassandra.tcm.ClusterMetadata#inProgressSequences} for snapshots and replication.
     * @return MetadataSerializer for the sequence's key
     */
    public MetadataSerializer<? extends InProgressSequences.SequenceKey> keySerializer()
    {
        return NodeId.serializer;
    }

    public String status()
    {
        return "kind: " + kind() + ", current step: " + idx + ", barrier: " + barrier();
    }

    /**
     * Specific mechanism to commit a transformation that is part of a sequence. This uses a rejection handler which
     * uses the sequence's index to determine whether a failure represents an genuine failure/rejection or a success
     * response that was dropped, missed or timed out.
     *
     * @param transformation to be committed. represents the metadata changes being made by a step in the sequence
     * @return the resulting metadata, which may be modified by the supplied transformation if the commit was successful
     */
    protected ClusterMetadata commit(Transformation transformation)
    {
        InProgressSequences.SequenceKey sequenceKey = sequenceKey();
        return ClusterMetadataService.instance().commit(transformation,
                                                        (metadata) -> metadata,
                                                        (metadata, code, reason) -> {
                                                            InProgressSequence<?> seq = metadata.inProgressSequences.get(sequenceKey);

                                                            // Suceeded after retry
                                                            if ((atFinalStep() && seq == null) ||
                                                                (!atFinalStep() && seq.idx == idx + 1))
                                                                return metadata;

                                                            throw new IllegalStateException(String.format("Could not commit %s: %s %s",
                                                                                                          transformation, code, reason));
                                                        });
    }
}
