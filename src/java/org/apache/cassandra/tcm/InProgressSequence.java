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
 * Represents a multi-step process the node has to go through in order to transition to some state.
 *
 * For example, in order to join, the node has to execute the following steps:
 *   * PrepareJoin, which introduces node's token, but makes no changes to range ownership, and creates BootstrapAndJoin
 *     in-progress sequence
 *   * StartJoin, which adds the bootstrapping node to the write placements for the ranges it gains
 *   * MidJoin, which adds the bootstrapping node to the read placements for the ranges it has gained, and removes
 *     owners of these ranges from the read placements
 *   * FinishJoin, which removes owners of the gained ranges from the write placements.
 *
 * Multi-step sequence necessarily holds all data required to execute the next step in sequence. All in-progress sequences
 * can be interleaved, as long as their internal logic permits. In other words, an arbitrary number of join,
 * move, leave, and replace sequences can be in flight at any point in time, as long as they operate on non-overlapping
 * ranges.
 *
 * It is assumed that the node may crash after committing any of the steps. {@link InProgressSequence#executeNext()}
 * should be implemented such that commits are done _after_ executing necessary prerequisites. For example,
 * streaming has to finish before MidJoin transformation, adding the node to the read placements, is executed.
 */
public abstract class InProgressSequence<SELF extends InProgressSequence<SELF>>
{
    public abstract InProgressSequences.Kind kind();

    public MetadataSerializer<? extends InProgressSequences.SequenceKey> keySerializer()
    {
        return NodeId.serializer;
    }

    public abstract ProgressBarrier barrier();

    public String status()
    {
        return "kind: " + kind() + ", next step: " + nextStep() +" barrier: " + barrier();
    }

    /**
     * Returns a kind of the next step
     */
    public abstract Transformation.Kind nextStep();

    /**
     * Executes the next step. Returns whether or the sequence can continue / retry safely. Can return
     * false in cases when bootstrap streaming failed, or when the user has requested to halt the bootstrap sequence
     * and avoid joining the ring.
     */
    public abstract SequenceState executeNext();

    /**
     * Advance the state of in-progress sequence after execution
     */
    public abstract SELF advance(Epoch waitForWatermark);

    // TODO rename this. It really provides the result of undoing any steps in the sequence
    //      which have already been executed
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a kind of step that follows the given in the sequence.
     */
    protected abstract Transformation.Kind stepFollowing(Transformation.Kind kind);

    protected abstract InProgressSequences.SequenceKey sequenceKey();

    protected ClusterMetadata commit(Transformation transform)
    {
        InProgressSequences.SequenceKey sequenceKey = sequenceKey();
        assert nextStep() == transform.kind() : String.format(String.format("Expected %s to be next step, but got %s.", nextStep(), transform.kind()));
        return ClusterMetadataService.instance().commit(transform,
                      (metadata) -> metadata,
                      (metadata, code, reason) -> {
                          InProgressSequence<?> seq = metadata.inProgressSequences.get(sequenceKey);
                          Transformation.Kind actual = seq == null ? null : seq.nextStep();

                          Transformation.Kind expectedNextOp = stepFollowing(transform.kind());
                          // It is possible that we have committed this transformation in our attempt to retry
                          // after a timeout. For example, if we commit START_JOIN, we would get MID_JOIN as
                          // an actual op. Since MID_JOIN is also what we expect after committing START_JOIN,
                          // we assume that we have successfully committed it.
                          if (expectedNextOp != actual)
                              throw new IllegalStateException(String.format("Expected next operation to be %s, but got %s: %s", expectedNextOp, actual, reason));

                          // Suceeded after retry
                          return metadata;
                      });
    }
}
