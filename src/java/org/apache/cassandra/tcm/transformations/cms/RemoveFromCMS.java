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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

@Deprecated
public class RemoveFromCMS extends BaseMembershipTransformation
{
    private static final Logger logger = LoggerFactory.getLogger(RemoveFromCMS.class);
    public static final Serializer serializer = new Serializer();
    // Note: use CMS reconfiguration rather than manual addition/removal
    public static final int MIN_SAFE_CMS_SIZE = 3;
    public final boolean force;

    public RemoveFromCMS(InetAddressAndPort addr, boolean force)
    {
        super(addr);
        this.force = force;
    }

    @Override
    public Kind kind()
    {
        return Kind.REMOVE_FROM_CMS;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        InProgressSequences sequences = prev.inProgressSequences;
        NodeId nodeId = prev.directory.peerId(endpoint);
        InProgressSequence<?> sequence = sequences.get(nodeId);

        // This is theoretically permissible, but feels unsafe
        if (sequence != null)
            return new Transformation.Rejected(INVALID, "Can't remove node from CMS as there are ongoing range movements on it");

        ReplicationParams metaParams = ReplicationParams.meta(prev);
        DataPlacement placements = prev.placements.get(metaParams);

        int minProposedSize = (int) Math.min(placements.reads.forRange(replica.range()).get().stream().filter(r -> !r.endpoint().equals(endpoint)).count(),
                                             placements.writes.forRange(replica.range()).get().stream().filter(r -> !r.endpoint().equals(endpoint)).count());
        if (minProposedSize < MIN_SAFE_CMS_SIZE)
        {
            logger.warn("Removing {} from CMS members would reduce the service size to {} which is below the " +
                        "configured safe quorum {}. This requires the force option which is set to {}, {}proceeding",
                        endpoint, minProposedSize, MIN_SAFE_CMS_SIZE, force, force ? "" : "not ");
            if (!force)
            {
                return new Transformation.Rejected(INVALID, String.format("Removing %s from the CMS would reduce the number of members to " +
                                                                          "%d, below the configured soft minimum %d. " +
                                                                          "To perform this operation anyway, resubmit with force=true",
                                                                          endpoint, minProposedSize, MIN_SAFE_CMS_SIZE));
            }
        }

        if (minProposedSize == 0)
            return new Transformation.Rejected(INVALID, String.format("Removing %s from the CMS would leave no members in CMS.", endpoint));

        return ReconfigureCMS.executeRemove(prev, prev.directory.peerId(endpoint), (i, ignored_)  -> i);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '{' +
               "endpoint=" + endpoint +
               ", replica=" + replica +
               ", force=" + force +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof RemoveFromCMS)) return false;
        RemoveFromCMS that = (RemoveFromCMS) o;
        return Objects.equals(endpoint, that.endpoint) && Objects.equals(replica, that.replica) && force == that.force;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind(), endpoint, replica, force);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, RemoveFromCMS>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            RemoveFromCMS remove = (RemoveFromCMS)t;
            InetAddressAndPort.MetadataSerializer.serializer.serialize(remove.endpoint, out, version);
            out.writeBoolean(remove.force);
        }

        public RemoveFromCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            InetAddressAndPort addr = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            boolean force = in.readBoolean();
            return new RemoveFromCMS(addr, force);
        }

        public long serializedSize(Transformation t, Version version)
        {
            RemoveFromCMS remove = (RemoveFromCMS)t;
            return InetAddressAndPort.MetadataSerializer.serializer.serializedSize(remove.endpoint, version) +
                   TypeSizes.sizeof(remove.force);
        }
    }
}
