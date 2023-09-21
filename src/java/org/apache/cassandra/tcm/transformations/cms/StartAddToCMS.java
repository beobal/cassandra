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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;

@Deprecated
public class StartAddToCMS extends BaseMembershipTransformation
{
    public static final AsymmetricMetadataSerializer<Transformation, StartAddToCMS> serializer = new SerializerBase<StartAddToCMS>()
    {
        public StartAddToCMS createTransformation(InetAddressAndPort addr)
        {
            return new StartAddToCMS(addr);
        }
    };

    public StartAddToCMS(InetAddressAndPort addr)
    {
        super(addr);
    }

    @Override
    public Kind kind()
    {
        return Kind.START_ADD_TO_CMS;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        NodeId nodeId = prev.directory.peerId(endpoint);

        return ReconfigureCMS.executeStartAdd(prev, nodeId, (inProgressSequences, streamCandidates) -> {
            AddToCMS joinSequence = new AddToCMS(prev.nextEpoch(), nodeId, streamCandidates, new FinishAddToCMS(endpoint));

            return inProgressSequences.with(nodeId, joinSequence);
        });
    }

    @Override
    public String toString()
    {
        return "StartAddMember{" +
               "endpoint=" + endpoint +
               ", replica=" + replica +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return super.equals(o);
    }
}
