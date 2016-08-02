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

package org.apache.cassandra.auth.capability;

import java.util.concurrent.ExecutionException;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;

public class RestrictionsCache extends AuthCache<Pair<RoleResource, IResource>, CapabilitySet>
{
    public RestrictionsCache(ICapabilityManager capabilityManager)
    {
        super("RestrictedCapabilitiesCache",
              DatabaseDescriptor::setCapabilityRestrictionsValidity,
              DatabaseDescriptor::getCapabilityRestrictionsValidity,
              DatabaseDescriptor::setCapabilityRestrictionsUpdateInterval,
              DatabaseDescriptor::getCapabilityRestrictionsUpdateInterval,
              DatabaseDescriptor::setCapabilityRestrictionsCacheMaxEntries,
              DatabaseDescriptor::getCapabilityRestrictionsCacheMaxEntries,
              (p) -> capabilityManager.getRestricted(p.left, p.right),
              () -> true);
    }

    public CapabilitySet getRestrictedCapabilities(AuthenticatedUser user, IResource resource)
    {
        try
        {
            return get(Pair.create(user.getPrimaryRole(), resource));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
