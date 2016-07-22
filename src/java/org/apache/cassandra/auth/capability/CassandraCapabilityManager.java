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

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.auth.capability.Restriction.Specification.ANY_CAPABILITY;

public class CassandraCapabilityManager implements ICapabilityManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraCapabilityManager.class);
    private static final CassandraVersion MIN_CASSANDRA_VERSION = new CassandraVersion("3.10");

    private volatile boolean isClusterReady = false;

    private final RestrictionHandler loader;

    // needs a public, no-args constructor for FBUtilities.newCapabilityManager
    public CassandraCapabilityManager()
    {
        loader = new TableBasedRestrictionHandler();
    }

    @VisibleForTesting
    public CassandraCapabilityManager(RestrictionHandler loader)
    {
        this.loader = loader;
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        if (StorageService.instance.getTokenMetadata().countAllEndpoints() <= 1)
        {
            logger.debug("TokenMetadata contains only single endpoint, skipping peer version checks");
            isClusterReady = true;
        }
        else
        {
            scheduleClusterVersionCheck();
        }
    }

    public void createRestriction(AuthenticatedUser performedBy, Restriction restriction)
    {
        checkReadyStatus();
        loader.modifyForRole(restriction, "+");
    }

    public void dropRestriction(AuthenticatedUser performedBy, Restriction restriction)
    {
        checkReadyStatus();
        loader.modifyForRole(restriction, "-");
    }

    public void dropAllRestrictionsOn(RoleResource role)
    {
        checkReadyStatus();
        loader.removeAllForRole(role);
    }

    public void dropAllRestrictionsWith(IResource resource)
    {
        checkReadyStatus();
        loader.removeAllForResource(resource);
    }

    public ImmutableSet<Capability> getRestrictions(RoleResource primaryRole, IResource resource)
    {
        ImmutableSet.Builder<Capability> restricted = ImmutableSet.builder();
        Restriction.Specification spec = new Restriction.Specification(primaryRole, resource, ANY_CAPABILITY);
        loader.fetch(spec, true)
              .stream()
              .map(Restriction::getCapability)
              .forEach(restricted::add);
        return restricted.build();
    }

    public ImmutableSet<Restriction> listRestrictions(Restriction.Specification spec, boolean includeInherited)
    {
        checkReadyStatus();
        return loader.fetch(spec, includeInherited);
    }

    private void scheduleClusterVersionCheck()
    {
        ScheduledExecutors.optionalTasks.schedule(() -> {
            // If not all nodes are on at least 3.10, reject any attempts to access the
            // underlying tables as they may not be present on replicas which are still
            // running an older version. The implication is that until all nodes are
            // upgraded, DCL statements relating to restrictions will be rejected on the
            // coordinator and any attempts at checking restrictions will behave as if
            // AllowAllCapabilityManager is configured.
            if (!allPeersOnMinimumVersion())
            {
                logger.trace("Not all nodes are upgraded to a version that supports Capabilities yet," +
                             " rescheduling setup task");
                scheduleClusterVersionCheck();
                return;
            }
            isClusterReady = true;
      }, AuthKeyspace.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);
    }

    private void checkReadyStatus()
    {
        if (!isClusterReady)
            throw new InvalidRequestException(String.format("Capability restrictions are unavailable until all " +
                                                            "nodes are running a compatible version. Until the " +
                                                            "cluster is fully upgraded to at least %s this " +
                                                            "functionality will be disabled",
                                                            MIN_CASSANDRA_VERSION));
    }

    private boolean allPeersOnMinimumVersion()
    {
        logger.debug("Verifying all peers meet minimum version requirement of {}", MIN_CASSANDRA_VERSION);
        for (InetAddress endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints())
        {
            if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                continue;

            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (state == null)
                continue;

            if (Gossiper.instance.isDeadState(state))
                continue;

            if (state.getApplicationState(ApplicationState.RELEASE_VERSION) == null)
                return false;

            CassandraVersion version =
                new CassandraVersion(state.getApplicationState(ApplicationState.RELEASE_VERSION).value);

            if (version.compareTo(MIN_CASSANDRA_VERSION) < 0)
                return false;
        }
        logger.debug("All peers meet minimum version requirement");
        return true;
    }
}
