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

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.cache.GenerationalCacheController;
import org.apache.cassandra.auth.cache.GenerationalCacheService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestExecutionException;

public class SnapshotRestrictionHandler implements RestrictionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotRestrictionHandler.class);

    private static final String CACHE_IDENTIFIER = "cap_res";

    private static final Holder EMPTY = new Holder(ImmutableTable.of());

    // Reference for the actual restriction data, so we can reload
    // in the background and swap in the new data atomically
    private final AtomicReference<Holder> data = new AtomicReference<>(EMPTY);

    // Delegate used by the bulk load method to fetch all defined restrictions
    private final RestrictionHandler persistenceHandler;

    // Notifies when a full background reload is required &
    // handles notifying peers about updates of local origin
    private final GenerationalCacheController controller;

    @VisibleForTesting
    SnapshotRestrictionHandler(RestrictionHandler persistenceHandler, GenerationalCacheController controller)
    {
        this.persistenceHandler = persistenceHandler;
        this.controller = controller;
    }

    SnapshotRestrictionHandler()
    {
        this.persistenceHandler = new TableBasedRestrictionHandler();
        this.controller =
            GenerationalCacheService.instance.registerCache(CACHE_IDENTIFIER,
                                                            DatabaseDescriptor.getAuthAggressiveCachingUpdateInterval(),
                                                            this::loadAll);
    }

    public void init()
    {
        loadAll();
        // initialize the new cache
        controller.init();
    }

    private void loadAll()
    {
        logger.debug("Refreshing all capability restrictions from base data");
        Restriction.Specification spec = new Restriction.Specification(Restriction.Specification.ANY_ROLE,
                                                                       Restriction.Specification.ANY_RESOURCE,
                                                                       Restriction.Specification.ANY_CAPABILITY);

        ImmutableSet<Restriction> all = persistenceHandler.fetch(spec, false);

        // build a temporary structure where we can accumulate the capabilities for each CapabilitySet
        Table<RoleResource, String, CapabilitySet.Builder> tempTable = HashBasedTable.create();
        for (Restriction restriction : all)
        {
            CapabilitySet.Builder builder = tempTable.get(restriction.getRole(), restriction.getResourceName());
            if (builder == null)
            {
                builder = new CapabilitySet.Builder();
                tempTable.put(restriction.getRole(), restriction.getResourceName(), builder);
            }
            builder.add(restriction.getCapability());
        }

        // finalize the data by copying to a new table, calling build() on each builder as we do
        Table<RoleResource, String, CapabilitySet> table = HashBasedTable.create();
        tempTable.cellSet()
                 .forEach(cell -> table.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue().build()));

        // swap in the new view
        while (true)
        {
            Holder current = data.get();
            if (data.compareAndSet(current, new Holder(table)))
            {
                logger.debug("Finished refreshing all capability restrictions from base data");
                return;
            }
        }
    }

    public CapabilitySet getRestrictions(RoleResource primaryRole, IResource resource)
    {
        Table<RoleResource, String, CapabilitySet> current = data.get().restrictions;
        List<CapabilitySet> capSets = new ArrayList<>();
        for (RoleResource role : Iterables.concat(Collections.singleton(primaryRole), Roles.getRoles(primaryRole)))
        {
            if (!current.contains(role, resource.getName()))
                continue;

            CapabilitySet capSet = current.get(role, resource.getName());

            if (capSet != null)
                capSets.add(capSet);
        }
        return CapabilitySet.union(capSets);
    }

    public ImmutableSet<Restriction> fetch(final Restriction.Specification spec, boolean includeInherited)
    {
        Table<RoleResource, String, CapabilitySet> current = data.get().restrictions;

        // Spec has neither role and resource are bound so stream through
        // all restrictions and let the spec filter by capability (which may
        // also be unbound). e.g.
        // LIST RESTRICTIONS
        // LIST RESTRICTIONS ON ANY ROLE USING <capability> WITH ANY RESOURCE
        if (spec.isAnyRole() && spec.isAnyResource())
            return current.cellSet()
                          .stream()
                          .flatMap(SnapshotRestrictionHandler::toRestrictions)
                          .filter(spec::matches)
                          .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

        // Where the the resource is bound but the role is not, use the
        // byResource index, e.g.
        // LIST RESTRICTIONS ON ANY ROLE USING ANY CAPABILITY WITH <resource>
        if (spec.isAnyRole() && !spec.isAnyResource())
            return current.column(spec.getResource().getName())
                          .entrySet()
                          .stream()
                          .flatMap(mapping -> toRestrictions(spec.getResource().getName(), mapping))
                          .filter(spec::matches)
                          .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

        // Spec is bound to a specific role. If we're only interested in restrictions
        // against that role directly, just pull them straight from the table
        if (!includeInherited)
            return current.row(spec.getRole())
                          .entrySet()
                          .stream()
                          .flatMap(mapping -> toRestrictions(spec.getRole(), mapping))
                          .filter(spec::matches)
                          .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

        // We're interested in the full set of effective restrictions for a
        // specific role, so include all roles granted to, or inherited by,
        // the one specified. Identify those roles, then modify the spec to
        // filter more permissively. Roles will be filtered before the spec
        // is used, so it will only be examining restrictions on valid roles
        Set<RoleResource> roles = Roles.getRoles(spec.getRole());
        Restriction.Specification newSpec = spec.withUpdatedRole(Restriction.Specification.ANY_ROLE);
        return current.rowMap()
                      .entrySet()
                      .stream()
                      .filter(entry -> roles.contains(entry.getKey()))
                      .flatMap(SnapshotRestrictionHandler::toRestrictions)
                      .filter(newSpec::matches)
                      .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));
    }

    public void modifyForRole(Restriction restriction, Operator operator) throws RequestExecutionException
    {
        persistenceHandler.modifyForRole(restriction, operator);
        controller.notifyUpdate();
    }

    public void removeAllForRole(RoleResource role)
    {
        persistenceHandler.removeAllForRole(role);
        controller.notifyUpdate();
    }

    public void removeAllForResource(IResource resource)
    {
        persistenceHandler.removeAllForResource(resource);
        controller.notifyUpdate();
    }

    private static Stream<Restriction> toRestrictions(Table.Cell<RoleResource, String, CapabilitySet> cell)
    {
        return toRestrictions(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
    }

    private static Stream<Restriction> toRestrictions(final String resource,
                                                      Map.Entry<RoleResource, CapabilitySet> forRole)
    {
        return toRestrictions(forRole.getKey(), resource, forRole.getValue());
    }

    private static Stream<Restriction> toRestrictions(final RoleResource role,
                                                      Map.Entry<String, CapabilitySet> forResource)
    {
        return toRestrictions(role, forResource.getKey(), forResource.getValue());
    }

    private static Stream<Restriction> toRestrictions(Map.Entry<RoleResource, Map<String, CapabilitySet>> allForRole)
    {
        return allForRole.getValue()
                         .entrySet()
                         .stream()
                         .flatMap(entry -> toRestrictions(allForRole.getKey(), entry));
    }

    private static Stream<Restriction> toRestrictions(RoleResource role, String resource, CapabilitySet capabilities)
    {
        return StreamSupport.stream(capabilities.spliterator(), false)
                            .map(cap -> new Restriction(role, resource, cap));
    }

    private static final class Holder
    {
        final Table<RoleResource, String, CapabilitySet> restrictions;

        Holder(Table<RoleResource, String, CapabilitySet> restrictions)
        {
            this.restrictions = restrictions;
        }
    }
}
