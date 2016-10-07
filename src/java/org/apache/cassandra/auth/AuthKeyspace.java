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
package org.apache.cassandra.auth;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;

public final class AuthKeyspace
{
    private AuthKeyspace()
    {
    }

    public static final String ROLES = "roles";
    public static final String ROLE_MEMBERS = "role_members";
    public static final String ROLE_PERMISSIONS = "role_permissions";
    public static final String RESOURCE_ROLE_INDEX = "resource_role_permissons_index";
    public static final String ROLE_CAP_RESTRICTIONS = "role_capability_restrictions";
    public static final String RESOURCE_RESTRICTIONS_INDEX = "resource_role_restrictions_index";
    public static final String CACHE_METADATA = "cache_metadata";


    public static final long SUPERUSER_SETUP_DELAY = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);

    private static final CFMetaData Roles =
        compile(ROLES,
                "role definitions",
                "CREATE TABLE %s ("
                + "role text,"
                + "is_superuser boolean,"
                + "can_login boolean,"
                + "salted_hash text,"
                + "member_of set<text>,"
                + "PRIMARY KEY(role))");

    private static final CFMetaData RoleMembers =
        compile(ROLE_MEMBERS,
                "role memberships lookup table",
                "CREATE TABLE %s ("
                + "role text,"
                + "member text,"
                + "PRIMARY KEY(role, member))");

    private static final CFMetaData RolePermissions =
        compile(ROLE_PERMISSIONS,
                "permissions granted to db roles",
                "CREATE TABLE %s ("
                + "role text,"
                + "resource text,"
                + "permissions set<text>,"
                + "PRIMARY KEY(role, resource))");

    private static final CFMetaData ResourceRoleIndex =
        compile(RESOURCE_ROLE_INDEX,
                "index of db roles with permissions granted on a resource",
                "CREATE TABLE %s ("
                + "resource text,"
                + "role text,"
                + "PRIMARY KEY(resource, role))");

    private static final CFMetaData RoleRestrictions =
        compile(ROLE_CAP_RESTRICTIONS,
                "capability restrictions imposed on db roles",
                "CREATE TABLE %s ("
                + "role text,"
                + "resource text,"
                + "capabilities set<text>,"
                + "PRIMARY KEY(role, resource))");

    private static final CFMetaData ResourceRestrictionIndex =
        compile(RESOURCE_RESTRICTIONS_INDEX,
                "index of db roles with capability restrictions for a given resource",
                "CREATE TABLE %s ("
                + "resource text,"
                + "role text,"
                + "PRIMARY KEY(resource, role))");

    private static final CFMetaData CacheMetadata =
        compile(CACHE_METADATA,
                "system metadata for auth specific caches",
                "CREATE TABLE %s ("
                + "id text,"
                + "generation bigint,"
                + "PRIMARY KEY(id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SchemaConstants.AUTH_KEYSPACE_NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.AUTH_KEYSPACE_NAME,
                                       KeyspaceParams.simple(1),
                                       Tables.of(Roles,
                                                 RoleMembers,
                                                 RolePermissions,
                                                 ResourceRoleIndex,
                                                 RoleRestrictions,
                                                 ResourceRestrictionIndex,
                                                 CacheMetadata));
    }
}
