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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.capability.AllowAllCapabilityManager;
import org.apache.cassandra.auth.capability.CassandraCapabilityManager;
import org.apache.cassandra.auth.capability.ICapabilityManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Only purpose is to Initialize authentication/authorization via {@link #applyAuth()}.
 * This is in this separate class as it implicitly initializes schema stuff (via classes referenced in here).
 */
public final class AuthConfig
{
    private static final Logger logger = LoggerFactory.getLogger(AuthConfig.class);

    private static boolean initialized;

    public static void applyAuth()
    {
        // some tests need this
        if (initialized)
            return;

        initialized = true;

        Config conf = DatabaseDescriptor.getRawConfig();

        IAuthenticator authenticator = new AllowAllAuthenticator();

        /* Authentication, authorization and role management backend,
           implementing IAuthenticator, IAuthorizer & IRoleManager */

        if (conf.authenticator != null)
            authenticator = FBUtilities.newAuthenticator(conf.authenticator);

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(authenticator instanceof PasswordAuthenticator)
            && (conf.credentials_update_interval_in_ms != -1
                || conf.credentials_validity_in_ms != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval_in_ms, credentials_validity_in_ms and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        authenticator.getClass().getName());
        }

        DatabaseDescriptor.setAuthenticator(authenticator);

        // authorizer

        IAuthorizer authorizer = new AllowAllAuthorizer();

        if (conf.authorizer != null)
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
            throw new ConfigurationException(conf.authenticator + " can't be used with " + conf.authorizer, false);

        DatabaseDescriptor.setAuthorizer(authorizer);

        // role manager

        IRoleManager roleManager;
        if (conf.role_manager != null)
            roleManager = FBUtilities.newRoleManager(conf.role_manager);
        else
            roleManager = new CassandraRoleManager();

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);

        DatabaseDescriptor.setRoleManager(roleManager);

        // capability manager

        ICapabilityManager capabilityManager = new AllowAllCapabilityManager();

        // aggressive caching is currently only available for capability restrictions
        // if enabled, the regular AuthCache for restrictions is turned off.
        // minimum refresh interval for the aggressive caching is 1000ms, so if a
        // lower value is set in yaml, log a warning and disable it.
        int aggressiveCacheRefreshInterval = DatabaseDescriptor.getAuthAggressiveCachingUpdateInterval();
        if (aggressiveCacheRefreshInterval < 1000 && aggressiveCacheRefreshInterval != -1) // -1 is the unset default
        {
            logger.warn("Invalid configuration detected. auth_aggressive_cache_update_interval_in_ms is set below" +
                        "minimum threshold of 1000ms. Aggressive caching will be disabled and non-aggressive caching" +
                        "may be turned on instead");
            conf.auth_aggressive_caching_update_interval_in_ms = -1;
        }

        if (conf.capability_manager != null)
            capabilityManager = FBUtilities.newCapabilityManager(conf.capability_manager);

        if (capabilityManager.enforcesRestrictions() && !authenticator.requireAuthentication())
            throw new ConfigurationException(conf.authenticator + " can't be used with " + conf.capability_manager, false);

        DatabaseDescriptor.setCapabilityManager(capabilityManager);

        // internode authenticator

        IInternodeAuthenticator internodeAuthenticator;
        if (conf.internode_authenticator != null)
            internodeAuthenticator = FBUtilities.construct(conf.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        DatabaseDescriptor.setInternodeAuthenticator(internodeAuthenticator);

        // Validate at last to have authenticator, authorizer, role-manager, capability manager
        // and internode-auth setup in case these rely on each other.

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        capabilityManager.validateConfiguration();
        internodeAuthenticator.validateConfiguration();
    }
}
