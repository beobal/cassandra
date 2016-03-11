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

package org.apache.cassandra.auth.cert;

import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Set;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Supports user authentication via the certificate chain presented by
 * the client when it connected to the native server.
 *
 * Should it be impossible to determine an authenticated user from the
 * presented certificate chain, the server decides the course of action
 * based on the configured Requirement. If REQUIRED, then the client
 * connection is rejected with an authentication error. If OPTIONAL,
 * then the server may proceed to attempting authentication via other
 * means, for instance using SASL for native protocol.
 *
 * If the stated Requirement is NOT_REQUIRED, then the server will
 * skip certificate based authentication altogether. This is useful
 * for unsecured/unencrypted servers and/or connections.
 */
public interface ICertificateAuthenticator
{
    /**
     * Convenience method to indicate whether the server will attempt
     * authentication via client certificates, given the configured
     * ICertificateAuthenticator instance
     */
    static boolean willAuthenticate()
    {
        return DatabaseDescriptor.getCertificateAuthenticator().getRequirement() != Requirement.NOT_REQUIRED;
    }

    /**
     * Convenience method to indicate whether certificate based client
     * authentication is mandatory given the system configuration
     */
    static boolean isRequired()
    {
        return DatabaseDescriptor.getCertificateAuthenticator().getRequirement() == Requirement.REQUIRED;
    }

    public enum Requirement
    {
        REQUIRED,
        OPTIONAL,
        NOT_REQUIRED
    }

    Requirement getRequirement();

    void setRequirement(Requirement requirement);

    /**
     * Evaluates the supplied certificate chain and returns the
     * AuthenticatedUser (if any) to which the chain corresponds.  If
     * the chain cannot be used to name an AuthenticatedUser, then
     * the implementation should throw an AuthenticationException.
     * Alternatively, if authentication is not required, it may also
     * return AuthenticatedUser.ANONYMOUS_USER.
     *
     * @param certificateChain the certificate chain presented by the client
     * @return non-null representation of the authenticated subject
     */
    AuthenticatedUser authenticate(Certificate[] certificateChain) throws AuthenticationException;

    default Set<? extends IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    default void validateConfiguration() throws ConfigurationException
    {
    }

    default void setup()
    {
    }
}
