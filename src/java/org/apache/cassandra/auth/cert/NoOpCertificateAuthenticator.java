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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * No op implementation of certificate based client authenticator.
 * Used as the default in DatabaseDescriptor and potentially overridden by
 * configuration values, the Requirement is hardcoded to ensure that cert
 * based auth is never attempted. If config declares that any other
 * Requirement be used with this class, a ConfigurationException will be
 * thrown at startup.
 */
public class NoOpCertificateAuthenticator implements ICertificateAuthenticator
{
    public void setRequirement(Requirement requirement)
    {
        if (requirement != Requirement.NOT_REQUIRED)
            throw new ConfigurationException(String.format("%s may only be used with requirement_level NOT_REQUIRED"));
    }

    public Requirement getRequirement()
    {
        return Requirement.NOT_REQUIRED;
    }

    public AuthenticatedUser authenticate(Certificate[] certificateChain) throws AuthenticationException
    {
        throw new UnsupportedOperationException();
    }
}
