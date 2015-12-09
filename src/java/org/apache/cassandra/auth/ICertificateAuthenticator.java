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

import java.security.cert.Certificate;

import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * Supports user authentication via the certificate chain presented by
 * the client when it connected to the native server.
 *
 * If this authenticator is in use, then the server must be configured
 * to use SSL unless the implementation does not require authentication.
 * If SSL is not used or clients connect that fail to present a
 * certificate, then they will be authenticated as
 * AuthenticatedUser.ANONYMOUS_USER.
 */
public interface ICertificateAuthenticator extends IAuthenticator {

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
     * @throws AuthenticationException
     */
    AuthenticatedUser authenticate(Certificate[] certificateChain) throws AuthenticationException;
}
