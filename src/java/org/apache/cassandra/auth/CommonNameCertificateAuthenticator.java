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

import org.apache.cassandra.exceptions.AuthenticationException;

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

/**
 * CommonNameCertificateAuthenticator is an ICertificateAuthenticator
 * implementation that uses the common name (CN) field on certificates
 * presented by clients as their username.  This implementation does not
 * require authentication; if the client fails to present a certificate,
 * then it will be authenticated as AuthenticatedUser.ANONYMOUS_USER.
 *
 * This implementation only accepts {@link java.security.cert.X509Certificate}
 * chains.
 */
public class CommonNameCertificateAuthenticator implements ICertificateAuthenticator {

    @Override
    public boolean requireAuthentication()
    {
        return false;
    }

    @Override
    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    @Override
    public void validateConfiguration() {}

    @Override
    public void setup() {}

    @Override
    public AuthenticatedUser authenticate(Certificate[] chain) throws AuthenticationException
    {
        validateCertificateChain(chain);
        LdapName subject = extractSubjectFromChain(chain);
        return extractCommonNameFromSubject(subject);
    }

    private void validateCertificateChain(Certificate[] chain) throws AuthenticationException
    {
        if (chain.length == 0)
            throw new AuthenticationException("Client certificate required for authentication");
        if (!(chain[0] instanceof X509Certificate))
            throw new AuthenticationException("Only X.509 certificates are supported for authentication");
    }

    private LdapName extractSubjectFromChain(Certificate[] chain) throws AuthenticationException {
        X509Certificate certificate = (X509Certificate)chain[0];
        String rdnString = certificate.getSubjectX500Principal().getName();
        try
        {
            return new LdapName(rdnString);
        }
        catch (InvalidNameException e)
        {
            throw new AuthenticationException("Unable to parse certificate subject");
        }
    }

    private AuthenticatedUser extractCommonNameFromSubject(LdapName subject) throws AuthenticationException
    {
        for (Rdn r : subject.getRdns())
            if ("CN".equals(r.getType()))
                return new AuthenticatedUser(r.getValue().toString());
        throw new AuthenticationException("Common name field required but not present in certificate subject");
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        throw new UnsupportedOperationException();
    }
}
