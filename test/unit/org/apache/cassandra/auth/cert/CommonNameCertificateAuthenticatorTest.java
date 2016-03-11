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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.exceptions.AuthenticationException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommonNameCertificateAuthenticatorTest
{

    private ICertificateAuthenticator authenticator;
    private static KeyStore testCertificates;

    @BeforeClass
    public static void setUpClass() throws Exception {
        testCertificates = KeyStore.getInstance("JKS");
        ClassLoader resourceLoader = CommonNameCertificateAuthenticatorTest.class.getClassLoader();
        try (InputStream resource = resourceLoader.getResourceAsStream("auth/CommonNameTestCertificates.jks"))
        {
            testCertificates.load(resource, "actest".toCharArray());
        }
    }

    @Before
    public void setUp()
    {
        authenticator = new CommonNameCertificateAuthenticator();
    }

    @Test
    public void testGetAndSetRequirement()
    {
        // default value of NOT_REQUIRED - this is always overriden in DatabaseDescriptor.applyConfig
        assertEquals(ICertificateAuthenticator.Requirement.NOT_REQUIRED, authenticator.getRequirement());
        authenticator.setRequirement(ICertificateAuthenticator.Requirement.REQUIRED);
        assertEquals(ICertificateAuthenticator.Requirement.REQUIRED, authenticator.getRequirement());

        try
        {
            authenticator.setRequirement(null);
            fail();
        }
        catch(AssertionError e)
        {
            // expected
        }
    }

    @Test
    public void testSupportMethodsShouldDoNothing() throws Exception
    {
        assertTrue(authenticator.protectedResources().isEmpty());
        authenticator.validateConfiguration();
        authenticator.setup();
    }

    @Test
    public void testShouldUseCommonNameAsAuthenticatedUser() throws Exception
    {
        Certificate[] chain = testCertificates.getCertificateChain("someuser");
        assertEquals("CN=someuser", subject(chain));
        assertAuthenticatesAs("someuser", chain);
    }

    @Test
    public void testShouldIgnoreOtherFieldsInCertificateSubject() throws Exception
    {
        Certificate[] chain = testCertificates.getCertificateChain("bigsubject");
        assertEquals("CN=bigsubject,OU=BigTech Engineering,O=Big Technology Company,ST=MA,C=US",
                     subject(chain));
        assertAuthenticatesAs("bigsubject", chain);
    }

    @Test
    public void testShouldIgnoreOtherCertificatesInChain() throws Exception
    {
        Certificate[] storedChain = testCertificates.getCertificateChain("someuser");
        Certificate[] testChain = {storedChain[0], null, null, null, null};
        assertAuthenticatesAs("someuser", testChain);
    }

    @Test
    public void testShouldRejectEmptyChainsOrNonX509Certificates() throws Exception
    {
        assertAuthenticationFailure(new Certificate[0]);
        Certificate[] notX509 = {new DummyCertificate()};
        assertAuthenticationFailure(notX509);
    }

    @Test
    public void testShouldRejectCertificatesWithoutCommonNameField() throws Exception
    {
        Certificate[] chain = testCertificates.getCertificateChain("nocn");
        assertEquals("O=No Common Name", subject(chain));
        assertAuthenticationFailure(chain);
    }

    private String subject(Certificate[] chain)
    {
        return ((X509Certificate)chain[0]).getSubjectX500Principal().getName();
    }

    private void assertAuthenticatesAs(String username, Certificate[] chain) throws Exception
    {
        AuthenticatedUser user = authenticator.authenticate(chain);
        assertEquals(username, user.getName());
    }

    private void assertAuthenticationFailure(Certificate[] chain) throws Exception
    {
        try
        {
            authenticator.authenticate(chain);
            fail("Expected authentication failure.");
        }
        catch (AuthenticationException e) {}
    }

    private static final class DummyCertificate extends Certificate {

        public DummyCertificate()
        {
            super(null);
        }

        @Override
        public byte[] getEncoded()
        {
            return null;
        }

        @Override
        public PublicKey getPublicKey()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return null;
        }

        @Override
        public void verify(PublicKey key)
        {

        }

        @Override
        public void verify(PublicKey key, String sigProvider)
        {

        }

        @Override
        public Object writeReplace()
        {
            return null;
        }
    }
}
