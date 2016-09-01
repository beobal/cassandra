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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.rmi.*;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.remote.*;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLContext;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import javax.security.auth.Subject;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jmx.remote.internal.RMIExporter;
import com.sun.jmx.remote.security.JMXPluggableAuthenticator;
import org.apache.cassandra.auth.jmx.AuthenticationProxy;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.JMXServerOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.SSLFactory;
import sun.rmi.registry.RegistryImpl;
import sun.rmi.server.UnicastServerRef2;

public class JMXServerUtils
{
    private static final Logger logger = LoggerFactory.getLogger(JMXServerUtils.class);

    private static java.rmi.registry.Registry registry;

    /**
     * Creates a server programmatically. This allows us to set parameters which normally are
     * inaccessable. Authentication & authorization options are applied to both remote and
     * local-only servers, but SSL is only applicable for remote.
     */
    public static JMXConnectorServer createJMXServer(JMXServerOptions options)
    throws IOException
    {
        Map<String, Object> env = new HashMap<>();

        String urlTemplate = "service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$d/jmxrmi";
        InetAddress serverAddress = null;
        if (!options.remote)
        {
            serverAddress = InetAddress.getLoopbackAddress();
            System.setProperty("java.rmi.server.hostname", serverAddress.getHostAddress());
        }

        // Configure the RMI client & server socket factories, including SSL config.
        env.putAll(configureJmxSocketFactories(serverAddress, options));


        // Configure authn, using a JMXAuthenticator which either wraps a set log LoginModules configured
        // via a JAAS configuration entry, or one which delegates to the standard file based authenticator.
        // Authn is disabled if com.sun.management.jmxremote.authenticate=false
        env.putAll(configureJmxAuthentication(options));

        // Configure authz - if a custom proxy class is specified an instance will be returned.
        // If not, but a location for the standard access file is set in system properties, the
        // return value is null, and an entry is added to the env map detailing that location
        // If neither method is specified, no access control is applied
        MBeanServerForwarder authzProxy = configureJmxAuthorization(env, options);

        // Make sure we use our custom exporter so a full GC doesn't get scheduled every
        // sun.rmi.dgc.server.gcInterval millis (default is 3600000ms/1 hour)
        env.put(RMIExporter.EXPORTER_ATTRIBUTE, new Exporter());

        String url = String.format(urlTemplate, (serverAddress != null ? serverAddress.getHostAddress() : "0.0.0.0"), options.port);

        int rmiPort = options.remote ? options.rmi_port : 0;
        JMXConnectorServer jmxServer =
            JMXConnectorServerFactory.newJMXConnectorServer(new JMXServiceURL("rmi", null, rmiPort),
                                                            env,
                                                            ManagementFactory.getPlatformMBeanServer());

        // If a custom authz proxy was created, attach it to the server now.
        if (authzProxy != null)
            jmxServer.setMBeanServerForwarder(authzProxy);

        jmxServer.start();

        // use a custom Registry to avoid having to interact with it internally using the remoting interface
        configureRMIRegistry(options.port, env);

        logger.info("Configured JMX server at: {}", url);
        return jmxServer;
    }

    private static void configureRMIRegistry(int port, Map<String, Object> env) throws RemoteException
    {
        Exporter exporter = (Exporter)env.get(RMIExporter.EXPORTER_ATTRIBUTE);
        // If ssl is enabled, make sure it's also in place for the RMI registry
        // by using the SSL socket factories already created and stashed in env
        if (Boolean.getBoolean("com.sun.management.jmxremote.ssl"))
        {
            registry = new Registry(port,
                                   (RMIClientSocketFactory)env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE),
                                   (RMIServerSocketFactory)env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE),
                                   exporter.connectorServer);
        }
        else
        {
            registry = new Registry(port, exporter.connectorServer);
        }
    }

    private static Map<String, Object> configureJmxAuthentication(JMXServerOptions options)
    {
        Map<String, Object> env = new HashMap<>();
        if (!options.authenticate)
            return env;

        // If authentication is enabled, initialize the appropriate JMXAuthenticator
        // and stash it in the environment settings.
        // A JAAS configuration entry takes precedence. If one is supplied, use
        // Cassandra's own custom JMXAuthenticator implementation which delegates
        // auth to the LoginModules specified by the JAAS configuration entry.
        // If no JAAS entry is found, an instance of the JDK's own
        // JMXPluggableAuthenticator is created. In that case, the admin may have
        // set a location for the JMX password file which must be added to env
        // before creating the authenticator. If no password file has been
        // explicitly set, it's read from the default location
        // $JAVA_HOME/lib/management/jmxremote.password
        String configEntry = options.login_config_name;
        if (configEntry != null)
        {
            if (Strings.isNullOrEmpty(System.getProperty("java.security.auth.login.config")))
            {
                if (Strings.isNullOrEmpty(options.login_config_file))
                {
                    throw new ConfigurationException(String.format("Login config name %s specified for JMX auth, but no " +
                                                                   "configuration is available. Please set config " +
                                                                   "location in cassandra.yaml or with the " +
                                                                   "'java.security.auth.login.config' system property",
                                                                   configEntry));
                }
                else
                {
                    System.setProperty("java.security.auth.login.config", options.login_config_file);
                }
            }
            env.put(JMXConnectorServer.AUTHENTICATOR, new AuthenticationProxy(configEntry));
        }
        else
        {
            String passwordFile = options.password_file;
            if (passwordFile != null)
            {
                // stash the password file location where JMXPluggableAuthenticator expects it
                env.put("jmx.remote.x.password.file", passwordFile);
            }

            env.put(JMXConnectorServer.AUTHENTICATOR, new JMXPluggableAuthenticatorWrapper(env));
        }

        return env;
    }

    private static MBeanServerForwarder configureJmxAuthorization(Map<String, Object> env, JMXServerOptions options)
    {
        // If a custom authz proxy is supplied (Cassandra ships with AuthorizationProxy, which
        // delegates to its own role based IAuthorizer), then instantiate and return one which
        // can be set as the JMXConnectorServer's MBeanServerForwarder.
        // If no custom proxy is supplied, check system properties for the location of the
        // standard access file & stash it in env
        String authzProxyClass = options.authorizer;
        if (authzProxyClass != null)
        {
            final InvocationHandler handler = FBUtilities.construct(authzProxyClass, "JMX authz proxy");
            final Class[] interfaces = { MBeanServerForwarder.class };

            Object proxy = Proxy.newProxyInstance(MBeanServerForwarder.class.getClassLoader(), interfaces, handler);
            return MBeanServerForwarder.class.cast(proxy);
        }
        else
        {
            String accessFile = options.access_file;
            if (accessFile != null)
            {
                env.put("jmx.remote.x.access.file", accessFile);
            }
            return null;
        }
    }

    private static Map<String, Object> configureJmxSocketFactories(InetAddress serverAddress, JMXServerOptions options)
    throws IOException
    {
        Map<String, Object> env = new HashMap<>();
        if (!options.remote)
        {
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                    new RMIServerSocketFactoryImpl(serverAddress));
        }
        else
        {
            EncryptionOptions.JMXEncryptionOptions encOptions = options.encryption_options;
            if (encOptions.enabled)
            {
                SSLContext context = SSLFactory.createSSLContext(encOptions, encOptions.require_client_auth);

                String[] supported_protocol_versions = encOptions.supported_protocol_versions;
                if (supported_protocol_versions != null)
                    System.setProperty("javax.rmi.ssl.client.enabledProtocols", String.join(",", supported_protocol_versions));

                String[] ciphers = SSLFactory.filterCipherSuites(context.getServerSocketFactory().getSupportedCipherSuites(),
                                                                 encOptions.cipher_suites);
                System.setProperty("javax.rmi.ssl.client.enabledCipherSuites", String.join(",", ciphers));

                SslRMIClientSocketFactory clientFactory = new SslRMIClientSocketFactory();
                SslRMIServerSocketFactory serverFactory = new SslRMIServerSocketFactory(context,
                                                                                        ciphers,
                                                                                        supported_protocol_versions,
                                                                                        encOptions.require_client_auth);
                env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);
                env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
                env.put("com.sun.jndi.rmi.factory.socket", clientFactory);
                logJmxSslConfig(serverFactory);
            }
        }

        return env;
    }

    private static void logJmxSslConfig(SslRMIServerSocketFactory serverFactory)
    {
        logger.debug("JMX SSL configuration. { protocols: [{}], cipher_suites: [{}], require_client_auth: {} }",
                     serverFactory.getEnabledProtocols() == null
                     ? "'JVM defaults'"
                     : Arrays.stream(serverFactory.getEnabledProtocols()).collect(Collectors.joining("','", "'", "'")),
                     serverFactory.getEnabledCipherSuites() == null
                     ? "'JVM defaults'"
                     : Arrays.stream(serverFactory.getEnabledCipherSuites()).collect(Collectors.joining("','", "'", "'")),
                     serverFactory.getNeedClientAuth());
    }

    private static class JMXPluggableAuthenticatorWrapper implements JMXAuthenticator
    {
        final Map<?, ?> env;
        private JMXPluggableAuthenticatorWrapper(Map<?, ?> env)
        {
            this.env = ImmutableMap.copyOf(env);
        }

        public Subject authenticate(Object credentials)
        {
            JMXPluggableAuthenticator authenticator = new JMXPluggableAuthenticator(env);
            return authenticator.authenticate(credentials);
        }
    }

    /**
     * In the RMI subsystem, the ObjectTable instance holds references to remote
     * objects for distributed garbage collection purposes. When objects are
     * added to the ObjectTable (exported), a flag is passed to * indicate the
     * "permanence" of that object. Exporting as permanent has two effects; the
     * object is not eligible for distributed garbage collection, and its
     * existence will not prevent the JVM from exiting after termination of all
     * non-daemon threads terminate. Neither of these is bad for our case, as we
     * attach the server exactly once (i.e. at startup, not subsequently using
     * the Attach API) and don't disconnect it before shutdown. The primary
     * benefit we gain is that it doesn't trigger the scheduled full GC that
     * is otherwise incurred by programatically configuring the management server.
     *
     * To that end, we use this private implementation of RMIExporter to register
     * our JMXConnectorServer as a permanent object by adding it to the map of
     * environment variables under the key RMIExporter.EXPORTER_ATTRIBUTE
     * (com.sun.jmx.remote.rmi.exporter) prior to calling server.start()
     *
     * See also:
     *  * CASSANDRA-2967 for background
     *  * https://www.jclarity.com/2015/01/27/rmi-system-gc-unplugged/ for more detail
     *  * https://bugs.openjdk.java.net/browse/JDK-6760712 for info on setting the exporter
     *  * sun.management.remote.ConnectorBootstrap to trace how the inbuilt management agent
     *    sets up the JMXConnectorServer
     */
    private static class Exporter implements RMIExporter
    {
        // the first object to be exported by this instance is *always* the JMXConnectorServer
        // instance created by createJMXServer. Keep a handle to it, as it needs to be supplied
        // to our custom Registry too.
        private Remote connectorServer;

        public Remote exportObject(Remote obj, int port, RMIClientSocketFactory csf, RMIServerSocketFactory ssf)
        throws RemoteException
        {
            Remote remote = new UnicastServerRef2(port, csf, ssf).exportObject(obj, null, true);
            // Keep a reference to the first object exported, the JMXConnectorServer
            if (connectorServer == null)
                connectorServer = remote;

            return remote;
        }

        public boolean unexportObject(Remote obj, boolean force) throws NoSuchObjectException
        {
            return UnicastRemoteObject.unexportObject(obj, force);
        }
    }

    /**
     * Using this class avoids the necessity to interact with the registry via its
     * remoting interface. This is necessary because when SSL is enabled for the registry,
     * that remote interaction is treated just the same as one from an external client.
     * That is problematic when binding the JMXConnectorServer to the Registry as it requires
     * the client, which in this case is our own internal code, to connect like any other SSL
     * client, meaning we need a truststore containing our own certificate.
     * This bypasses the binding API completely, which emulates the behaviour of
     * ConnectorBootstrap when the subsystem is initialized by the JVM Agent directly.
     *
     * See CASSANDRA-12109.
     */
    private static class Registry extends RegistryImpl
    {
        private final static String KEY = "jmxrmi";
        private final Remote connectorServer;

        private Registry(int port, Remote connectorServer) throws RemoteException
        {
            super(port);
            this.connectorServer = connectorServer;
        }

        private Registry(int port,
                         RMIClientSocketFactory csf,
                         RMIServerSocketFactory ssf,
                         Remote connectorServer) throws RemoteException
        {
            super(port, csf, ssf);
            this.connectorServer = connectorServer;
        }

        public Remote lookup(String name) throws RemoteException, NotBoundException
        {
            if (name.equals(KEY))
                return connectorServer;

            throw new NotBoundException(String.format("Only the JMX Connector Server named %s " +
                                                      "is bound in this registry", KEY));
        }

        public void bind(String name, Remote obj) throws RemoteException, AlreadyBoundException
        {
            throw new UnsupportedOperationException("Unsupported");
        }

        public void unbind(String name) throws RemoteException, NotBoundException
        {
            throw new UnsupportedOperationException("Unsupported");
        }

        public void rebind(String name, Remote obj) throws RemoteException
        {
            throw new UnsupportedOperationException("Unsupported");
        }

        public String[] list() throws RemoteException
        {
            return new String[] {KEY};
        }
    }
}
