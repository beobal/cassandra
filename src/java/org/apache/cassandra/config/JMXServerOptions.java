package org.apache.cassandra.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMXServerOptions {
    private static final Logger logger = LoggerFactory.getLogger(JMXServerOptions.class);

    private static final String JMX_REMOTE_PORT = "cassandra.jmx.remote.port";
    private static final String JMX_LOCAL_PORT = "cassandra.jmx.local.port";
    private static final String JMXREMOTE_AUTHENTICATE = "com.sun.management.jmxremote.authenticate";
    private static final String JMXREMOTE_SSL = "com.sun.management.jmxremote.ssl";
    private static final String JMXREMOTE_SSL_NEED_CLIENT_AUTH = "com.sun.management.jmxremote.ssl.need.client.auth";
    private static final String JMXREMOTE_REGISTRY_SSL = "com.sun.management.jmxremote.registry.ssl";
    private static final String JMXREMOTE_SSL_ENABLED_PROTOCOLS = "com.sun.management.jmxremote.ssl.enabled.protocols";
    private static final String JMXREMOTE_SSL_ENABLED_CIPHER_SUITES = "com.sun.management.jmxremote.ssl.enabled.cipher.suites";
    private static final String JMXREMOTE_PASSWORD_FILE = "com.sun.management.jmxremote.password.file";
    private static final String JMXREMOTE_ACCESS_FILE = "com.sun.management.jmxremote.access.file";
    private static final String JMX_REMOTE_LOGIN_CONFIG = "cassandra.jmx.remote.login.config";
    private static final String JMX_AUTHORIZER = "cassandra.jmx.authorizer";

    //jxm server settings
    public boolean enabled = true; 
    public boolean remote = true;
    public int port = 7199;
    public boolean authenticate = false;

    // jmx server ssl options
    public boolean ssl_enabled = false;
    public boolean ssl_need_client_auth=true;
    public boolean registry_ssl=true;
    public String[] ssl_protocols;
    public String[] ssl_cipher_suites;

    // these options does not work currently
    public String keystore;
    public String keystore_password;
    public String truststore;
    public String truststore_password;

    public String password_file;
    public String access_file;

    public String login_config;
    public String java_security_auth_login_config;

    public String authorizer;

    public void maybeOverwriteSettingsFromSystemProperty()
    {    	    	
        // If "cassandra.jmx.remote.port" or "cassandra.jmx.local.port" are set in the system property,
        // JMX options are assumed to be set by cassandra-env.(sh|ps1). In such case, 
        // settings are overwritten by settings in the system property.
        // Otherwise, options set in cassandra.yaml will be used.
        // javax.net.ssl.keyStore, javax.net.ssl.keyStorePassword, javax.net.ssl.trustStore
        // javax.net.ssl.trustStorePassword and java.security.auth.login.config are not overwritten here,
        // because these options are finally set to system property.
        if (System.getProperty(JMX_REMOTE_PORT) == null
                && System.getProperty(JMX_LOCAL_PORT) == null)
        {
            return;    		
        }

        logger.warn("Overwrite JMX server options from system property. " +
                "JMX options in cassadnra-env.(sh|ps1) should moved to cassandra.yaml.");

        if (System.getProperty(JMX_REMOTE_PORT) != null)
        {
            this.port = Integer.parseInt(System.getProperty(JMX_REMOTE_PORT));
            this.remote = true;
        } else {
            this.port = Integer.parseInt(System.getProperty(JMX_LOCAL_PORT));
            this.remote = false;
        }

        this.authenticate = Boolean.parseBoolean(
                System.getProperty(JMXREMOTE_AUTHENTICATE, "true"));

        this.ssl_enabled = Boolean.parseBoolean(
                System.getProperty(JMXREMOTE_SSL, "false"));

        this.ssl_need_client_auth = Boolean.parseBoolean(
                System.getProperty(JMXREMOTE_SSL_NEED_CLIENT_AUTH, "true"));

        this.registry_ssl = Boolean.parseBoolean(
                System.getProperty(JMXREMOTE_REGISTRY_SSL, "true"));

        String ssl_protocols = System.getProperty(JMXREMOTE_SSL_ENABLED_PROTOCOLS);
        if (ssl_protocols != null)
        {
            this.ssl_protocols = StringUtils.split(ssl_protocols, ",");
        }

        String ssl_cipher_suites = System.getProperty(JMXREMOTE_SSL_ENABLED_CIPHER_SUITES);
        if (ssl_cipher_suites != null)
        {
            this.ssl_cipher_suites = StringUtils.split(ssl_cipher_suites, ",");
        }

        this.password_file = System.getProperty(JMXREMOTE_PASSWORD_FILE);
        this.access_file = System.getProperty(JMXREMOTE_ACCESS_FILE);

        this.login_config = System.getProperty(JMX_REMOTE_LOGIN_CONFIG);

        this.authorizer = System.getProperty(JMX_AUTHORIZER);
    }
}
