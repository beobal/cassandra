package org.apache.cassandra.config;

public class JMXServerOptions
{
    //jmx server settings
    public boolean enabled = true; 
    public boolean remote = true;
    public int port = 7199;
    public int rmi_port = 0;
    public boolean authenticate = false;

    // ssl options
    public EncryptionOptions.JMXEncryptionOptions encryption_options;

    // location for credentials file if using JVM's file-based authentication
    public String password_file;

    // options for using Cassandra's own authentication mechanisms
    public String login_config_name;
    public String login_config_file;

    // location of standard access file, if using JVM's file-based access control
    public String access_file;

    // classname of authorizer if using a custom authz mechanism. Usually, this will
    // refer to o.a.c.auth.jmx.AuthorizationProxy which delegates to the IAuthorizer
    // configured in cassandra.yaml
    public String authorizer;
}
