/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Ludovic Launer
 * Contributors: Robert Hodges
 */

package com.continuent.tungsten.common.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Helper class for security related topics
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelper
{
    private static final Logger logger = Logger.getLogger(SecurityHelper.class);

    /*
     * Defines the type of application requesting Security information. This
     * allows module specific configuration of security.
     */
    // TUC-1872
    public static enum TUNGSTEN_APPLICATION_NAME
    {
        CONNECTOR, REPLICATOR, REST_API, ANY;
    }

    /**
     * Save passwords from a TungstenProperties into a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void saveCredentialsFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        String passwordFileLocation = authenticationInfo
                .getPasswordFileLocation();

        try
        {
            String username = authenticationInfo.getUsername();
            String password = authenticationInfo.getPassword();

            PropertiesConfiguration props = new PropertiesConfiguration(
                    passwordFileLocation); // Use Apache commons-configuration:
                                           // preserves comments in .properties
                                           // !
            props.setProperty(username, password);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
            logger.error("Error while saving properties for file:"
                    + authenticationInfo.getPasswordFileLocation(), ce);
            throw new ServerRuntimeException("Error while saving Credentials: "
                    + ce.getMessage());
        }
    }

    /**
     * Delete a user and password from a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void deleteUserFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        String username = authenticationInfo.getUsername();
        String passwordFileLocation = authenticationInfo
                .getPasswordFileLocation();

        try
        {
            PropertiesConfiguration props = new PropertiesConfiguration(
                    passwordFileLocation);

            // --- Check that the user exists ---
            String usernameInFile = props.getString(username);
            if (usernameInFile == null)
            {
                throw new ServerRuntimeException(MessageFormat.format(
                        "Username does not exist: {0}", username));
            }

            props.clearProperty(username);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
            logger.error("Error while saving properties for file:"
                    + authenticationInfo.getPasswordFileLocation(), ce);
            throw new ServerRuntimeException("Error while saving Credentials: "
                    + ce.getMessage());
        }
    }

    /**
     * Loads passwords from a TungstenProperties from a .properties file
     * 
     * @return TungstenProperties containing logins as key and passwords as
     *         values
     */
    public static TungstenProperties loadPasswordsFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        try
        {
            String passwordFileLocation = authenticationInfo
                    .getPasswordFileLocation();
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(passwordFileLocation), false);
            newProps.trim();
            logger.debug(MessageFormat.format("Passwords loaded from: {0}",
                    passwordFileLocation));
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            throw new ServerRuntimeException("Unable to find properties file: "
                    + authenticationInfo.getPasswordFileLocation(), e);

        }
        catch (IOException e)
        {
            throw new ServerRuntimeException("Unable to read properties file: "
                    + authenticationInfo.getPasswordFileLocation(), e);
        }
    }

    /**
     * Loads Authentication and Encryption parameters from default location for
     * service.properties file
     * 
     * @return AuthenticationInfo loaded from file
     * @throws ConfigurationException
     */
    public static AuthenticationInfo loadAuthenticationInformation()
            throws ConfigurationException
    {
        return loadAuthenticationInformation((String) null);
    }

    public static AuthenticationInfo loadAuthenticationInformation(
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
            throws ConfigurationException
    {
        return loadAuthenticationInformation(null, true,
                tungstenApplicationName);
    }

    /**
     * Loads Authentication and Encryption parameters from service.properties
     * file
     * 
     * @param propertiesFileLocation Location of the security.properties file.
     *            If set to null, will try to locate default file.
     * @return AuthenticationInfo
     * @throws ConfigurationException
     * @throws ReplicatorException
     */
    public static AuthenticationInfo loadAuthenticationInformation(
            String propertiesFileLocation) throws ConfigurationException
    {
        return loadAuthenticationInformation(propertiesFileLocation, true,
                TUNGSTEN_APPLICATION_NAME.ANY);
    }

    public static AuthenticationInfo loadAuthenticationInformation(
            String propertiesFileLocation, boolean doConsistencyChecks,
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
            throws ConfigurationException
    {
        // Load properties and perform substitution
        TungstenProperties securityProperties = null;
        try
        {
            securityProperties = loadSecurityPropertiesFromFile(propertiesFileLocation);
        }
        catch (ConfigurationException ce)
        {
            if (doConsistencyChecks)
                throw ce;
        }

        AuthenticationInfo authInfo = new AuthenticationInfo(
                propertiesFileLocation);

        // Authorisation and/or encryption
        if (securityProperties != null)
        {
            securityProperties.trim(); // Remove white spaces

            // --- Get common values ---
            List<String> enabledProtocols = securityProperties
                    .getStringList(SecurityConf.SECURITY_ENABLED_TRANSPORT_PROTOCOL);
            List<String> enabledCipherSuites = securityProperties
                    .getStringList(SecurityConf.SECURITY_ENABLED_CIPHER_SUITES);

            boolean useTungstenAuthenticationRealm = securityProperties
                    .getBoolean(
                            SecurityConf.SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM,
                            SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT,
                            false);
            boolean useEncryptedPassword = securityProperties
                    .getBoolean(
                            SecurityConf.SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD,
                            SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT,
                            false);

            // Define application specific settings
            // Use default values by default
            String security_keystore_location = SecurityConf.SECURITY_KEYSTORE_LOCATION;
            String security_keystore_password = SecurityConf.SECURITY_KEYSTORE_PASSWORD;
            String security_truststore_location = SecurityConf.SECURITY_TRUSTSTORE_LOCATION;
            String security_truststore_password = SecurityConf.SECURITY_TRUSTSTORE_PASSWORD;
            // Use application specific settings if needed
            switch (tungstenApplicationName)
            {
                case CONNECTOR :
                    security_keystore_location = SecurityConf.CONNECTOR_SECURITY_KEYSTORE_LOCATION;
                    security_keystore_password = SecurityConf.CONNECTOR_SECURITY_KEYSTORE_PASSWORD;
                    security_truststore_location = SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_LOCATION;
                    security_truststore_password = SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_PASSWORD;
                    break;

                case REST_API :
                    security_use_encryption = SecurityConf.HTTP_REST_API_SSL_USESSL;
                    security_use_encryption_default = SecurityConf.HTTP_REST_API_SSL_USESSL_DEFAULT;

                    security_use_authentication = SecurityConf.HTTP_REST_API_AUTHENTICATION;
                    security_use_authentication_default = SecurityConf.HTTP_REST_API_AUTHENTICATION_DEFAULT;

                    security_authentication_use_encrypted_password = SecurityConf.HTTP_REST_API_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD;
                    security_authentication_use_encrypted_password_default = SecurityConf.HTTP_REST_API_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT;

                    security_keystore_location = SecurityConf.HTTP_REST_API_KEYSTORE_LOCATION;
                    security_keystore_password = SecurityConf.HTTP_REST_API_KEYSTORE_PASSWORD;
                    security_truststore_location = SecurityConf.HTTP_REST_API_TRUSTSTORE_LOCATION;
                    security_truststore_password = SecurityConf.HTTP_REST_API_TRUSTSTORE_PASSWORD;
                    break;
                default :
                    // Keep default values
                    break;
            }

            // --- Retrieve properties ---
            boolean connectorUseSSL = securityProperties.getBoolean(
                    SecurityConf.CONNECTOR_USE_SSL, "false", false);

            boolean useEncryption = securityProperties.getBoolean(
                    security_use_encryption, security_use_encryption_default,
                    false);

            boolean useAuthentication = securityProperties.getBoolean(
                    security_use_authentication,
                    security_use_authentication_default, false);

            boolean authenticationByCertificateNeeded = securityProperties
                    .getBoolean(
                            SecurityConf.HTTP_REST_API_AUTHENTICATION_USE_CERTIFICATE,
                            SecurityConf.HTTP_REST_API_AUTHENTICATION_USE_CERTIFICATE_DEFAULT,
                            false);

            boolean useEncryptedPassword = securityProperties.getBoolean(
                    security_authentication_use_encrypted_password,
                    security_authentication_use_encrypted_password_default,
                    false);

            String parentFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION);
            String passwordFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_PASSWORD_FILE_LOCATION);
            String accessFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_ACCESS_FILE_LOCATION);
            String keystoreLocation = securityProperties
                    .getString(security_keystore_location);
            keystoreLocation = (keystoreLocation != null && StringUtils
                    .isNotBlank(keystoreLocation)) ? keystoreLocation : null;
            String keystorePassword = securityProperties
                    .getString(security_keystore_password);
            String truststoreLocation = securityProperties
                    .getString(security_truststore_location);
            truststoreLocation = (truststoreLocation != null && StringUtils
                    .isNotBlank(truststoreLocation))
                    ? truststoreLocation
                    : null;
            String truststorePassword = securityProperties
                    .getString(security_truststore_password);
            String clientKeystoreLocation = securityProperties
                    .getString(SecurityConf.HTTP_REST_API_CLIENT_KEYSTORE_LOCATION);
            clientKeystoreLocation = (clientKeystoreLocation != null && StringUtils
                    .isNotBlank(clientKeystoreLocation))
                    ? clientKeystoreLocation
                    : null;
            String clientKeystorePassword = securityProperties
                    .getString(SecurityConf.HTTP_REST_API_CLIENT_KEYSTORE_PASSWORD);
            clientKeystorePassword = (clientKeystorePassword != null && StringUtils
                    .isNotBlank(clientKeystorePassword))
                    ? clientKeystorePassword
                    : null;

            String userName = securityProperties.getString(
                    SecurityConf.SECURITY_JMX_USERNAME, null, false);
            // Aliases for keystore
            String connector_alias_client_to_connector = securityProperties
                    .getString(
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR_DEFAULT,
                            false);
            String connector_alias_connector_to_db = securityProperties
                    .getString(
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB_DEFAULT,
                            false);
            String replicator_alias_master_to_slave = securityProperties
                    .getString(
                            SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE,
                            SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE_DEFAULT,
                            false);

            // --- Populate return object ---
            authInfo.setConnectorUseSSL(connectorUseSSL);
            authInfo.setParentPropertiesFileLocation(parentFileLocation);
            authInfo.setAuthenticationNeeded(useAuthentication);
            authInfo.setAuthenticationByCertificateNeeded(authenticationByCertificateNeeded);
            authInfo.setUseTungstenAuthenticationRealm(useTungstenAuthenticationRealm);
            authInfo.setUseEncryptedPasswords(useEncryptedPassword);
            authInfo.setEncryptionNeeded(useEncryption);
            authInfo.setPasswordFileLocation(passwordFileLocation);
            authInfo.setAccessFileLocation(accessFileLocation);
            authInfo.setKeystoreLocation(keystoreLocation);
            authInfo.setKeystorePassword(keystorePassword);
            authInfo.setTruststoreLocation(truststoreLocation);
            authInfo.setTruststorePassword(truststorePassword);
            authInfo.setUsername(userName);
            authInfo.setParentProperties(securityProperties);
            // aliases
            if (connector_alias_client_to_connector != null)
                authInfo.getMapKeystoreAliasesForTungstenApplication()
                        .put(SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                                connector_alias_client_to_connector);
            if (connector_alias_connector_to_db != null)
                authInfo.getMapKeystoreAliasesForTungstenApplication().put(
                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                        connector_alias_connector_to_db);
            if (replicator_alias_master_to_slave != null)
                authInfo.getMapKeystoreAliasesForTungstenApplication().put(
                        SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE,
                        replicator_alias_master_to_slave);

            // --- Check information is correct ---
            // Checks authentication and encryption parameters
            // file exists, aliases exists...
            if (doConsistencyChecks)
                authInfo.checkAndCleanAuthenticationInfo(tungstenApplicationName);

            // --- Set critical properties as System Properties ---
            SecurityHelper.setSecurityProperties(authInfo, false);
        }
        return authInfo;
    }

    /**
     * Set system properties required for SSL and password management. Since
     * these settings are critical to correct operation we optionally log them.
     * 
     * @param authInfo Populated authenticatino information
     * @param verbose If true, log information
     */
    private static void setSecurityProperties(AuthenticationInfo authInfo,
            boolean verbose)
    {
        if (verbose)
        {
            CLUtils.println("Setting security properties!");
        }
        setSystemProperty("javax.net.ssl.keyStore",
                authInfo.getKeystoreLocation(), verbose);
        setSystemProperty("javax.net.ssl.keyStorePassword",
                authInfo.getKeystorePassword(), verbose);
        setSystemProperty("javax.net.ssl.trustStore",
                authInfo.getTruststoreLocation(), verbose);
        setSystemProperty("javax.net.ssl.trustStorePassword",
                authInfo.getTruststorePassword(), verbose);

        // Protocols and Cipher Suites
        if (!authInfo.getEnabledProtocols().isEmpty())
        {
            String enabledProtocols = StringUtils.join(
                    authInfo.getEnabledProtocols(), ",");
            setSystemProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS,
                    enabledProtocols, verbose);
            setSystemProperty("https.protocols",
                    enabledProtocols, verbose);
        }
        
        if (!authInfo.getEnabledCipherSuites().isEmpty())
        {
            String[] supportedCipherSuites = ((SSLSocketFactory) SSLSocketFactory
                    .getDefault()).getSupportedCipherSuites();
            String[] enabledCipherSuites = authInfo.getEnabledCipherSuites()
                    .toArray(new String[0]);
            String[] possibleCipherSuites = SecurityHelper.getMatchingStrings(
                    supportedCipherSuites, enabledCipherSuites);
            if (possibleCipherSuites.length == 0)
            {
                // We don't have any cipher suites in common. This is not good!
                String message = "Unable to find approved ciphers in the supported cipher suites on this JVM";
                StringBuffer sb = new StringBuffer(message).append("\n");
                sb.append(String.format("JVM supported cipher suites: %s\n",
                        StringUtils.join(supportedCipherSuites)));
                sb.append(String
                        .format("Approved cipher suites from security.properties: %s\n",
                                StringUtils.join(enabledCipherSuites)));
                logger.error(sb.toString());
                throw new RuntimeException(message);
            }
            String enabledCipherSuitesList = StringUtils.join(
                    possibleCipherSuites, ",");
            setSystemProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS,
                    enabledCipherSuitesList, verbose);
        }
    }

    /**
     * Set system properties required for client SSL support and authentication.
     * The keyStore holds the client's private key to be used for authentication
     * The trustStore holds the certificates of the trusted servers
     * 
     * @param authInfo
     * @param verbose
     */
    public static void setClientSecurityProperties(AuthenticationInfo authInfo,
            boolean debug)
    {
        if (debug)
        {
            CLUtils.println("Clearing and Setting Client security properties");
        }

        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.trustStore");

        setSystemProperty("javax.net.ssl.keyStore",
                authInfo.getClientKeystoreLocation(), debug);
        setSystemProperty("javax.net.ssl.keyStorePassword",
                authInfo.getClientKeystorePassword(), debug);
        setSystemProperty("javax.net.ssl.trustStore",
                authInfo.getTruststoreLocation(), debug);
        setSystemProperty("javax.net.ssl.trustStorePassword",
                authInfo.getTruststorePassword(), debug);
    }

    /**
     * Sets a system property with a log message. Java -Dxxx system property
     * 
     * @param name the name of the system property to set
     * @param value value of the system property
     * @param verbose log the property being set if true.
     */
    private static void setSystemProperty(String name, String value,
            boolean verbose)
    {
        if (verbose)
        {
            CLUtils.println("Setting system property: name=" + name + " value="
                    + value);
        }
        if (value != null)
            System.setProperty(name, value);
    }

    /**
     * Loads Security related properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @param propertiesFileLocation location of the security.properties file.
     *            If set to null will look for the default file.
     * @return TungstenProperties containing security parameters
     * @throws ConfigurationException
     */
    private static TungstenProperties loadSecurityPropertiesFromFile(
            String propertiesFileLocation) throws ConfigurationException
    {
        TungstenProperties securityProps = null;
        FileInputStream securityConfigurationFileInputStream = null;

        // --- Get Configuration file ---
        if (propertiesFileLocation == null
                && ClusterConfiguration.getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File securityPropertiesFile;
        if (propertiesFileLocation == null) // Get from default location
        {
            File clusterConfDirectory = ClusterConfiguration
                    .getDir(ClusterConfiguration
                            .getGlobalConfigDirName(ClusterConfiguration
                                    .getClusterHome()));
            securityPropertiesFile = new File(clusterConfDirectory.getPath(),
                    SecurityConf.SECURITY_PROPERTIES_FILE_NAME);
        }
        else
        // Get from supplied location
        {
            securityPropertiesFile = new File(propertiesFileLocation);
        }

        // --- Get properties ---
        try
        {
            securityProps = new TungstenProperties();
            securityConfigurationFileInputStream = new FileInputStream(
                    securityPropertiesFile);
            securityProps.load(securityConfigurationFileInputStream, true);
            closeSecurityConfigurationFileInputStream(securityConfigurationFileInputStream);
        }
        catch (FileNotFoundException e)
        {
            String msg = MessageFormat.format(
                    "Cannot find configuration file: {0}",
                    securityPropertiesFile.getPath());
            logger.debug(msg, e);
            throw new ConfigurationException(msg);
        }
        catch (IOException e)
        {
            String msg = MessageFormat.format(
                    "Cannot load configuration file: {0}.\n Reason: {1}",
                    securityPropertiesFile.getPath(), e.getMessage());
            logger.debug(msg, e);
            throw new ConfigurationException(msg);
        }
        finally
        {
            closeSecurityConfigurationFileInputStream(securityConfigurationFileInputStream);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug(MessageFormat.format(": {0}",
                    securityPropertiesFile.getPath()));
        }

        // Update propertiesFileLocation with the location actualy used
        securityProps.put(
                SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION,
                securityPropertiesFile.getAbsolutePath());

        return securityProps;
    }

    /**
     * Close the security.properties input stream once it's been used. Best
     * effort
     * 
     * @param fis
     */
    private static void closeSecurityConfigurationFileInputStream(
            FileInputStream fis)
    {
        // TUC-2065 Close input stream once it's used
        if (fis != null)
        {
            try
            {
                fis.close();
            }
            catch (Exception ignoreMe)
            {
            }
        }
    }

    /**
     * Read client's SSL protocols
     * 
     * @return first value on the list or null if property doesn't have value.
     */
    public static String getProtocol()
    {
        if (System.getProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS) != null)
            return System.getProperty(
                    SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS).split(",")[0];
        else
            return null;
    }

    public static String[] getProtocols()
    {
        if (System.getProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS) != null)
            return System.getProperty(
                    SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS).split(",");
        else
            return null;
    }

    /**
     * Read client's SSL ciphers
     * 
     * @return first value on the list or null if property doesn't have value.
     */
    public static String getCipher()
    {
        if (System.getProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS) != null)
            return System.getProperty(
                    SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS).split(",")[0];
        else
            return null;
    }

    /** Get client encryption ciphers. */
    public static String[] getCiphers()
    {
        if (System.getProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS) != null)
            return System.getProperty(
                    SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS).split(",");
        else
            return null;
    }

    /** Return the ciphers available on this JVM. */
    public static String[] getJvmSupportedCiphers()
    {
        String[] supportedCiphers = ((SSLSocketFactory) SSLSocketFactory
                .getDefault()).getSupportedCipherSuites();
        return supportedCiphers;
    }

    /**
     * Process a list of candidate ciphers for use on the JVM and return those
     * candidates that are supported and hence usable.
     */
    public static String[] getJvmEnabledCiphers(String[] candidateCiphers)
    {
        String[] jvmEnabledCiphers = getMatchingStrings(
                getJvmSupportedCiphers(), candidateCiphers);
        return jvmEnabledCiphers;
    }

    /**
     * Get the system keystore location
     * 
     * @return The keyStore location
     */
    public static String getKeyStoreLocation()
    {
        return System.getProperty("javax.net.ssl.keyStore");
    }

    /**
     * Get the system truststore location
     * 
     * @return
     */
    public static String getTrustStoreLocation()
    {
        return System.getProperty("javax.net.ssl.trustStore");
    }

    /**
     * Get alias in a keystore or truststore
     * 
     * @param keystoreLocation
     * @param keystorePassword
     * @return
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws IOException
     */
    public static List<String> getAliasesforKeystore(String keystoreLocation,
            String keystorePassword) throws KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException
    {
        Enumeration<String> enumAliases = null;

        FileInputStream inputstreamStore;

        inputstreamStore = new FileInputStream(keystoreLocation);
        KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        keystore.load(inputstreamStore, keystorePassword.toCharArray());

        // List the aliases
        enumAliases = keystore.aliases();

        List<String> listAliases = Collections.list(enumAliases);

        // while (enumAliases.hasMoreElements())
        // {
        // String alias = enumAliases.nextElement();
        //
        // // Does alias refer to a private key?
        // // boolean b = keystore.isKeyEntry(alias);
        //
        // // Does alias refer to a trusted certificate?
        // // b = keystore.isCertificateEntry(alias);
        // }

        return listAliases;
    }

    /**
     * Compare two String arrays and store matching Strings to result
     * array.
     * 
     * @param str1
     * @param str2
     * @return String array consisting of all common Strings in str1 and str2
     * or null if the set is empty.
     */
    public static void checkAccessAndAliasesForKeystore(String storeLocation,
            String storePassword, boolean shouldNotBeEmpty)
            throws ConfigurationException
    {
        final String errorMessage = MessageFormat.format(
                "Could not access or retrieve aliases from {0}", storeLocation);
        try
        {
            List<String> aliasesInKeystore = SecurityHelper
                    .getAliasesforKeystore(storeLocation, storePassword);

            if (aliasesInKeystore.isEmpty() && shouldNotBeEmpty)
            {
                throw new ConfigurationException(
                        MessageFormat
                                .format("Keystore / Truststore does not contain any aliases: {0}",
                                        storeLocation));
            }
        }
        else
        {
            throw new ConfigurationException(MessageFormat.format(errorMessage,
                    e));
        }
        for (int li=0; li<longerStr.length; li++)
        {
            throw new ConfigurationException(MessageFormat.format(errorMessage,
                    e));
        }
        catch (CertificateException e)
        {
            throw new ConfigurationException(MessageFormat.format(errorMessage,
                    e));
        }
        catch (IOException e)
        {
            throw new ConfigurationException(MessageFormat.format(errorMessage,
                    e));
        }
    }

    /**
     * Stores the certificate in the host certificate truststore with the
     * associated alias. If the certificate is already added, this operation
     * does nothing. Any other certificate associated with the alias is
     * replaced.
     * 
     * @param certificateAlias Alias of the certificate in the truststore
     * @param certificateLocation Where to get the certificate from
     */
    public static void addCertificateToTruststore(String certificateAlias,
            String certificateLocation)
    {

    }

    /**
     * Compare two String arrays and store matching Strings to result array.
     * 
     * @param str1
     * @param str2
     * @return String array consisting of all common Strings in str1 and str2.
     */
    public static String[] getMatchingStrings(String[] str1, String[] str2)
    {
        // Put first string in table and then iterate across it using the
        // second.
        HashMap<String, String> map = new HashMap<String, String>();
        for (String s : str1)
        {
            map.put(s, "FOUND");
        }

        ArrayList<String> resultList = new ArrayList<String>();
        for (String s : str2)
        {
            if (map.get(s) != null)
                resultList.add(s);
        }

        return resultList.toArray(new String[0]);
    }

    public static void setCiphersAndProtocolsToSSLSocket(SSLSocket sslSocket,
            String[] enabledCiphers, String[] enabledProtocols)
            throws ConfigurationException
    {
        if (enabledCiphers != null && enabledCiphers.length > 0)
        {
            if (SecurityHelper.getMatchingStrings(
                    sslSocket.getSupportedCipherSuites(), enabledCiphers).length == 0)
            {
                throw new ConfigurationException("SSLSocket doesn't support any "
                        + "of the enabled (configured) cipher suites.");
            }

            // Enable ciphers which are both supported by socket service and
            // configured by user
            sslSocket.setEnabledCipherSuites(SecurityHelper.getMatchingStrings(
                    sslSocket.getSupportedCipherSuites(), enabledCiphers));            
        }
               
        if (enabledProtocols != null && enabledProtocols.length > 0)
        {
            if (SecurityHelper.getMatchingStrings(
                    sslSocket.getSupportedProtocols(), enabledProtocols).length == 0)
            {
                throw new ConfigurationException("SSLSocket doesn't support any "
                        + "of the enabled (configured) protocols.");
            }
            // Enable protocols which are both supported by socket and
            // configured by user.
            sslSocket.setEnabledProtocols(SecurityHelper.getMatchingStrings(
                    sslSocket.getSupportedProtocols(), enabledProtocols));
        }
    }
}
