/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Ludovic Launer
 */

package com.continuent.tungsten.common.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityConf.KEYSTORE_TYPE;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;
import com.continuent.tungsten.common.utils.CLLogLevel;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Information class holding Authentication and Encryption parameters Some of
 * the properties may be left null depending on how and when this is used
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
@XmlRootElement
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public final class AuthenticationInfo implements Cloneable
{
    private static final Logger          logger                                   = Logger
            .getLogger(AuthenticationInfo.class);
    /** Location of the file from which this was built **/
    private String                       parentPropertiesFileLocation             = null;
    /** Properties from the files from which this was built **/
    private TungstenProperties           parentProperties                         = null;
    private TUNGSTEN_APPLICATION_NAME    tungstenApplicationName                  = null;

    private boolean                      authenticationNeeded                     = false;
    private Integer                      minWaitOnFailedLogin                     = null;                         // (ms)
    private Integer                      maxWaitOnFailedLogin                     = null;                         // (ms)
    private Integer                      incrementStepWaitOnFailedLogin           = 1;

    private boolean                      authenticationByCertificateNeeded        = false;
    private boolean                      encryptionNeeded                         = false;
    private boolean                      useTungstenAuthenticationRealm           = true;
    private boolean                      useEncryptedPasswords                    = false;
    /** Set to true if the connector should be using SSL **/
    private boolean                      connectorUseSSL                          = false;

    // Authentication parameters
    private String                       username                                 = null;
    private String                       password                                 = null;
    private String                       passwordFileLocation                     = null;
    private String                       accessFileLocation                       = null;
    // Encryption parameters
    private String                       keystoreLocation                         = null;
    private String                       keystorePassword                         = null;
    private String                       clientKeystoreLocation                   = null;
    private String                       clientKeystorePassword                   = null;
    private String                       truststoreLocation                       = null;
    private String                       truststorePassword                       = null;
    private List<String>                 enabledProtocols                         = null;
    private List<String>                 enabledCipherSuites                      = null;

    // Alias for entries in keystore
    // key=identifier as defined in SecurityConf value=alias for this
    // application
    private HashMap<String, String>      mapKeystoreAliasesForTungstenApplication = new HashMap<String, String>();

    public transient final static String SECURITY_INFO_PROPERTY                   = "securityInfo";
    public transient final static String TUNGSTEN_AUTHENTICATION_REALM            = "tungstenAutenthicationRealm";
    // Possible command line parameters
    public transient final static String USERNAME                                 = "-username";
    public transient final static String PASSWORD                                 = "-password";
    public transient final static String KEYSTORE_LOCATION                        = "-keystoreLocation";
    public transient final static String KEYSTORE_PASSWORD                        = "-keystorePassword";
    public transient final static String TRUSTSTORE_LOCATION                      = "-truststoreLocation";
    public transient final static String TRUSTSTORE_PASSWORD                      = "-truststorePassword";
    public transient final static String SECURITY_CONFIG_FILE_LOCATION            = "-securityProperties";

    /**
     * Creates a new <code>AuthenticationInfo</code> object
     */
    public AuthenticationInfo(String parentPropertiesFileLocation)
    {
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }

    public AuthenticationInfo()
    {
        this((String) null);
    }

    /**
     * Check Authentication information consistency
     * 
     * @throws ConfigurationException
     */
    public void checkAndCleanAuthenticationInfo()
            throws ServerRuntimeException, ConfigurationException
    {
        checkAndCleanAuthenticationInfo(TUNGSTEN_APPLICATION_NAME.ANY);
    }

    public void checkAndCleanAuthenticationInfo(
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
                    throws ServerRuntimeException, ConfigurationException
    {
        // --- Check security.properties location ---
        if (this.parentPropertiesFileLocation != null)
        {
            File f = new File(this.parentPropertiesFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.parentPropertiesFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SECURITY_CONFIG_FILE_LOCATION,
                        this.parentPropertiesFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
        }
        // --- Clean up ---
        if (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                && !this.isConnectorUseSSL())
        {
            // The Connector does not use SSL, delete unnecessary information.
            this.keystoreLocation = null;
            this.keystorePassword = null;
            this.truststoreLocation = null;
            this.truststorePassword = null;
        }

        // ---------------------- Check Keystore ----------------------------
        String keystoreLocationProperty = (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR)
                ? SecurityConf.CONNECTOR_SECURITY_KEYSTORE_LOCATION
                : SecurityConf.SECURITY_KEYSTORE_LOCATION;
        String keystorePasswordProperty = (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR)
                ? SecurityConf.CONNECTOR_SECURITY_KEYSTORE_PASSWORD
                : SecurityConf.SECURITY_KEYSTORE_PASSWORD;

        if ((this.isEncryptionNeeded() && this.keystoreLocation != null)
                || ((tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                        || tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REPLICATOR)
                        && this.isConnectorUseSSL()))
        {
            // --- Check file location is specified ---
            if (this.keystoreLocation == null)
            {
                String msg = MessageFormat.format(
                        "Configuration error: {0}={1} but: {2}={3}",
                        SecurityConf.CONNECTOR_USE_SSL,
                        this.isConnectorUseSSL(), keystoreLocationProperty,
                        this.keystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
            File f = new File(this.keystoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.keystoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", KEYSTORE_LOCATION,
                        this.keystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }

            // --- Check password is defined
            if (this.keystorePassword == null)
            {
                throw new ConfigurationException(keystorePasswordProperty);
            }
        }

        // --- Check that the keystore is not empty ---
        if ((tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REST_API
                && this.isEncryptionNeeded())
                || (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                        && this.connectorUseSSL)
                || this.isEncryptionNeeded())
        {
            boolean succp = false;
            /**
             * Check accessibility of both key store and keys with a given
             * password
             */
            for (KEYSTORE_TYPE keystoreType : KEYSTORE_TYPE
                    .values())
            {
                try
                {
                    SecurityHelper.checkKeyStorePasswords(
                            this.getKeystoreLocation(), keystoreType,
                            this.getKeystorePassword(), null,
                            this.getKeystorePassword());
                }
                catch (GeneralSecurityException e)
                {
                    continue;
                }
                catch (IOException e)
                {
                    continue;
                }
                succp = true;
            }
            if (!succp)
            {
                String message = "Can't access key store "
                        + this.keystoreLocation + " with the given password.";
                logger.error(message);
                throw new ConfigurationException(message);
            }
            // Check keystore: should be accessible, and not empty
            SecurityHelper.checkAccessAndAliasesForKeystore(
                    this.getKeystoreLocation(), this.getKeystorePassword(),
                    true);
        }

        // --- Check that the keystore and truststore ---
        if (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REST_API
                && this.isEncryptionNeeded())
        {
            // Check truststore: should be accessible
            SecurityHelper.checkAccessAndAliasesForKeystore(
                    this.getTruststoreLocation(), this.getTruststorePassword(),
                    false);

            if (this.isAuthenticationNeeded()
                    && this.isAuthenticationByCertificateNeeded())
            {
                // Check client keystore: should be accessible, and not empty
                SecurityHelper.checkAccessAndAliasesForKeystore(
                        this.getClientKeystoreLocation(),
                        this.getClientKeystorePassword(), true);
            }
        }

        // --- Check Aliases are defined in the keystore ---
        if ((this.isEncryptionNeeded() && this.keystoreLocation != null)
                || ((tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                        || tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REPLICATOR)
                        && this.isConnectorUseSSL()))
        {
            FileInputStream is = null;
            try
            {
                // Aliases to check
                HashMap<String, String> mapAliases = this
                        .getMapKeystoreAliasesForTungstenApplication();

                boolean connector_alias_client_to_connector_isFound = false;
                boolean connector_alias_connector_to_db_isFound = false;
                boolean replicator_alias_master_to_slave_isFound = false;

                String connector_alias_client_to_connector = mapAliases.get(
                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR);
                String connector_alias_connector_to_db = mapAliases.get(
                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB);
                String replicator_alias_master_to_slave = mapAliases.get(
                        SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);

                // If an aliase is not defined, do not look for it...obviously
                connector_alias_client_to_connector_isFound = (connector_alias_client_to_connector == null)
                        ? true
                        : false;
                connector_alias_connector_to_db_isFound = (connector_alias_connector_to_db == null)
                        ? true
                        : false;

                // Load the keystore in the user's home directory
                // Check only if there are aliases to find
                if (!connector_alias_client_to_connector_isFound
                        || !connector_alias_connector_to_db_isFound
                        || !replicator_alias_master_to_slave_isFound)
                {
                    is = new FileInputStream(this.getKeystoreLocation());
                    KeyStore keystore = KeyStore
                            .getInstance(KeyStore.getDefaultType());
                    String password = this.getKeystorePassword();
                    keystore.load(is, password.toCharArray());

                    // List the aliases
                    Enumeration<String> enumAliases = keystore.aliases();
                    while (enumAliases.hasMoreElements())
                    {
                        String alias = enumAliases.nextElement();

                        // Does alias refer to a private key?
                        // boolean b = keystore.isKeyEntry(alias);

                        // Does alias refer to a trusted certificate?
                        // b = keystore.isCertificateEntry(alias);

                        connector_alias_client_to_connector_isFound = connector_alias_client_to_connector_isFound == true
                                || (connector_alias_client_to_connector != null
                                        && connector_alias_client_to_connector
                                                .equals(alias));

                        connector_alias_connector_to_db_isFound = connector_alias_connector_to_db_isFound == true
                                || (connector_alias_connector_to_db != null
                                        && connector_alias_connector_to_db
                                                .equals(alias));

                        replicator_alias_master_to_slave_isFound = replicator_alias_master_to_slave_isFound == true
                                || (replicator_alias_master_to_slave != null
                                        && replicator_alias_master_to_slave
                                                .equals(alias));
                    }
                    // --- Exception when an alias is defined but not found ---

                    // --- Connector
                    // Client to Connector
                    if (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR)
                    {
                        // Client to Connector
                        this.buildAndThrowExceptionforMissingAlias(
                                connector_alias_client_to_connector,
                                connector_alias_client_to_connector_isFound,
                                SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                                is);

                        // Connector to DB
                        this.buildAndThrowExceptionforMissingAlias(
                                connector_alias_connector_to_db,
                                connector_alias_connector_to_db_isFound,
                                SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                                is);

                    }

                    // --- Replicator
                    if (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REPLICATOR)
                    {
                        this.buildAndThrowExceptionforMissingAlias(
                                replicator_alias_master_to_slave,
                                replicator_alias_master_to_slave_isFound,
                                SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE,
                                is);
                    }
                }

            }
            catch (java.security.cert.CertificateException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (NoSuchAlgorithmException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (FileNotFoundException e)
            {
                this.closeInputStream(is);
                // Noting to do: this has already been checked
            }
            catch (KeyStoreException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (IOException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            this.closeInputStream(is); // Close inputStream if not already done
        }

        // --- Check Truststore location ---
        if ((this.isEncryptionNeeded() && this.truststoreLocation != null)
                || (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                        && this.isConnectorUseSSL()))
        {
            // --- Check file location is specified ---
            if (this.truststoreLocation == null)
            {
                String msg = MessageFormat.format(
                        "Configuration error: {0}={1} but: {2}={3}",
                        SecurityConf.CONNECTOR_USE_SSL,
                        this.isConnectorUseSSL(),
                        SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_LOCATION,
                        this.truststoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
            File f = new File(this.truststoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.truststoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        TRUSTSTORE_LOCATION, this.truststoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
        }
        else if (this.isEncryptionNeeded() && this.truststoreLocation == null)
        {
            throw new ConfigurationException("truststore.location");
        }

        // --- Check client keystore location ---
        // Used by the client for certificate based authentication
        if (this.isAuthenticationNeeded()
                && this.isAuthenticationByCertificateNeeded()
                && tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REST_API
                && this.clientKeystoreLocation != null)
        {
            // --- Check file location is specified ---
            if (this.clientKeystoreLocation == null)
            {
                String msg = MessageFormat.format(
                        "Configuration error: {0}={1} but: {2}={3}",
                        SecurityConf.HTTP_REST_API_SSL_USESSL,
                        this.isEncryptionNeeded(),
                        SecurityConf.HTTP_REST_API_CLIENT_KEYSTORE_LOCATION,
                        this.clientKeystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
            File f = new File(this.clientKeystoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.clientKeystoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", KEYSTORE_LOCATION,
                        this.clientKeystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
        }
        else if (this.isAuthenticationNeeded()
                && this.isAuthenticationByCertificateNeeded()
                && tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.REST_API
                && this.clientKeystoreLocation == null)
        {
            throw new ConfigurationException(
                    SecurityConf.HTTP_REST_API_CLIENT_KEYSTORE_LOCATION);
        }

        // --- Check password for Truststore ---
        if (this.isEncryptionNeeded() && this.truststorePassword == null)
        {
            throw new ConfigurationException("truststore.password");
        }

        // --- Check password file location ---
        if (this.isAuthenticationNeeded()
                && !this.isAuthenticationByCertificateNeeded()
                && this.passwordFileLocation != null)
        {
            File f = new File(this.passwordFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.passwordFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_PASSWORD_FILE_LOCATION,
                        this.passwordFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
        }

        // --- Check access file location ---
        if (this.isAuthenticationNeeded() && this.accessFileLocation != null)
        {
            File f = new File(this.accessFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.accessFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_ACCESS_FILE_LOCATION,
                        this.accessFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg,
                        new AssertionError("File must exist"));
            }
        }

    }

    /**
     * TODO: buildAndThrowExceptionforMissingAlias definition.
     * 
     * @param targetAlias
     * @param targetAliasIsFound
     * @param aliasDefinitionProperty
     * @param inputStreamToClose
     */
    private void buildAndThrowExceptionforMissingAlias(String targetAlias,
            boolean targetAliasIsFound, String aliasDefinitionProperty,
            InputStream inputStreamToClose)
    {
        String _aliasErrorMessage = "Keystore alias is defined as {0}={1} but cannot be found in {2}";

        if (targetAlias != null && targetAliasIsFound == false)
        {
            this.closeInputStream(inputStreamToClose);

            String aliasErrorMessage = MessageFormat.format(_aliasErrorMessage,
                    aliasDefinitionProperty, targetAlias,
                    this.getKeystoreLocation());
            throw new ServerRuntimeException(aliasErrorMessage,
                    new AssertionError("Alias must exist in keystore"));
        }
    }

    /**
     * Get the AuthenticationInfo as a TungstenProperties
     * 
     * @return TungstenProperties
     */
    @JsonIgnore
    public TungstenProperties getAsTungstenProperties()
    {
        TungstenProperties jmxProperties = new TungstenProperties();
        jmxProperties.put(SECURITY_INFO_PROPERTY, this);

        return jmxProperties;
    }

    // /**
    // * Retrieve (encrypted) password from file
    // *
    // * @throws ConfigurationException
    // */
    // public void retrievePasswordFromFile() throws ConfigurationException
    // {
    // TungstenProperties passwordProps = SecurityHelper
    // .loadPasswordsFromAuthenticationInfo(this);
    // String username = this.getUsername();
    // String goodPassword = passwordProps.get(username);
    // this.password = goodPassword;
    //
    // if (goodPassword == null)
    // throw new ConfigurationException(
    // MessageFormat
    // .format("Cannot find password for username= {0} \n PasswordFile={1}",
    // username, this.getPasswordFileLocation()));
    // }

    /**
     * Returns the decrypted password
     * 
     * @return String containing the (if needed) decrypted password
     * @throws ConfigurationException
     */
    public String getDecryptedPassword() throws ConfigurationException
    {
        if (this.password == null)
            return null;

        String clearTextPassword = this.password;
        // --- Try to decrypt the password ---
        if (this.useEncryptedPasswords)
        {
            Encryptor encryptor = new Encryptor(this);
            clearTextPassword = encryptor.decrypt(this.password);
        }
        return clearTextPassword;
    }

    /**
     * @return the encrypted password if useEncryptedPasswords==true or the
     *         clear text password otherwise
     */
    public String getPassword()
    {
        return this.password;
    }

    public void setKeystore(String keyStoreLocation, String keystorePassword)
    {
        this.setKeystoreLocation(keyStoreLocation);
        this.setKeystorePassword(keystorePassword);
    }

    public void setTruststore(String truststoreLocation,
            String truststorePassword)
    {
        this.setTruststoreLocation(truststoreLocation);
        this.setTruststorePassword(truststorePassword);
    }

    public boolean isAuthenticationNeeded()
    {
        return authenticationNeeded;
    }

    /**
     * Returns the minWaitOnFailedLogin value.
     * 
     * @return Returns the minWaitOnFailedLogin.
     */
    public Integer getMinWaitOnFailedLogin()
    {
        return minWaitOnFailedLogin;
    }

    /**
     * Returns the maxWaitOnFailedLogin value.
     * 
     * @return Returns the maxWaitOnFailedLogin.
     */
    public Integer getMaxWaitOnFailedLogin()
    {
        return maxWaitOnFailedLogin;
    }

    /**
     * Sets the maxWaitOnFailedLogin value.
     * 
     * @param maxWaitOnFailedLogin The maxWaitOnFailedLogin to set.
     */
    public void setMaxWaitOnFailedLogin(Integer maxWaitOnFailedLogin)
    {
        this.maxWaitOnFailedLogin = maxWaitOnFailedLogin;
    }

    /**
     * Returns the incrementStepWaitOnFailedLogin value.
     * 
     * @return Returns the incrementStepWaitOnFailedLogin.
     */
    public Integer getIncrementStepWaitOnFailedLogin()
    {
        return incrementStepWaitOnFailedLogin;
    }

    /**
     * Sets the incrementStepWaitOnFailedLogin value.
     * 
     * @param incrementStepWaitOnFailedLogin The incrementStepWaitOnFailedLogin
     *            to set.
     */
    public void setIncrementStepWaitOnFailedLogin(
            Integer incrementStepWaitOnFailedLogin)
    {
        this.incrementStepWaitOnFailedLogin = incrementStepWaitOnFailedLogin;
    }

    /**
     * Sets the minWaitOnFailedLogin value.
     * 
     * @param minWaitOnFailedLogin The minWaitOnFailedLogin to set.
     */
    public void setMinWaitOnFailedLogin(Integer minWaitOnFailedLogin)
    {
        this.minWaitOnFailedLogin = minWaitOnFailedLogin;
    }

    public void setAuthenticationNeeded(boolean authenticationNeeded)
    {
        this.authenticationNeeded = authenticationNeeded;
    }

    /**
     * Returns the authenticationByCertificateNeeded value.
     * 
     * @return Returns the authenticationByCertificateNeeded.
     */
    public boolean isAuthenticationByCertificateNeeded()
    {
        return authenticationByCertificateNeeded;
    }

    /**
     * Sets the authenticationByCertificateNeeded value.
     * 
     * @param authenticationByCertificateNeeded The
     *            authenticationByCertificateNeeded to set.
     */
    public void setAuthenticationByCertificateNeeded(
            boolean authenticationByCertificateNeeded)
    {
        this.authenticationByCertificateNeeded = authenticationByCertificateNeeded;
    }

    public boolean isEncryptionNeeded()
    {
        return encryptionNeeded;
    }

    public void setEncryptionNeeded(boolean encryptionNeeded)
    {
        this.encryptionNeeded = encryptionNeeded;
    }

    public String getKeystoreLocation()
    {

        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
    }

    /**
     * Returns the clientKeystoreLocation value.
     * 
     * @return Returns the clientKeystoreLocation.
     */
    public String getClientKeystoreLocation()
    {
        return clientKeystoreLocation;
    }

    /**
     * Sets the clientKeystoreLocation value.
     * 
     * @param clientKeystoreLocation The clientKeystoreLocation to set.
     */
    public void setClientKeystoreLocation(String clientKeystoreLocation)
    {
        this.clientKeystoreLocation = clientKeystoreLocation;
    }

    /**
     * Returns the clientKeystorePassword value.
     * 
     * @return Returns the clientKeystorePassword.
     */
    public String getClientKeystorePassword()
    {
        return clientKeystorePassword;
    }

    /**
     * Sets the clientKeystorePassword value.
     * 
     * @param clientKeystorePassword The clientKeystorePassword to set.
     */
    public void setClientKeystorePassword(String clientKeystorePassword)
    {
        this.clientKeystorePassword = clientKeystorePassword;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getPasswordFileLocation()
    {
        return passwordFileLocation;
    }

    public void setPasswordFileLocation(String passwordFileLocation)
    {
        this.passwordFileLocation = passwordFileLocation;
    }

    public String getAccessFileLocation()
    {
        return accessFileLocation;
    }

    public void setAccessFileLocation(String accessFileLocation)
    {
        this.accessFileLocation = accessFileLocation;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
    }

    public String getTruststoreLocation()
    {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation)
    {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
    }

    public boolean isUseTungstenAuthenticationRealm()
    {
        return useTungstenAuthenticationRealm;
    }

    public void setUseTungstenAuthenticationRealm(
            boolean useTungstenAuthenticationRealm)
    {
        this.useTungstenAuthenticationRealm = useTungstenAuthenticationRealm;
    }

    public boolean isUseEncryptedPasswords()
    {
        return useEncryptedPasswords;
    }

    public void setUseEncryptedPasswords(boolean useEncryptedPasswords)
    {
        this.useEncryptedPasswords = useEncryptedPasswords;
    }

    public String getParentPropertiesFileLocation()
    {
        return parentPropertiesFileLocation;
    }

    public void setParentPropertiesFileLocation(
            String parentPropertiesFileLocation)
    {
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }

    /**
     * Returns the connectorUseSSL value.
     * 
     * @return Returns the connectorUseSSL.
     */
    public boolean isConnectorUseSSL()
    {
        return connectorUseSSL;
    }

    /**
     * Sets the connectorUseSSL value.
     * 
     * @param connectorUseSSL The connectorUseSSL to set.
     */
    public void setConnectorUseSSL(boolean connectorUseSSL)
    {
        this.connectorUseSSL = connectorUseSSL;
    }

    /**
     * Returns the parentProperties value.
     * 
     * @return Returns the parentProperties.
     */
    public TungstenProperties getParentProperties()
    {
        return parentProperties;
    }

    /**
     * Sets the parentProperties value.
     * 
     * @param parentProperties The parentProperties to set.
     */
    public void setParentProperties(TungstenProperties parentProperties)
    {
        this.parentProperties = parentProperties;
    }

    /**
     * Returns the tungstenApplicationName value.
     * 
     * @return Returns the tungstenApplicationName.
     */
    public TUNGSTEN_APPLICATION_NAME getTungstenApplicationName()
    {
        return tungstenApplicationName;
    }

    /**
     * Sets the tungstenApplicationName value.
     * 
     * @param tungstenApplicationName The tungstenApplicationName to set.
     */
    public void setTungstenApplicationName(
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
    {
        this.tungstenApplicationName = tungstenApplicationName;
    }

    /**
     * Returns the mapKeystoreAliasesForTungstenApplication value.
     * 
     * @return Returns the mapKeystoreAliasesForTungstenApplication.
     */
    public HashMap<String, String> getMapKeystoreAliasesForTungstenApplication()
    {
        return mapKeystoreAliasesForTungstenApplication;
    }

    /**
     * Sets the mapKeystoreAliasesForTungstenApplication value.
     * 
     * @param mapKeystoreAliasesForTungstenApplication The
     *            mapKeystoreAliasesForTungstenApplication to set.
     */
    public void setMapKeystoreAliasesForTungstenApplication(
            HashMap<String, String> mapKeystoreAliasesForTungstenApplication)
    {
        this.mapKeystoreAliasesForTungstenApplication = mapKeystoreAliasesForTungstenApplication;
    }

    /**
     * Get the alias defined for the corresponding Tungsten application
     * 
     * @param tungestenApplicationName
     * @return the alias defined in security.properties if it exists. null
     *         otherwise
     */
    public String getKeystoreAliasForConnectionType(
            String aliasForConnectionType)
    {
        String alias = this.mapKeystoreAliasesForTungstenApplication
                .get(aliasForConnectionType);

        return alias;
    }

    /**
     * Returns the enabledProtocols value.
     * 
     * @return Returns the enabledProtocols.
     */
    public List<String> getEnabledProtocols()
    {
        return enabledProtocols;
    }

    /**
     * Sets the enabledProtocols value.
     * 
     * @param enabledProtocols The enabledProtocols to set.
     */
    public void setEnabledProtocols(List<String> enabledProtocols)
    {
        this.enabledProtocols = enabledProtocols;
    }

    /**
     * Returns the enabledCipherSuites value.
     * 
     * @return Returns the enabledCipherSuites.
     */
    public List<String> getEnabledCipherSuites()
    {
        return enabledCipherSuites;
    }

    /**
     * Sets the enabledCipherSuites value.
     * 
     * @param enabledCipherSuites The enabledCipherSuites to set.
     */
    public void setEnabledCipherSuites(List<String> enabledCipherSuites)
    {
        this.enabledCipherSuites = enabledCipherSuites;
    }

    /**
     * Returns the list of ciphers that are enabled on this JVM according to the
     * following rule.
     * <ol>
     * <li>If the enabled cipher suites list is empty, we return the default JVM
     * ciphers.</li>
     * <li>Otherwise we take the intersection of the enabled ciphers and the
     * ciphers available on the JVM.</li>
     * </ol>
     */
    @JsonIgnore
    public List<String> getJvmEnabledCipherSuites()
    {
        String[] jvmSupportedCiphers = SecurityHelper.getJvmSupportedCiphers();
        if (enabledCipherSuites == null || enabledCipherSuites.size() == 0)
        {
            return Arrays.asList(jvmSupportedCiphers);
        }
        else
        {
            String[] enabledAndSupported = SecurityHelper.getJvmEnabledCiphers(
                    enabledCipherSuites.toArray(new String[0]));
            return Arrays.asList(enabledAndSupported);
        }
    }

    /**
     * Try to find a file absolute path from a series of default location
     * 
     * @param fileToFind the file for which to look for an absolute path
     * @return the file with absolute path if found. returns the same unchanged
     *         object otherwise
     */
    private File findAbsolutePath(File fileToFind)
    {
        File foundFile = fileToFind;

        try
        {
            String clusterHome = ClusterConfiguration.getClusterHome();

            if (fileToFind.getPath() == fileToFind.getName()) // No absolute or
            // relative path
            // was given
            {
                // --- Try to find find in: cluster-home/conf
                File candidateFile = new File(clusterHome + File.separator
                        + "conf" + File.separator + fileToFind.getName());
                if (candidateFile.isFile())
                {
                    foundFile = candidateFile;
                    logger.debug(MessageFormat.format(
                            "File was specified with name only, and found in default location: {0}",
                            foundFile.getAbsoluteFile()));
                }
                else
                    throw new ConfigurationException(
                            MessageFormat.format("File does not exist: {0}",
                                    candidateFile.getAbsolutePath()));
            }
        }
        catch (ConfigurationException e)
        {
            logger.debug(MessageFormat.format(
                    "Cannot find absolute path for file: {0} \n{1}",
                    fileToFind.getName(), e.getMessage()));
            return fileToFind;
        }

        return foundFile;
    }

    /**
     * Load values from a JSON serialized string
     * 
     * @param json The JSON serialized string
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static AuthenticationInfo loadFromJSON(String json)
            throws JsonParseException, JsonMappingException, IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        AuthenticationInfo securityInfo = mapper.readValue(json,
                AuthenticationInfo.class);

        return securityInfo;
    }

    public static AuthenticationInfo _loadFromJSON(String json)
    {
        AuthenticationInfo securityInfo = null;
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            securityInfo = mapper.readValue(json, AuthenticationInfo.class);
        }
        catch (Exception e)
        {
            logger.error(MessageFormat.format(
                    "Internal Error. Could not load from JSON: {0}",
                    e.getMessage()));
            logger.debug(MessageFormat.format("json input= {0}", json));
        }

        return securityInfo;
    }

    /**
     * Serialize the TungstenProperties into a JSON String
     * 
     * @param prettyPrint Set to true to have the JSON output formatted for
     *            easier read
     * @return String representing JSON serialization of the TungstenProperties
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public String toJSON()
    {
        String json = null;
        try
        {
            json = this.toJSON(false);
        }
        catch (Exception e)
        {
            logger.error("Could not Serialize into JSON:", e);
        }
        return json;
    }

    public String toJSON(boolean prettyPrint)
            throws JsonGenerationException, JsonMappingException, IOException
    {
        String json = null;
        ObjectMapper mapper = new ObjectMapper(); // Setup Jackson
        mapper.configure(Feature.INDENT_OUTPUT, true);
        mapper.configure(Feature.SORT_PROPERTIES_ALPHABETICALLY, true);

        ObjectWriter writer = mapper.writer();
        if (prettyPrint)
            writer = writer.withDefaultPrettyPrinter();

        json = writer.writeValueAsString(this);

        return json;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuilder strAuthInfo = new StringBuilder();
        strAuthInfo.append(MessageFormat.format(
                "------------------------------------------------------------------- ### security.properties ### {0}\n",
                this.getTungstenApplicationName()));
        switch (tungstenApplicationName)
        {
            case ANY :
                strAuthInfo.append(MessageFormat.format("\t\t\t\t{0}={1}\n",
                        SecurityConf.SECURITY_JMX_USE_ENCRYPTION,
                        this.isEncryptionNeeded()));
                strAuthInfo.append(MessageFormat.format("\t\t\t\t{0}={1}\n",
                        SecurityConf.SECURITY_JMX_USE_AUTHENTICATION,
                        this.isAuthenticationNeeded()));
                strAuthInfo.append(MessageFormat.format("\t\t\t\t{0}={1}\n",
                        SecurityConf.SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD,
                        this.useEncryptedPasswords));
                break;
        }

        strAuthInfo.append(
                "\t\t\t\t-------------------------------------------------------------------");
        return strAuthInfo.toString();
    }

    /**
     * Silently tries to close an InputStream.
     * 
     * @param in
     */
    private void closeInputStream(InputStream in)
    {
        try
        {
            in.close();
        }
        catch (Exception e)
        {
            // Nothing to do, it's a last chance close
        }
    }

    /**
     * {@inheritDoc} This is used in SecurityHelperTest
     * 
     * @see java.lang.Object#clone()
     */
    public Object clone()
    {
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = (AuthenticationInfo) super.clone();
        }
        catch (CloneNotSupportedException cnse)
        {
            cnse.printStackTrace(System.err);
        }

        return authInfo;
    }

}
