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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * This class defines a SecurityConf This matches parameters used for
 * Authentication and Encryption
 * 
 * @author Ludovic Launer
 * @version 1.0
 */
public class SecurityConf
{
    /** Location of the file where this is all coming from **/
    static public final String SECURITY_PROPERTIES_PARENT_FILE_LOCATION                                   = "security.properties.parent.file.location";

    static public final String SECURITY_PROPERTIES_FILE_NAME                                              = "security.properties";

    /** Location of file used for security **/
    static public final String SECURITY_DIR                                                               = "security.dir";

    /** Authentication and Encryption */
    static public final String SECURITY_JMX_USE_AUTHENTICATION                                            = "security.rmi.authentication";
    static public final String SECURITY_JMX_USERNAME                                                      = "security.rmi.authentication.username";
    static public final String SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD          = "security.rmi.tungsten.authenticationRealm.encrypted.password";
    static public final String SECURITY_JMX_USE_ENCRYPTION                                                = "security.rmi.encryption";
    static public final String SECURITY_PASSWORD_FILE_LOCATION                                            = "security.password_file.location";
    static public final String SECURITY_ACCESS_FILE_LOCATION                                              = "security.rmi.jmxremote.access_file.location";
    static public final String SECURITY_KEYSTORE_LOCATION                                                 = "security.keystore.location";
    static public final String SECURITY_KEYSTORE_PASSWORD                                                 = "security.keystore.password";
    static public final String SECURITY_TRUSTSTORE_LOCATION                                               = "security.truststore.location";
    static public final String SECURITY_TRUSTSTORE_PASSWORD                                               = "security.truststore.password";
    static public final String SECURITY_ENABLED_TRANSPORT_PROTOCOL                                        = "security.enabled.protocols";
    static public final String SECURITY_ENABLED_CIPHER_SUITES                                             = "security.enabled.cipher.suites";
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MIN                                   = "security.randomWaitOnFailedLogin.min";                         // (ms)
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MAX                                   = "security.randomWaitOnFailedLogin.max";                         // (ms)
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_INCREMENT_STEP                        = "security.randomWaitOnFailedLogin.increment.step";
    static public final String CONNECTOR_USE_SSL                                                          = "connector.security.use.ssl";
    static public final String CONNECTOR_SECURITY_KEYSTORE_LOCATION                                       = "connector.security.keystore.location";
    static public final String CONNECTOR_SECURITY_KEYSTORE_PASSWORD                                       = "connector.security.keystore.password";
    static public final String CONNECTOR_SECURITY_TRUSTSTORE_LOCATION                                     = "connector.security.truststore.location";
    static public final String CONNECTOR_SECURITY_TRUSTSTORE_PASSWORD                                     = "connector.security.truststore.password";

    static public final String HTTP_REST_API_SSL_USESSL                                                   = "http.rest.api.security.ssl.useSsl";
    static public final String HTTP_REST_API_KEYSTORE_LOCATION                                            = "http.rest.api.security.keystore.location";
    static public final String HTTP_REST_API_KEYSTORE_PASSWORD                                            = "http.rest.api.security.keystore.password";
    static public final String HTTP_REST_API_TRUSTSTORE_LOCATION                                          = "http.rest.api.security.truststore.location";
    static public final String HTTP_REST_API_TRUSTSTORE_PASSWORD                                          = "http.rest.api.security.truststore.password";
    static public final String HTTP_REST_API_AUTHENTICATION                                               = "http.rest.api.security.authentication";
    static public final String HTTP_REST_API_AUTHENTICATION_USE_CERTIFICATE                               = "http.rest.api.security.authentication.use.certificate";
    static public final String HTTP_REST_API_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD         = "http.rest.api.security.authentication.use.encrypted.password";
    static public final String HTTP_REST_API_CLIENT_KEYSTORE_LOCATION                                     = "http.rest.api.security.client.keystore.location";
    static public final String HTTP_REST_API_CLIENT_KEYSTORE_PASSWORD                                     = "http.rest.api.security.client.keystore.password";

    /** Alias for Tungsten applications */
    static public final String KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR                               = "connector.security.keystore.alias.client.to.connector";
    static public final String KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB                                   = "connector.security.keystore.alias.connector.to.db";
    static public final String KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE                                  = "replicator.security.keystore.alias.replicator.master.to.slave";

    /** Authentication and Encryption: DEFAULT values */
    static public final String CONNECTOR_USE_SSL_DEFAULT                                                  = "false";
    static public final String SECURITY_USE_AUTHENTICATION_DEFAULT                                        = "false";
    static public final String SECURITY_USE_ENCRYPTION_DEFAULT                                            = "false";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT                         = "true";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT      = "false";
    static public final String HTTP_REST_API_AUTHENTICATION_DEFAULT                                       = "false";
    static public final String HTTP_REST_API_AUTHENTICATION_USE_CERTIFICATE_DEFAULT                       = "false";
    static public final String HTTP_REST_API_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT = "false";
    static public final String HTTP_REST_API_SSL_USESSL_DEFAULT                                           = "false";
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MIN_DEFAULT                           = "500";                                                          // (ms)
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MAX_DEFAULT                           = "1000";                                                         // (ms)
    static public final String SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_INCREMENT_STEP_DEFAULT                = "1";

    static public final String KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR_DEFAULT                       = null;
    static public final String KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB_DEFAULT                           = null;
    static public final String KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE_DEFAULT                          = null;

    /** Application specific information */
    static public final String SECURITY_APPLICATION_RMI_JMX                                               = "rmi_jmx";
    static public final String SECURITY_APPLICATION_CONNECTOR                                             = "connector";
    static public final String SECURITY_APPLICATION_REST_API                                              = "rest_api";

    /** System variable names */
    static final String        SYSTEM_PROP_CLIENT_SSLPROTOCOLS                                            = "javax.rmi.ssl.client.enabledProtocols";
    static final String        SYSTEM_PROP_CLIENT_SSLCIPHERS                                              = "javax.rmi.ssl.client.enabledCipherSuites";
    
    
    /**
     * Defines the different kinds of keystore.
     * Encapsulates utility functions
     * 
     * @author <a href="mailto:llauner@vmware.com">Ludovic Launer</a>
     * @version 1.0
     */
    public static enum KEYSTORE_TYPE
    {
        jks, jceks, pkcs12;

        /**
         * Converts a string into the corresponding KEYSTORE_TYPE
         * 
         * @param x
         * @return
         * @throws ConfigurationException if it cannot cast
         */
        public static KEYSTORE_TYPE fromString(String x)
                throws ConfigurationException
        {
            for (KEYSTORE_TYPE currentType : KEYSTORE_TYPE
                    .values())
            {
                if (x.equalsIgnoreCase(currentType.toString()))
                    return currentType;
            }
            throw new ConfigurationException(MessageFormat.format(
                    "Cannot cast into a known CERTIFICATE_KEY_TYPE: {0}", x));
        }

        /**
         * Get the list of possible values as a String
         * 
         * @param separator to separate elements in the string
         * @return
         */
        public static String getListValues(String separator)
        {
            List<String> listValues = new ArrayList<String>();

            for (KEYSTORE_TYPE value : KEYSTORE_TYPE.values())
            {
                listValues.add(value.name());
            }
            return StringUtils.join(listValues, separator);
        }

    }
}
