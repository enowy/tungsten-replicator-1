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

import junit.framework.TestCase;

import java.util.List;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class AuthenticationInfoTest extends TestCase
{
    /**
     * Client side: checks that if a trustore location is set, the
     * checkAuthenticationInfo verifies that the trustore existe If it doesn't,
     * it throws an exception with a non null cause.
     * 
     * @throws Exception
     */
    public void testCheckAuthenticationInfo() throws Exception
    {
        AuthenticationInfo authInfo = new AuthenticationInfo();
        boolean sreThrown = false;

        // If encryption required: trustore location exist
        try
        {
            authInfo.setTruststoreLocation("");
            authInfo.checkAndCleanAuthenticationInfo();
        }
        catch (ServerRuntimeException sre)
        {
            assertNotNull(sre.getCause());
            sreThrown = true;
        }

        assert(sreThrown);
    }

    /**
     * Confirm that the getKeystoreAliasForConnectionType returns an alias name,
     * and null if it cannot be found.
     * 
     * @throws ConfigurationException
     */
    public void testgetKeystoreAlias()
    {
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);

            // --- Confirm we can retrieve the alias when it exists ---
            String alias = authInfo.getKeystoreAliasForConnectionType(
                    SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR);
            assertNotNull(alias);
            assertEquals("tungsten_data_fabric", alias);

            // --- Confirm that we return null when the alias does not exist ---
            alias = authInfo.getKeystoreAliasForConnectionType(
                    SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);
            assertNull(alias);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();
    }

    /**
     * Confirm that when setting security.rmi.encryption=true and
     * connector.security.use.ssl=false, we still have
     * isEncryptionNeeded()=false TUC-1071
     * 
     * @throws ConfigurationException
     */
    public void testTUC1071()
    {
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.tuc1071.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);

            assertFalse(authInfo.isEncryptionNeeded());
            assertFalse(authInfo.isConnectorUseSSL());
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();
    }

    /**
     * Confirm that trying to load an empty keystore will generate an error
     * 
     * @throws ConfigurationException
     */
    public void testTUC1080()
    {
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.emptyKeystore.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.ANY);
            assertFalse(
                    "The keystore is empty: An exception should have been thrown",
                    true);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertTrue("That's expected: the keystore is empty", true);
        }

        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();
    }

    /**
     * Confirm that we can retrieve enabled protocols and cipher suites
     * 
     * @throws ConfigurationException
     */
    public void testEnabledProtocolsAndCipherSuites()
    {
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        AuthenticationInfo authInfo_noInfo = null;
        try
        {
            // Confirm that values can be read
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.ANY);
            // File with no protocol or cipher info inside.
            authInfo_noInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.ANY);
            
            List<String> listEnabledProtocols = authInfo.getEnabledProtocols();
            assertNotNull("The list of Protocols should not be null", listEnabledProtocols);
            assertFalse("The list of Protocols should contain protocols", listEnabledProtocols.isEmpty());
            assertTrue("The list of Protocols should contain 2 protocols", listEnabledProtocols.size()==2);
            
            List<String> listEnabledCipherSuites = authInfo.getEnabledCipherSuites();
            assertNotNull("The list of Cipher Suites should not be null", listEnabledCipherSuites);
            assertFalse("The list of Cipher Suites should contain cipher suites", listEnabledCipherSuites.isEmpty());
            // Length of VMware approved ciphers is 9 according to 
            // https://wiki.eng.vmware.com/VSECR/vSDL/PSP/PSPRequirements#.C2.A0.C2.A0.5B3.3.E2.80.93M.5D_TLS_Cipher-Suites
            assertTrue("The list of Cipher Suites should contain at most 9 cipher suites", listEnabledCipherSuites.size()<=9);
            
            // Confirm that empty properties return correct values
            listEnabledProtocols = authInfo_noInfo.getEnabledProtocols();
            assertNotNull("The list of Protocols should not be null", listEnabledProtocols);
            assertTrue("The list of Protocols should be empty", listEnabledProtocols.isEmpty());
            
            listEnabledCipherSuites = authInfo_noInfo.getEnabledCipherSuites();
            assertNotNull("The list of Cipher Suites should not be null", listEnabledCipherSuites);
            assertTrue("The list of Cipher Suites should be empty", listEnabledCipherSuites.isEmpty());
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }

        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();
    }

}
