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
     * Client side: checks that if a trustorelocation is set, the
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

        assert (sreThrown);
    }

    
    /**
     * Confirm that the getKeystoreAliasForConnectionType returns an alias name, and null if it cannot be found.
     * 
     * @throws ConfigurationException
     */
    public void testgetKeystoreAlias()
    {
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation("test.ssl.alias.security.properties", true, TUNGSTEN_APPLICATION_NAME.CONNECTOR);
            
            // --- Confirm we can retrieve the alias when it exists ---
            String alias = authInfo.getKeystoreAliasForConnectionType(SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR);
            assertNotNull(alias);
            assertEquals("tungsten_data_fabric", alias);
            
            // --- Confirm that we return null when the alias does not exist ---
            alias = authInfo.getKeystoreAliasForConnectionType(SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);
            assertNull(alias);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse("That should not be this kind of Exception being thrown", true);
        }
        
        // Reset info
        SecurityHelperTest.resetSecuritySystemProperties();
    }
}
