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
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;

import junit.framework.TestCase;

import org.junit.Assert;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;

/**
 * Implements a simple unit test for SecurityHelper
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelperTest extends TestCase
{

    public static int stopNow = 0;

    /**
     * Test we can retrieve passwords from the passwords.store file
     * 
     * @throws Exception
     */
    public void testLoadPasswordsFromFile() throws Exception
    {
        AuthenticationInfo authenticationInfo = new AuthenticationInfo();
        authenticationInfo.setPasswordFileLocation("sample.passwords.store");

        // This should not be null if we retrieve passwords from the file
        TungstenProperties tungsteProperties = SecurityHelper
                .loadPasswordsFromAuthenticationInfo(authenticationInfo);
        assertNotNull(tungsteProperties);
    }

    /**
     * Test we can retrieve authentication information from the
     * security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation()
            throws ConfigurationException
    {
        // Get authInfo from the configuration file on the CLIENT_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Get authInfo from the configuration file on the SERVER_SIDE
        authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Check that an Exception is thrown when the configuration file is not
        // found
        ConfigurationException configurationException = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties_DOES_NOT_EXIST");
        }
        catch (ConfigurationException ce)
        {
            configurationException = ce;
        }
        assertNotNull(configurationException);

    }

    /**
     * Confirm that we can retrieve values from the security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testRetrieveInformation() throws ConfigurationException
    {
        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        TungstenProperties securityProp = authInfo.getAsTungstenProperties();
        assertNotNull(securityProp);

        Boolean useJmxAuthentication = securityProp
                .getBoolean(SecurityConf.SECURITY_JMX_USE_AUTHENTICATION);
        assertNotNull(useJmxAuthentication);
    }

    /**
     * Reset system properties to null value
     */
    public static void resetSecuritySystemProperties()
    {
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.rmi.ssl.client.enabledCipherSuites");
        System.clearProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLPROTOCOLS);
        System.clearProperty(SecurityConf.SYSTEM_PROP_CLIENT_SSLCIPHERS);
        System.clearProperty("https.protocols");
    }

    /**
     * Confirm that once we have loaded the security information, it becomes
     * available in system properties
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation_and_setSystemProperties()
            throws ConfigurationException
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Check it's available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNotNull(systemProperty);
    }

    /**
     * Confirm that once we have loaded the security information, it becomes
     * available in system properties Confirm that application specific info is
     * used Note: Connector only info for now
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation_and_setSystemProperties_4_Connector()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties", false,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertNotNull(authInfo);

        // Check it's available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));
    }

    /**
     * Confirm that when set to empty in the configuration file, important
     * properties are set to null and not ""
     * 
     * @throws ConfigurationException
     */
    public void testCrucialEmptyPropertiesAreNull()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertNotNull(authInfo);

        assertTrue(authInfo.getKeystoreLocation() == null);
        assertTrue(authInfo.getTruststoreLocation() == null);
    }

    /**
     * Confirm behavior when connector.security.use.SSL=false
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_false()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that by default connector.security.use.SSL=false
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertFalse(authInfo.isConnectorUseSSL());

        // Confirm that unnecessary information has been deleted
        assertNull(authInfo.getKeystoreLocation());
        assertNull(authInfo.getKeystorePassword());
        assertNull(authInfo.getTruststoreLocation());
        assertNull(authInfo.getTruststorePassword());

        // Check it's NOT available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNull(systemProperty);

    }

    // Determines whether protocols, cipher suites or both properties is set to empty.
    private static enum TEST_ARG {PROTOCOLS, CIPHERS, BOTH}; 
    
    /**
     * check that loading AuthenticationInfo also succeeds with empty protocol
     * property
     */
    public void testSSLWithEmptyProtocolsProperty()
    {
        testSSLProtocolsAndCipherSuitesProperties(SecurityHelperTest.TEST_ARG.PROTOCOLS);
    }

    /**
     * check that loading AuthenticationInfo also succeeds with empty cipher
     * suites property
     */
    public void testSSLWithEmptyCipherSuitesProperty()
    {
        testSSLProtocolsAndCipherSuitesProperties(SecurityHelperTest.TEST_ARG.CIPHERS);
    }

    /**
     * check that loading AuthenticationInfo also succeeds with empty protocols
     * and cipher suites properties
     */
    public void testSSLWithEmptyProtocolsAndCipherSuitesProperties()
    {
        testSSLProtocolsAndCipherSuitesProperties(SecurityHelperTest.TEST_ARG.BOTH);
    }
    
    /**
     * check that loading AuthenticationInfo also succeeds with empty protocols
     * and cipher suites properties
     */
    private void testSSLProtocolsAndCipherSuitesProperties(SecurityHelperTest.TEST_ARG arg)
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that by default connector.security.use.SSL=false
        AuthenticationInfo authInfo = null;
        try
        {
            List <String> emptyList = new ArrayList <String>();
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties");
            if (arg == SecurityHelperTest.TEST_ARG.PROTOCOLS)
            {
                // Set empty protocols property
                authInfo.setEnabledProtocols(emptyList);
            } 
            else if (arg == SecurityHelperTest.TEST_ARG.CIPHERS)
            {
                // Set empty cipher suites property
                authInfo.setEnabledCipherSuites(emptyList);
            }
            else if (arg == SecurityHelperTest.TEST_ARG.BOTH)
            {
                // Set empty protocols property
                authInfo.setEnabledProtocols(emptyList);
                // Set empty cipher suites property
                authInfo.setEnabledCipherSuites(emptyList);
            }
            
            // Test
            SecurityHelper.testSetSecurityProperties(authInfo, true);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertTrue(authInfo.isConnectorUseSSL());
        // Reset info
        resetSecuritySystemProperties();
    }

    
    /**
     * Confirm behavior when connector.security.use.SSL=true
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_true()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that by default connector.security.use.SSL=false
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertTrue(authInfo.isConnectorUseSSL());
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_errors_true()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }
        assertEquals(null, authInfo);

        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when trustore location is not
        // specified
        authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl2.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }
        assertEquals(null, authInfo);

    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias are
     * defined
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
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
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and wrong alias is
     * specified
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_error()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.wrong.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias is
     * defined + alias is not the first in the list inside the keystore. This
     * shows that we can select an alias inside the keystore
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_2()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.2.position.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
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
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias are not
     * defined This shows that it uses first alias it finds
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_not_defined()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.not.defined.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
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
        resetSecuritySystemProperties();
    }

    /**
     * Confirm that an Exception is thrown when the security.properties files
     * does not pass validation. Confirm validation steps
     * 
     * @throws ConfigurationException
     */
    public void testValidateSecurityProperties()
            throws ConfigurationException, ServerRuntimeException
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that initial values are OK
        AuthenticationInfo authInfo = null;
        AuthenticationInfo badAuthInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.validation.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.REST_API);
            badAuthInfo = (AuthenticationInfo) authInfo.clone();
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "Initial values should not cause an exception as they are supposed to be OK",
                    true);
        }

        // --- Try validations steps one after the other ---

        // ###### REST API: If Authentication is on, Encryption can be off
        badAuthInfo.setAuthenticationNeeded(true);
        try
        {
            badAuthInfo.setAuthenticationNeeded(true);
            assertFalse(badAuthInfo.isEncryptionNeeded()); // Encryption is off

            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse(badAuthInfo.isEncryptionNeeded()); // Not Updated to
                                                           // true
        }
        catch (ConfigurationException e)
        {
            assertTrue(
                    "That should not throw an exception, just update values.",
                    false);
        }

        // ############################## Check password file location ########
        badAuthInfo = (AuthenticationInfo) authInfo.clone();

        // Feature off + bad value
        badAuthInfo.setPasswordFileLocation(
                badAuthInfo.getPasswordFileLocation() + "_XXX");
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("Authentication not needed. No exception was thrown.", true);

        // Feature on + good value
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setAuthenticationNeeded(true);

        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("File exists, no exception should be thrown !", true);

        // Feature on + bad value
        try
        {
            badAuthInfo.setPasswordFileLocation(
                    badAuthInfo.getPasswordFileLocation() + "_XXX");
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("That's expected", true);
        }

        // ############################## Check keystore for https ###########
        badAuthInfo = (AuthenticationInfo) authInfo.clone();

        // Feature off + bad value
        badAuthInfo.setKeystoreLocation(
                badAuthInfo.getKeystoreLocation() + "_XXX");
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("Feature off. No exception was thrown.", true);

        // Feature on + good value
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setEncryptionNeeded(true);
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("File exists, no exception should be thrown !", true);

        // Feature on + bad value
        try
        {
            badAuthInfo.setKeystoreLocation(
                    badAuthInfo.getKeystoreLocation() + "_XXX");
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("That's expected", true);
        }

        // ############################## Check trustore for server ###########
        badAuthInfo = (AuthenticationInfo) authInfo.clone();

        // Feature off + bad value
        badAuthInfo.setTruststoreLocation(
                badAuthInfo.getTruststoreLocation() + "_XXX");
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("Feature off, no exception should be thrown !", true);

        // Feature on + good value
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setEncryptionNeeded(true);
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("File exists, no exception should be thrown !", true);

        // Feature on + bad value
        try
        {
            badAuthInfo.setTruststoreLocation(
                    badAuthInfo.getTruststoreLocation() + "_XXX");
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (Exception ee)
        {
            assertTrue("That's expected", true);
        }

        // ############################## Check keystore for client ###########
        badAuthInfo = (AuthenticationInfo) authInfo.clone();

        // Feature off + bad value
        badAuthInfo.setClientKeystoreLocation(
                badAuthInfo.getClientKeystoreLocation() + "_XXX");
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("Feature off. No exception was thrown.", true);

        // Feature on + good value
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setAuthenticationNeeded(true);
        badAuthInfo.setAuthenticationByCertificateNeeded(true);
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("File exists, no exception should be thrown !", true);

        // Feature on + bad value
        try
        {
            badAuthInfo.setClientKeystoreLocation(
                    badAuthInfo.getClientKeystoreLocation() + "_XXX");
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("That's expected", true);
        }

        // ######################## Check non empty aliases in Keystore #######
        // This also checks that the password can open the keystore
        String EMPTY_KEYSTORE = "empty_keystore.jks";
        badAuthInfo = (AuthenticationInfo) authInfo.clone();

        // Feature off + bad value
        badAuthInfo.setKeystoreLocation(EMPTY_KEYSTORE);
        badAuthInfo.checkAndCleanAuthenticationInfo(
                TUNGSTEN_APPLICATION_NAME.REST_API);
        assertTrue("Feature off. No exception was thrown.", true);

        // Feature on + bad value (empty aliases)
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setEncryptionNeeded(true);
        badAuthInfo.setKeystoreLocation(EMPTY_KEYSTORE);

        try
        {
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ConfigurationException e)
        {
            assertTrue("That's expected", true);
        }

        // Feature on + bad value (wrong password)
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setEncryptionNeeded(true);
        badAuthInfo.setKeystoreLocation(EMPTY_KEYSTORE);
        badAuthInfo.setKeystorePassword("bad_password");

        try
        {
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ConfigurationException e)
        {
            assertTrue("That's expected", true);
        }

        // Check that the trustore is accessible
        // Feature on + bad value (empty aliases)
        badAuthInfo = (AuthenticationInfo) authInfo.clone();
        badAuthInfo.setEncryptionNeeded(true);
        badAuthInfo.setTruststorePassword(
                badAuthInfo.getTruststorePassword() + "_XXX");

        try
        {
            badAuthInfo.checkAndCleanAuthenticationInfo(
                    TUNGSTEN_APPLICATION_NAME.REST_API);

            assertFalse("Exception should have been thrown !", true);
        }
        catch (ConfigurationException e)
        {
            assertTrue("That's expected", true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

    /**
     * Confirm that the SecurityHelper.getMatchingStrings() method correctly
     * computes intersection between two arrays of strings.
     * 
     * @throws ConfigurationException
     */
    public void testMatchingStrings()
            throws ConfigurationException, ServerRuntimeException
    {
        String[] s0 = {};
        String[] s1 = {"abcd"};
        String[] s2 = {"defg", "abcd"};
        String[] s3 = {"hijk", "abcd", "defg"};
        String[] s4 = {"0123", "defg", "hijl", "abcd"};
        String[] s5 = {"0123", "0234"};

        // Prove that intersecting with empty array results in empty array.
        Assert.assertArrayEquals(s0, SecurityHelper.getMatchingStrings(s0, s1));
        Assert.assertArrayEquals(s0, SecurityHelper.getMatchingStrings(s3, s0));

        // Prove that intersecting with a single member results in just that
        // member.
        Assert.assertArrayEquals(s1, SecurityHelper.getMatchingStrings(s1, s1));
        Assert.assertArrayEquals(s1, SecurityHelper.getMatchingStrings(s2, s1));
        Assert.assertArrayEquals(s1, SecurityHelper.getMatchingStrings(s3, s1));

        // Prove that intersection with multiple members results in only common
        // members. Sort result to ensure match.
        String[] intersection = SecurityHelper.getMatchingStrings(s3, s4);
        Arrays.sort(intersection);
        Assert.assertArrayEquals(new String[]{"abcd", "defg"}, intersection);

        // Prove that disjoint array values result in an empty set.
        Assert.assertArrayEquals(s0, SecurityHelper.getMatchingStrings(s3, s5));
    }

    /**
     * Confirm that RealmJMXAuthenticator.getRandomInt(min, max) always returns
     * a coherent value
     */
    public void testRealmJMXAuthenticatorRandomNumberGenerator()
    {
        // 0 value -> 0
        int randomNumber = SecurityHelper.getRandomInt(0, 0, 0);
        assertTrue(randomNumber == 0);

        // negative values -> 0
        randomNumber = SecurityHelper.getRandomInt(-1, -10, 1);
        assertTrue(randomNumber == 0);

        // One negative value -> 0 and other value
        randomNumber = SecurityHelper.getRandomInt(-10, 2, 1);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                randomNumber >= 0 && randomNumber <= 2);

        // inverted min and max -> revert values
        randomNumber = SecurityHelper.getRandomInt(5, 2, 1);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                randomNumber <= 5 && randomNumber >= 2);

        // steps of 100
        randomNumber = SecurityHelper.getRandomInt(500, 3000, 100);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                (randomNumber - 500) % 100 == 0);

        // steps of 15
        randomNumber = SecurityHelper.getRandomInt(500, 3000, 15);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                (randomNumber - 500) % 15 == 0);

        // step of 0 -> step of 1
        randomNumber = SecurityHelper.getRandomInt(500, 3000, 0);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                randomNumber >= 500 && randomNumber <= 3000);

        // step > max-min
        randomNumber = SecurityHelper.getRandomInt(0, 10, 11);
        assertTrue(MessageFormat.format("randomNumber={0}", randomNumber),
                randomNumber >= 0 && randomNumber <= 10);

    }

}
