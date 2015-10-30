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
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Random;

import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Custom Authentication Realm
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class RealmJMXAuthenticator implements JMXAuthenticator
{
    private static final Logger logger                 = Logger
            .getLogger(RealmJMXAuthenticator.class);

    private AuthenticationInfo  authenticationInfo     = null;

    private PasswordManager     passwordManager        = null;

    private static final String INVALID_CREDENTIALS    = "Invalid credentials";
    private static final String AUTHENTICATION_PROBLEM = "Error while trying to authenticate";

    public RealmJMXAuthenticator(AuthenticationInfo authenticationInfo)
            throws ConfigurationException
    {
        this.authenticationInfo = authenticationInfo;
        this.passwordManager = new PasswordManager(
                authenticationInfo.getParentPropertiesFileLocation(),
                ClientApplicationType.RMI_JMX);

        // this.passwordProps = SecurityHelper
        // .loadPasswordsFromAuthenticationInfo(authenticationInfo);
    }

    /**
     * Authenticate {@inheritDoc}
     * 
     * @see javax.management.remote.JMXAuthenticator#authenticate(java.lang.Object)
     */
    public Subject authenticate(Object credentials)
    {
        boolean authenticationOK = false;

        String[] aCredentials = this.checkCredentials(credentials);

        // --- Get auth parameters ---
        String username = (String) aCredentials[0];
        String password = (String) aCredentials[1];
        // String realm = (String) aCredentials[2];

        // --- Perform authentication ---
        try
        {
            // Password file syntax:
            // username=password
            // String goodPassword = this.passwordProps.get(username);
            String goodPassword = this.passwordManager
                    .getClearTextPasswordForUser(username);
            // this.authenticationInfo.setPassword(goodPassword);
            // // Decrypt password if needed
            // goodPassword = this.authenticationInfo.getPassword();

            if (goodPassword.equals(password))
                authenticationOK = true;

        }
        catch (Exception e)
        {
            // Throw same exception as authentication not OK :
            // Do not give any hint on failure reason
            throw new SecurityException(AUTHENTICATION_PROBLEM);
        }

        if (authenticationOK)
        {
            return new Subject(true,
                    Collections.singleton(new JMXPrincipal(username)),
                    Collections.EMPTY_SET, Collections.EMPTY_SET);
        }
        else
        {
            try
            {
                // Generate a random number between
                // security.randomWaitOnFailedLogin.min and
                // security.randomWaitOnFailedLogin.max
                int min = authenticationInfo.getMinWaitOnFailedLogin();
                int max = authenticationInfo.getMaxWaitOnFailedLogin();
                int increment = authenticationInfo
                        .getIncrementStepWaitOnFailedLogin();
                int randomNum = getRandomInt(min, max, increment);

                // Sleep a random number of seconds = between min and max
                logger.info(MessageFormat.format(
                        "Invalid credentials. Sleeping (ms): {0,number,#}",
                        randomNum));
                if (randomNum > 0)
                    Thread.sleep(randomNum);

            }
            catch (InterruptedException e)
            {
                logger.error(MessageFormat.format("Could not sleep !: {0}", e));
            }
            throw new SecurityException(INVALID_CREDENTIALS);
        }
    }

    /**
     * Check credentials are OK
     * 
     * @param credentials
     * @return String[] containing {username, password, realm}
     */
    private String[] checkCredentials(Object credentials)
    {
        // Verify that credentials is of type String[].
        if (!(credentials instanceof String[]))
        {
            // Special case for null so we get a more informative message
            if (credentials == null)
            {
                throw new SecurityException("Credentials required");
            }
            throw new SecurityException("Credentials should be String[]");
        }

        // Verify that the array contains three elements
        // (username/password/realm).
        final String[] aCredentials = (String[]) credentials;
        // if (aCredentials.length != 3)
        // {
        // throw new SecurityException("Credentials should have 3 elements");
        // }

        return aCredentials;
    }

    /**
     * Returns the authenticationInfo value.
     * 
     * @return Returns the authenticationInfo.
     */
    public AuthenticationInfo getAuthenticationInfo()
    {
        return authenticationInfo;
    }

    /**
     * Sets the authenticationInfo value.
     * 
     * @param authenticationInfo The authenticationInfo to set.
     */
    public void setAuthenticationInfo(AuthenticationInfo authenticationInfo)
    {
        this.authenticationInfo = authenticationInfo;
    }

    /**
     * Returns a psuedo-random number between min and max, inclusive. The
     * difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimim value
     * @param max Maximim value. Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int getRandomInt(int min, int max, int incrementStep)
    {
        // Checks and validation
        if (min < 0)
            min = 0;
        if (max < 0)
            max = 0;
        if (max < min)
        {
            // Invert max and min
            logger.warn(MessageFormat.format(
                    "{0} and {1} are inverted. You must specify values so that min <= max",
                    SecurityConf.SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MIN,
                    SecurityConf.SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MAX));
            int tmpMin = max;
            min = max;
            max = tmpMin;
        }
        if (incrementStep <= 0)
            incrementStep = 1;
        if (incrementStep >= max - min && (max - min) > 0)
        {
            logger.warn(MessageFormat.format(
                    "{0} is not coherent. {0} >= {1}-{2}",
                    SecurityConf.SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_INCREMENT_STEP,
                    SecurityConf.SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MIN,
                    SecurityConf.SECURITY_RANDOM_WAIT_ON_FAILED_LOGIN_MAX));
            incrementStep = 1;
        }
        if (max - min == 0)
            return max;

        // Usually this can be a field rather than a method variable
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        // int randomNum = rand.nextInt((max - min) + 1) + min;

        Integer range = max - min;
        int maxMultiplier = Double.valueOf(Math.floor(range / incrementStep))
                .intValue();
        int randomMultiplier = rand.nextInt(maxMultiplier);

        int randomNumberWithIncrement = min
                + (randomMultiplier * incrementStep);

        return randomNumberWithIncrement;
    }

}
