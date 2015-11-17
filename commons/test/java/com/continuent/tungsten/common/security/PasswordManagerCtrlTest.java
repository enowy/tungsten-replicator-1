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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.contrib.java.lang.system.internal.CheckExitCalled;

/**
 * Unit test against PasswordManagerCtrl.
 * 
 * @author <a href="mailto:llauner@vmware.com">Launer Ludovic</a>
 * @version 1.0
 */
public class PasswordManagerCtrlTest
{

    @Rule
    public final ExpectedSystemExit exit                              = ExpectedSystemExit
            .none();

    @Rule
    public final SystemOutRule      systemOutRule                     = new SystemOutRule()
            .enableLog();

    @Rule
    public final SystemErrRule      systemErrRule                     = new SystemErrRule()
            .enableLog();

    private int                     EXIT_OK                           = PasswordManagerCtrl.EXIT_CODE.EXIT_OK.value;
    private int                     EXIT_ERROR                        = PasswordManagerCtrl.EXIT_CODE.EXIT_ERROR.value;
    private static final String     PASSWORDS_STORE_LOCATION          = "sample.passwords.store";
    private static final String     USERNAME                          = "tungsten";
    private static final String     PASSWORD                          = "secret";
    private static final String     PASSWORD_FILE_LOCATION            = "secretPassword.txt";

    private static final String     KEYSTORE_LOCATION                 = "tungsten_keystore.jks";
    private static final String     KEYSTORE_PASSWORD                 = "tungsten";
    private static final String     KEYSTORE_PASSWORD_FILE_LOCATION   = "tungstenPassword.txt";
    private static final String     TRUSTSTORE_LOCATION               = "tungsten_truststore.ts";
    private static final String     TRUSTSTORE_PASSWORD               = "tungsten";
    private static final String     TRUSTSTORE_PASSWORD_FILE_LOCATION = "tungstenPassword.txt";

    /**
     * Confirm that we can display help and get EXIT_OK return code
     */
    @Test
    public void testDisplayHelp()
    {
        String argv[] = {"--help"};
        try
        {
            exit.expectSystemExitWithStatus(0);
            exit.checkAssertionAfterwards(new Assertion()
            {
                public void checkAssertion()
                {
                    String out = systemOutRule.getLog();
                    // Confirm that we got the help displayed
                    String expected = "usage: tpasswd";
                    assertTrue(
                            "Can't find expected string on std out " + expected,
                            out.contains(expected));
                }
            });

            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Confirm that when giving non existing parameter, help is displayed and
     * EXIT_ERROR is returned
     */
    @Test
    public void testHelpAndErrorOnWrongParameter()
    {
        String argv[] = {"--does_not_exist"};
        try
        {
            exit.expectSystemExitWithStatus(EXIT_ERROR);
            exit.checkAssertionAfterwards(new Assertion()
            {
                public void checkAssertion()
                {
                    String out = systemOutRule.getLog();
                    // Confirm that we got the help displayed
                    String expected = "usage: tpasswd";
                    assertTrue(
                            "Can't find expected string on std out " + expected,
                            out.contains(expected));
                }
            });

            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_ERROR, exitCode);
        }
    }

    /**
     * Confirm that we can create a user: using clear text password
     */
    @Test
    public void testCreateUser()
    {
        String commnandLine = "-c " + USERNAME + " " + PASSWORD
                + " -t rest_api -p " + PASSWORDS_STORE_LOCATION + "";
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Confirm that we can create a user: using encrypted password
     */
    @Test
    public void testCreateUse_Encrypted()
    {
        String commnandLine = "-c " + USERNAME + " " + PASSWORD
                + " -t rest_api -p " + PASSWORDS_STORE_LOCATION + " -e -ts "
                + TRUSTSTORE_LOCATION + " -tsp " + TRUSTSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Confirm that we can authenticate a user Exit code is EXIT_OK
     */
    @Test
    public void testAuthenticateUser()
    {
        // Create user
        this.testCreateUser();
        // Authenticate user
        this.authenticateUser(EXIT_OK);
    }

    public void authenticateUser(int expectedExitCode)
    {
        String commnandLine = "-a " + USERNAME + " " + PASSWORD
                + " -t rest_api -p " + PASSWORDS_STORE_LOCATION + "";
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(expectedExitCode);

            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    expectedExitCode, exitCode);
        }
    }

    /**
     * Confirm that we can authenticate a user Exit code is EXIT_OK. Using
     * encrypted password
     */
    @Test
    public void testAuthenticateUser_Encrypted()
    {
        String commnandLine = "-a " + USERNAME + " " + PASSWORD
                + " -t rest_api -p " + PASSWORDS_STORE_LOCATION + " -e -ts "
                + TRUSTSTORE_LOCATION + " -tsp " + TRUSTSTORE_PASSWORD + " -ks "
                + KEYSTORE_LOCATION + " -ksp " + KEYSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        // Create user
        this.testCreateUse_Encrypted();

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);

            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Confirm that we can delete a user Exit code is EXIT_OK
     */
    @Test
    public void testDeleteUser()
    {
        String commnandLine = "-d " + USERNAME + " -t rest_api -p "
                + PASSWORDS_STORE_LOCATION + "";
        String argv[] = commnandLine.split(" ");

        // Create user
        this.testCreateUser();

        // Delete user
        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);

            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }

        // Try to authenticate deleted use
        this.authenticateUser(EXIT_ERROR);

        exit.expectSystemExitWithStatus(EXIT_OK);
    }

    /**
     * Confirm that we can create a user: using encrypted password and providing
     * password information in a file
     */
    @Test
    public void testCreateUse_Encrypted_Password_in_file()
    {
        String commnandLine = "-c " + USERNAME + " -t rest_api -p "
                + PASSWORDS_STORE_LOCATION + " -e -ts " + TRUSTSTORE_LOCATION
                + " -tspf " + TRUSTSTORE_PASSWORD_FILE_LOCATION + " -upf "
                + PASSWORD_FILE_LOCATION;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Confirm that we can get the list of users
     */
    @Test
    public void testListUsers()
    {
        String commnandLine = "-l -p " + PASSWORDS_STORE_LOCATION + "";
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            PasswordManagerCtrl.main(argv);
        }
        catch (Exception e)
        {
            // Confirm that we got the right exit code
            int exitCode = getExitCode(e);
            assertEquals("The program did not exist with expected status",
                    EXIT_OK, exitCode);
        }
    }

    /**
     * Retrieve exit code from CheckExitCalled exception
     * 
     * @param e CheckExitCalled exception
     * @return program exit code if found, 99 otherwise
     */
    private int getExitCode(Exception e)
    {
        int exitCode = 99;

        if (e instanceof CheckExitCalled)
        {
            CheckExitCalled checkExitCalled = (CheckExitCalled) e;
            exitCode = checkExitCalled.getStatus();
        }
        return exitCode;
    }

}
