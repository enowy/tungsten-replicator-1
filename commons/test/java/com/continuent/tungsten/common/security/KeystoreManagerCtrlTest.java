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
public class KeystoreManagerCtrlTest
{

    @Rule
    public final ExpectedSystemExit exit                                 = ExpectedSystemExit
            .none();

    @Rule
    public final SystemOutRule      systemOutRule                        = new SystemOutRule()
            .enableLog();

    @Rule
    public final SystemErrRule      systemErrRule                        = new SystemErrRule()
            .enableLog();

    private int                     EXIT_OK                              = PasswordManagerCtrl.EXIT_CODE.EXIT_OK.value;
    private int                     EXIT_ERROR                           = PasswordManagerCtrl.EXIT_CODE.EXIT_ERROR.value;
    private static final String     KEYSTORE_LOCATION                    = "tungsten_keystore.jks";
    private static final String     KEYSTORE_JCEKS_LOCATION              = "tungsten_jgroups_keystore.jceks";
    private static final String     ALIAS_JCEKS                          = "jgroups";
    private static final String     KEYSTORE_DIFFERENT_PASSWORD_LOCATION = "tungsten_keystore_different_password.jks";
    private static final String     KEYSTORE_TYPE                        = "jks";
    private static final String     KEYSTORE_PASSWORD                    = "tungsten";

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
                    String expected = "usage: tkeystoremanager";
                    assertTrue(
                            "Can't find expected string on std out " + expected,
                            out.contains(expected));
                }
            });

            KeystoreManagerCtrl.main(argv);
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
                    String expected = "usage: tkeystoremanager";
                    assertTrue(
                            "Can't find expected string on std out " + expected,
                            out.contains(expected));
                }
            });

            KeystoreManagerCtrl.main(argv);
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
     * Confirm that the exit code is 0 when alias password is the same as
     * keystore password
     */
    @Test
    public void testCheckCorrectAliasPassword()
    {
        String commnandLine = "-c -ks " + KEYSTORE_LOCATION + " -ksp "
                + KEYSTORE_PASSWORD + " -kt " + KEYSTORE_TYPE
                + " -ka alias_jgroups -kp " + KEYSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            KeystoreManagerCtrl.main(argv);
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
     * Confirm that the exit code is 0 when alias password is the same as
     * keystore password and keystore type is jceks
     */
    @Test
    public void testCheckCorrectAliasPassword_jceks()
    {
        String commnandLine = "-c -ks " + KEYSTORE_JCEKS_LOCATION + " -ksp "
                + KEYSTORE_PASSWORD + " -kt "
                + SecurityConf.KEYSTORE_TYPE.jceks.name() + " -ka "
                + ALIAS_JCEKS + " -kp " + KEYSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            KeystoreManagerCtrl.main(argv);
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
     * Confirm that the exit code is 1 when alias password is not the same as
     * keystore password
     */
    @Test
    public void testCheckDifferentAliasPassword()
    {
        String commnandLine = "-c -ks " + KEYSTORE_LOCATION + " -ksp "
                + KEYSTORE_DIFFERENT_PASSWORD_LOCATION + " -kt " + KEYSTORE_TYPE
                + " -ka different_password -kp " + KEYSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_ERROR);
            KeystoreManagerCtrl.main(argv);
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
     * Confirm that the exit code is 0 when alias password is not the same as
     * keystore password but specified correctly in the command line
     */
    @Test
    public void testCheckDifferentAliasPasswordOK()
    {
        String commnandLine = "-c -ks " + KEYSTORE_DIFFERENT_PASSWORD_LOCATION
                + " -ksp " + KEYSTORE_PASSWORD + " -kt " + KEYSTORE_TYPE
                + " -ka different_password -kp different_password";
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_OK);
            KeystoreManagerCtrl.main(argv);
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
     * Confirm that the exit code is 1 when wrong keystore type is given
     */
    @Test
    public void testWrongkeystoreType()
    {
        String commnandLine = "-c -ks " + KEYSTORE_JCEKS_LOCATION + " -ksp "
                + KEYSTORE_PASSWORD + " -kt "
                + SecurityConf.KEYSTORE_TYPE.jks.name() + " -ka "
                + ALIAS_JCEKS + " -kp " + KEYSTORE_PASSWORD;
        String argv[] = commnandLine.split(" ");

        try
        {
            exit.expectSystemExitWithStatus(EXIT_ERROR);
            KeystoreManagerCtrl.main(argv);
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
