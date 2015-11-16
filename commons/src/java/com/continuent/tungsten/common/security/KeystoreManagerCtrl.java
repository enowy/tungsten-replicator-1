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

import java.security.UnrecoverableKeyException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.security.SecurityConf.KEYSTORE_TYPE;

/**
 * Application to manage passwords and users
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class KeystoreManagerCtrl
{

    private static Logger              logger             = Logger
            .getLogger(KeystoreManagerCtrl.class);

    private static KeystoreManagerCtrl keystoreManager    = null;

    private Options                    helpOptions        = new Options();
    private Options                    options            = new Options();

    // --- Options overriding elements in security.properties ---
    private String                     keystoreLocation   = null;
    private String                     keyAlias           = null;
    private KEYSTORE_TYPE              keyType            = null;

    private String                     keystorePassword   = null;
    private String                     keyPassword        = null;

    // -- Define constants for command line arguments ---
    private static final String        HELP               = "help";
    private static final String        _HELP              = "h";
    private static final String        _CHECK             = "c";
    private static final String        CHECK              = "check";
    private static final String        KEYSTORE_LOCATION  = "keystore.location";
    private static final String        _KEYSTORE_LOCATION = "ks";
    private static final String        KEYSTORE_PASSWORD  = "keystore.password";
    private static final String        _KEYSTORE_PASSWORD = "ksp";
    private static final String        KEY_ALIAS          = "key.alias";
    private static final String        _KEY_ALIAS         = "ka";
    private static final String        KEY_TYPE           = "key.type";
    private static final String        _KEY_TYPE          = "kt";
    private static final String        KEY_PASSWORD       = "key.password";
    private static final String        _KEY_PASSWORD      = "kp";

    private static Option              check;

    // --- Exit codes ---
    public enum EXIT_CODE
    {
        EXIT_OK(0), EXIT_ERROR(1);

        final int value;

        private EXIT_CODE(int value)
        {
            this.value = value;
        }
    }

    /**
     * Setup command line options
     */
    @SuppressWarnings("static-access")
    private void setupCommandLine()
    {
        // --- Options on the command line ---
        Option help = OptionBuilder.withLongOpt(HELP)
                .withDescription("Displays this message").create(_HELP);

        // Mutually excluding options
        OptionGroup optionGroup = new OptionGroup();
        check = OptionBuilder.withLongOpt(CHECK).hasArgs(0)
                .withDescription("Checks key password inside a keystore")
                .create(_CHECK);

        optionGroup.addOption(check);
        optionGroup.setRequired(true); // At least 1 command required

        // --- Options replacing parameters from security.properties ---
        Option keystoreLocation = OptionBuilder.withLongOpt(KEYSTORE_LOCATION)
                .withArgName("filename").hasArg()
                .withDescription("Location of the keystore file").isRequired()
                .create(_KEYSTORE_LOCATION);
        Option keyAlias = OptionBuilder.withLongOpt(KEY_ALIAS)
                .withArgName("key alias").hasArg().isRequired()
                .withDescription("Alias for the key").create(_KEY_ALIAS);
        Option keyType = OptionBuilder.withLongOpt(KEY_TYPE)
                .withArgName("key type").hasArg().isRequired()
                .withDescription(MessageFormat.format("Type of key: {0}",
                        KEYSTORE_TYPE.getListValues("|")))
                .create(_KEY_TYPE);

        Option keystorePassword = OptionBuilder.withLongOpt(KEYSTORE_PASSWORD)
                .withArgName("password").hasArg()
                .withDescription("Password for the keystore file")
                .create(_KEYSTORE_PASSWORD);
        Option keyPassword = OptionBuilder.withLongOpt(KEY_PASSWORD)
                .withArgName("key password").hasArg()
                .withDescription("Password for the key").create(_KEY_PASSWORD);

        // --- Add options to the list ---
        // --- Help
        this.helpOptions.addOption(help);

        // --- Program command line options
        this.options.addOptionGroup(optionGroup);
        this.options.addOption(help);
        this.options.addOption(keystoreLocation);
        this.options.addOption(keyAlias);
        this.options.addOption(keyType);
        this.options.addOption(keystorePassword);
        this.options.addOption(keyPassword);
    }

    /**
     * Creates a new <code>PasswordManager</code> object
     */
    public KeystoreManagerCtrl()
    {
        this.setupCommandLine();
    }

    /**
     * Password Manager entry point
     * 
     * @param argv
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception
    {
        keystoreManager = new KeystoreManagerCtrl();

        // --- Options ---
        CommandLine line = null;

        try
        {
            CommandLineParser parser = new GnuParser();
            // --- Parse the command line arguments ---

            // --- Help
            line = parser.parse(keystoreManager.helpOptions, argv, true);
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit(EXIT_CODE.EXIT_OK);
            }

            // --- Program command line options
            line = parser.parse(keystoreManager.options, argv);

            // --- Compulsory arguments : Get options ---
            if (line.hasOption(_KEYSTORE_LOCATION))
                keystoreManager.keystoreLocation = line
                        .getOptionValue(_KEYSTORE_LOCATION);

            if (line.hasOption(_KEY_ALIAS))
                keystoreManager.keyAlias = line.getOptionValue(_KEY_ALIAS);

            if (line.hasOption(_KEY_TYPE))
            {
                String keyType = line.getOptionValue(_KEY_TYPE);
                // This will throw an exception if the type is not recognised
                KEYSTORE_TYPE certificateTYpe = KEYSTORE_TYPE
                        .fromString(keyType);
                keystoreManager.keyType = certificateTYpe;
            }

            if (line.hasOption(_KEYSTORE_PASSWORD))
                keystoreManager.keystorePassword = line
                        .getOptionValue(_KEYSTORE_PASSWORD);
            if (line.hasOption(_KEY_PASSWORD))
                keystoreManager.keyPassword = line
                        .getOptionValue(_KEY_PASSWORD);

        }
        catch (ParseException exp)
        {
            logger.error(exp.getMessage());
            DisplayHelpAndExit(EXIT_CODE.EXIT_ERROR);
        }
        catch (Exception e)
        {
            // Workaround for Junit test
            if (e.toString().contains("CheckExitCalled"))
            {
                throw e;
            }
            else
            // Normal behaviour
            {
                logger.error(e.getMessage());
                Exit(EXIT_CODE.EXIT_ERROR);
            }

        }

        // --- Perform commands ---

        // ######### Check password ##########
        if (line.hasOption(_CHECK))
        {
            try
            {
                if (keystoreManager.keystorePassword == null
                        || keystoreManager.keyPassword == null)
                {
                    // --- Get passwords from stdin if not given on command line
                    List<String> listPrompts = Arrays
                            .asList("Keystore password:",
                                    MessageFormat.format(
                                            "Password for key {0}:",
                                            keystoreManager.keyAlias));
                    List<String> listUserInput = keystoreManager
                            .getUserInputFromStdin(listPrompts);

                    if (listUserInput.size() == listPrompts.size())
                    {
                        keystoreManager.keystorePassword = listUserInput.get(0);
                        keystoreManager.keyPassword = listUserInput.get(1);
                    }
                    else
                    {
                        throw new NoSuchElementException();
                    }
                }
                try
                {
                    // --- Check that all keys in keystore use the keystore
                    // password
                    logger.info(MessageFormat.format("Using keystore:{0}",
                            keystoreManager.keystoreLocation));

                    SecurityHelper.checkKeyStorePasswords(
                            keystoreManager.keystoreLocation,
                            keystoreManager.keyType,
                            keystoreManager.keystorePassword,
                            keystoreManager.keyAlias,
                            keystoreManager.keyPassword);
                    logger.info(MessageFormat.format(
                            "OK : Identical password for Keystore and key={0}",
                            keystoreManager.keyAlias));
                }
                catch (UnrecoverableKeyException uke)
                {
                    logger.error(MessageFormat.format(
                            "At least 1 key has a wrong password.{0}",
                            uke.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format("{0}", e.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }

            }
            catch (NoSuchElementException nse)
            {
                logger.error(nse.getMessage());
                Exit(EXIT_CODE.EXIT_ERROR);
            }
            catch (Exception e)
            {
                logger.error(MessageFormat.format(
                        "Error while running the program: {0}",
                        e.getMessage()));
                Exit(EXIT_CODE.EXIT_ERROR);
            }
        }

        Exit(EXIT_CODE.EXIT_OK);
    }

    /**
     * Reads user input from stdin
     * 
     * @param listPrompts list of prompts to display, asking for user input
     * @return a list containing user inputs
     */
    private List<String> getUserInputFromStdin(List<String> listPrompts)
    {
        List<String> listUserInput = new ArrayList<String>();

        Scanner console = new Scanner(System.in);
        Scanner lineTokenizer = null;

        for (String prompt : listPrompts)
        {
            System.out.println(prompt);

            try
            {
                lineTokenizer = new Scanner(console.nextLine());
            }
            catch (NoSuchElementException nse)
            {
                console.close();
                throw new NoSuchElementException(MessageFormat
                        .format("Missing user input=  {0}", prompt));
            }

            if (lineTokenizer.hasNext())
            {
                String userInput = lineTokenizer.next();
                listUserInput.add(userInput);
            }
        }

        console.close();
        return listUserInput;
    }

    /**
     * Display the program help and exits
     */
    private static void DisplayHelpAndExit(EXIT_CODE exitCode)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp("tkeystoremanager", keystoreManager.options);
        Exit(exitCode);
    }

    private static void Exit(EXIT_CODE exitCode)
    {
        System.exit(exitCode.value);
    }

}
