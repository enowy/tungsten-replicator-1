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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Application to manage passwords and users
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerCtrl
{

    private static Logger                                logger                         = Logger
            .getLogger(PasswordManagerCtrl.class);

    private Options                                      helpOptions                    = new Options();
    private Options                                      options                        = new Options();
    private static PasswordManagerCtrl                   pwd;
    private static PasswordManager.ClientApplicationType clientApplicationType          = null;
    public PasswordManager                               passwordManager                = null;

    // --- Options overriding elements in security.properties ---
    private Boolean                                      useEncryptedPassword           = null;
    private String                                       truststoreLocation             = null;
    private String                                       truststorePassword             = null;
    private String                                       truststorePasswordFileLocation = null;
    private String                                       keystoreLocation               = null;
    private String                                       keystorePassword               = null;
    private String                                       keystorePasswordFileLocation   = null;
    private String                                       passwordFileLocation           = null;
    private String                                       userPasswordFileLocation       = null;

    // -- Define constants for command line arguments ---
    private static final String                          HELP                           = "help";
    private static final String                          _HELP                          = "h";
    private static final String                          _AUTHENTICATE                  = "a";
    private static final String                          AUTHENTICATE                   = "authenticate";
    private static final String                          CREATE                         = "create";
    private static final String                          _CREATE                        = "c";
    private static final String                          DELETE                         = "delete";
    private static final String                          _DELETE                        = "d";
    private static final String                          FILE                           = "file";
    private static final String                          _FILE                          = "f";
    private static final String                          TARGET_APPLICATION             = "target";
    private static final String                          _TARGET_APPLICATION            = "t";
    private static final String                          _ENCRYPTED_PASSWORD            = "e";
    private static final String                          ENCRYPTED_PASSWORD             = "encrypted.password";
    private static final String                          _TRUSTSTORE_LOCATION           = "ts";
    private static final String                          TRUSTSTORE_LOCATION            = "truststore.location";
    private static final String                          _TRUSTSTORE_PASSWORD           = "tsp";
    private static final String                          TRUSTSTORE_PASSWORD            = "truststore.password";
    private static final String                          _TRUSTSTORE_PASSWORD_FILE      = "tspf";
    private static final String                          TRUSTSTORE_PASSWORD_FILE       = "truststore.password.file";
    private static final String                          KEYSTORE_LOCATION              = "keystore.location";
    private static final String                          _KEYSTORE_LOCATION             = "ks";
    private static final String                          KEYSTORE_PASSWORD              = "keystore.password";
    private static final String                          _KEYSTORE_PASSWORD             = "ksp";
    private static final String                          KEYSTORE_PASSWORD_FILE         = "keystore.password.file";
    private static final String                          _KEYSTORE_PASSWORD_FILE        = "kspf";
    private static final String                          _PASSWORD_FILE_LOCATION        = "p";
    private static final String                          PASSWORD_FILE_LOCATION         = "password.file.location";
    private static final String                          _USER_PASSWORD_FILE_LOCATION   = "upf";
    private static final String                          USER_PASSWORD_FILE_LOCATION    = "user.password.file.location";
    private static final String                          _LIST_USERS                    = "l";
    private static final String                          LIST_USERS                     = "list";

    private static Option                                create;
    private static Option                                authenticate;

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
        Option listUsers = OptionBuilder.withLongOpt(LIST_USERS)
                .withDescription("List known users").create(_LIST_USERS);

        Option file = OptionBuilder.withLongOpt(FILE).withArgName("filename")
                .hasArgs()
                .withDescription("Location of the "
                        + SecurityConf.SECURITY_PROPERTIES_FILE_NAME + " file")
                .create(_FILE);

        // Mutually excluding options
        OptionGroup optionGroup = new OptionGroup();
        authenticate = OptionBuilder.withLongOpt(AUTHENTICATE).hasArgs(2)
                .withArgName("username> <password")
                .withDescription("Authenticates a user with given password")
                .create(_AUTHENTICATE);
        create = OptionBuilder.withLongOpt(CREATE).hasArgs(2)
                .withArgName("username> <password")
                .withDescription("Creates or Updates a user").create(_CREATE);
        Option delete = OptionBuilder.withLongOpt(DELETE)
                .withArgName("username").hasArgs()
                .withDescription("Deletes a user").create(_DELETE);

        optionGroup.addOption(listUsers);
        optionGroup.addOption(authenticate);
        optionGroup.addOption(create);
        optionGroup.addOption(delete);
        optionGroup.setRequired(true); // At least 1 command required

        Option targetApplication = OptionBuilder.withLongOpt(TARGET_APPLICATION)
                .withArgName("target").hasArgs()
                .withDescription("Target application: "
                        + getListOfClientApplicationType())
                .create(_TARGET_APPLICATION);

        // --- Options replacing parameters from security.properties ---
        Option encryptedPassword = OptionBuilder.withLongOpt(ENCRYPTED_PASSWORD)
                .withArgName("encrypt password")
                .withDescription("Encrypts the password")
                .create(_ENCRYPTED_PASSWORD);
        Option truststoreLocation = OptionBuilder
                .withLongOpt(TRUSTSTORE_LOCATION).withArgName("filename")
                .hasArg().withDescription("Location of the tuststore file")
                .create(_TRUSTSTORE_LOCATION);
        Option truststorePassword = OptionBuilder
                .withLongOpt(TRUSTSTORE_PASSWORD).withArgName("password")
                .hasArg().withDescription("Password for the truststore file")
                .create(_TRUSTSTORE_PASSWORD);
        Option truststorePasswordFile = OptionBuilder
                .withLongOpt(TRUSTSTORE_PASSWORD_FILE)
                .withArgName("truststore password file").hasArg()
                .withDescription(MessageFormat.format(
                        "Hidden password alternative to -{0}",
                        _TRUSTSTORE_PASSWORD))
                .create(_TRUSTSTORE_PASSWORD_FILE);
        Option keystoreLocation = OptionBuilder.withLongOpt(KEYSTORE_LOCATION)
                .withArgName("filename").hasArg()
                .withDescription("Location of the keystore file")
                .create(_KEYSTORE_LOCATION);
        Option keystorePassword = OptionBuilder.withLongOpt(KEYSTORE_PASSWORD)
                .withArgName("password").hasArg()
                .withDescription("Password for the keystore file")
                .create(_KEYSTORE_PASSWORD);
        Option keystorePasswordFile = OptionBuilder
                .withLongOpt(KEYSTORE_PASSWORD_FILE)
                .withArgName("password file").hasArg()
                .withDescription(MessageFormat.format(
                        "Hidden password alternative to -{0}",
                        _KEYSTORE_PASSWORD))
                .create(_KEYSTORE_PASSWORD_FILE);
        Option passwordFileLocation = OptionBuilder
                .withLongOpt(PASSWORD_FILE_LOCATION).withArgName("filename")
                .hasArg().withDescription("Location of the password file")
                .create(_PASSWORD_FILE_LOCATION);
        Option userPasswordFileLocation = OptionBuilder
                .withLongOpt(USER_PASSWORD_FILE_LOCATION)
                .withArgName("filename").hasArg()
                .withDescription("Location of the user password file")
                .create(_USER_PASSWORD_FILE_LOCATION);

        // --- Add options to the list ---
        // --- Help
        this.helpOptions.addOption(help);

        // --- Program command line options
        this.options.addOptionGroup(optionGroup);
        this.options.addOption(file);
        this.options.addOption(help);
        this.options.addOption(encryptedPassword);
        this.options.addOption(truststoreLocation);
        this.options.addOption(truststorePassword);
        this.options.addOption(truststorePasswordFile);
        this.options.addOption(keystoreLocation);
        this.options.addOption(keystorePassword);
        this.options.addOption(keystorePasswordFile);
        this.options.addOption(passwordFileLocation);
        this.options.addOption(userPasswordFileLocation);

        this.options.addOption(targetApplication);
    }

    /**
     * Creates a new <code>PasswordManager</code> object
     */
    public PasswordManagerCtrl()
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
        pwd = new PasswordManagerCtrl();

        // --- Options ---
        ClientApplicationType clientApplicationType = null;
        String securityPropertiesFileLocation = null;
        String username = null;
        String password = null;
        CommandLine line = null;

        try
        {
            CommandLineParser parser = new GnuParser();
            // --- Parse the command line arguments ---

            // --- Help
            line = parser.parse(pwd.helpOptions, argv, true);
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit(EXIT_CODE.EXIT_OK);
            }

            // --- Program command line options
            line = parser.parse(pwd.options, argv);

            // --- Handle options ---

            // --- Optional arguments : Get options ---
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit(EXIT_CODE.EXIT_OK);
            }
            if (line.hasOption(_TARGET_APPLICATION)) // Target Application
            {
                String target = line.getOptionValue(TARGET_APPLICATION);
                clientApplicationType = PasswordManagerCtrl
                        .getClientApplicationType(target);
            }
            if (line.hasOption(_FILE)) // security.properties file location
            {
                securityPropertiesFileLocation = line.getOptionValue(_FILE);
            }

            if (line.hasOption(_AUTHENTICATE))
            {
                String[] authenticateArgs = line.getOptionValues(_AUTHENTICATE);
                if (!line.hasOption(_USER_PASSWORD_FILE_LOCATION))
                {
                    // Make sure username + password are provided
                    if (authenticateArgs.length < 2)
                        throw new MissingArgumentException(authenticate);
                    else
                    {
                        // Credentials on command line
                        username = authenticateArgs[0];
                        password = authenticateArgs[1];
                    }
                }
                else
                {
                    // Credentials on command line and file
                    username = authenticateArgs[0];
                    pwd.userPasswordFileLocation = line
                            .getOptionValue(USER_PASSWORD_FILE_LOCATION);
                    password = null;
                }
            }

            if (line.hasOption(_CREATE))
            {
                String[] authenticateArgs = line.getOptionValues(_CREATE);
                if (!line.hasOption(_USER_PASSWORD_FILE_LOCATION))
                {
                    // Make sure username + password are provided
                    if (authenticateArgs.length < 2)
                        throw new MissingArgumentException(authenticate);
                    else
                    {
                        // Credentials on command line
                        username = authenticateArgs[0];
                        password = authenticateArgs[1];
                    }
                }
                else
                {
                    // Credentials on command line and file
                    username = authenticateArgs[0];
                    pwd.userPasswordFileLocation = line
                            .getOptionValue(USER_PASSWORD_FILE_LOCATION);
                    password = null;
                }
            }

            // --- Options to replace values in security.properties file ---
            if (line.hasOption(_ENCRYPTED_PASSWORD))
                pwd.useEncryptedPassword = true;
            if (line.hasOption(_TRUSTSTORE_LOCATION))
                pwd.truststoreLocation = line
                        .getOptionValue(_TRUSTSTORE_LOCATION);
            if (line.hasOption(_TRUSTSTORE_PASSWORD))
                pwd.truststorePassword = line
                        .getOptionValue(_TRUSTSTORE_PASSWORD);
            if (line.hasOption(_TRUSTSTORE_PASSWORD_FILE))
                pwd.truststorePasswordFileLocation = line
                        .getOptionValue(_TRUSTSTORE_PASSWORD_FILE);
            if (line.hasOption(_KEYSTORE_LOCATION))
                pwd.keystoreLocation = line.getOptionValue(_KEYSTORE_LOCATION);
            if (line.hasOption(_KEYSTORE_PASSWORD))
                pwd.keystorePassword = line.getOptionValue(_KEYSTORE_PASSWORD);
            if (line.hasOption(_KEYSTORE_PASSWORD_FILE))
                pwd.keystorePasswordFileLocation = line
                        .getOptionValue(_KEYSTORE_PASSWORD_FILE);
            if (line.hasOption(_PASSWORD_FILE_LOCATION))
                pwd.passwordFileLocation = line
                        .getOptionValue(_PASSWORD_FILE_LOCATION);

            try
            {
                pwd.passwordManager = new PasswordManager(
                        securityPropertiesFileLocation, clientApplicationType);

                AuthenticationInfo authenticationInfo = pwd.passwordManager
                        .getAuthenticationInfo();
                // --- Substitute with user provided options
                if (pwd.userPasswordFileLocation != null)
                    password = pwd
                            .getPassewordFromFile(pwd.userPasswordFileLocation);

                if (pwd.useEncryptedPassword != null)
                    authenticationInfo
                            .setUseEncryptedPasswords(pwd.useEncryptedPassword);
                if (pwd.truststoreLocation != null)
                    authenticationInfo
                            .setTruststoreLocation(pwd.truststoreLocation);
                // Truststore password
                if (pwd.truststorePasswordFileLocation != null)
                    authenticationInfo
                            .setTruststorePassword(pwd.getPassewordFromFile(
                                    pwd.truststorePasswordFileLocation));
                else if (pwd.truststorePassword != null)
                    authenticationInfo
                            .setTruststorePassword(pwd.truststorePassword);

                if (pwd.keystoreLocation != null)
                    authenticationInfo
                            .setKeystoreLocation(pwd.keystoreLocation);
                // Keystore password
                if (pwd.keystorePasswordFileLocation != null)
                    pwd.keystorePasswordFileLocation = pwd.getPassewordFromFile(
                            pwd.keystorePasswordFileLocation);
                else if (pwd.keystorePassword != null)
                    authenticationInfo
                            .setKeystorePassword(pwd.keystorePassword);

                // Target password file location
                if (pwd.passwordFileLocation != null)
                    authenticationInfo
                            .setPasswordFileLocation(pwd.passwordFileLocation);

                // --- Display summary of used parameters ---
                logger.info("Using parameters: ");
                logger.info("-----------------");
                if (pwd.userPasswordFileLocation != null)
                    logger.info(MessageFormat.format("{0} \t = {1}",
                            USER_PASSWORD_FILE_LOCATION,
                            pwd.userPasswordFileLocation));

                if (authenticationInfo
                        .getParentPropertiesFileLocation() != null)
                    logger.info(MessageFormat.format(
                            "security.properties \t\t = {0}", authenticationInfo
                                    .getParentPropertiesFileLocation()));
                logger.info(MessageFormat.format(
                        "password.file.location \t\t = {0}",
                        authenticationInfo.getPasswordFileLocation()));
                logger.info(
                        MessageFormat.format("encrypted.password \t\t = {0}",
                                authenticationInfo.isUseEncryptedPasswords()));

                // --- Keystore
                if (line.hasOption(_AUTHENTICATE))
                {
                    logger.info(
                            MessageFormat.format("keystore.location \t\t = {0}",
                                    authenticationInfo.getKeystoreLocation()));

                    // Keystore password from command line
                    if (pwd.keystorePasswordFileLocation == null)
                        logger.info(MessageFormat.format(
                                "keystore.password \t\t = {0}",
                                getHiddenPassword(authenticationInfo
                                        .getKeystorePassword())));
                    // Keystore password from file
                    else
                        logger.info(MessageFormat.format("{0} = {1}",
                                KEYSTORE_PASSWORD_FILE,
                                pwd.keystorePasswordFileLocation));
                }

                // --- Truststore
                if (authenticationInfo.isUseEncryptedPasswords())
                {
                    logger.info(MessageFormat.format(
                            "truststore.location \t\t = {0}",
                            authenticationInfo.getTruststoreLocation()));
                    // Truststore password from command line
                    if (pwd.truststorePasswordFileLocation == null)
                        logger.info(MessageFormat.format(
                                "truststore.password \t\t = {0}",
                                getHiddenPassword(authenticationInfo
                                        .getTruststorePassword())));
                    // Truststore password from file
                    else
                        logger.info(MessageFormat.format("{0} = {1}",
                                TRUSTSTORE_PASSWORD_FILE,
                                pwd.truststorePasswordFileLocation));
                }
                logger.info("-----------------");

                // --- AuthenticationInfo consistency check
                // Try to create files if possible
                pwd.passwordManager.try_createAuthenticationInfoFiles();
                authenticationInfo.checkAndCleanAuthenticationInfo();

            }
            catch (ConfigurationException ce)
            {
                logger.error(MessageFormat.format(
                        "Could not retrieve configuration information: {0}\nTry to specify a security.properties file location, provide options on the command line, or have the cluster.home variable set.",
                        ce.getMessage()));
                System.exit(EXIT_CODE.EXIT_ERROR.value);
            }
            catch (ServerRuntimeException sre)
            {
                logger.error(sre.getLocalizedMessage());
                // AuthenticationInfo consistency check : failed
                Exit(EXIT_CODE.EXIT_ERROR);
            }
            catch (Exception e)
            {
                logger.error(e);
                Exit(EXIT_CODE.EXIT_ERROR);
            }

            // --- Perform commands ---

            // ######### List users ##########
            if (line.hasOption(_LIST_USERS))
            {
                try
                {
                    logger.info("Listing users by application type:");
                    HashMap<String, List<String>> mapUsers = new HashMap<String, List<String>>();

                    // Get users for each application type
                    for (ClientApplicationType applicationType : ClientApplicationType
                            .values())
                    {
                        List<String> listUsernames = pwd.passwordManager
                                .listUsers(applicationType);
                        mapUsers.put(applicationType.name(), listUsernames);
                    }

                    // Display result
                    for (ClientApplicationType applicationType : ClientApplicationType
                            .values())
                    {
                        List<String> listUsers = mapUsers
                                .get(applicationType.name());
                        if (listUsers != null && !listUsers.isEmpty())
                        {
                            logger.info("\n");
                            logger.info(MessageFormat.format("[{0}]",
                                    applicationType.name().toLowerCase()));
                            logger.info("-----------");
                            for (String user : listUsers)
                            {
                                logger.info(user);
                            }
                        }

                    }
                    logger.info("\n");
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while listing users: {0}", e.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }
            }
            // ######### Authenticate ##########
            if (line.hasOption(_AUTHENTICATE))
            {
                try
                {
                    boolean authOK = pwd.passwordManager
                            .authenticateUser(username, password);
                    String msgAuthOK = (authOK) ? "SUCCESS" : "FAILED";
                    logger.info(MessageFormat.format(
                            "Authenticating  {0}:{1} = {2}", username, password,
                            msgAuthOK));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while authenticating user: {0}",
                            e.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }
            }
            // ######### Create ##########
            if (line.hasOption(_CREATE))
            {
                try
                {
                    pwd.passwordManager.setPasswordForUser(username, password);
                    logger.info(MessageFormat
                            .format("User created successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while creating user: {0}", e.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }
            }

            // ########## DELETE ##########
            else if (line.hasOption(_DELETE))
            {
                username = line.getOptionValue(_DELETE);

                try
                {
                    pwd.passwordManager.deleteUser(username);
                    logger.info(MessageFormat
                            .format("User deleted successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while deleting user: {0}", e.getMessage()));
                    Exit(EXIT_CODE.EXIT_ERROR);
                }
            }

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

        Exit(EXIT_CODE.EXIT_OK);
    }

    /**
     * Retrieve password from a 1 line / 1 word file
     * 
     * @param passwordFileLocation location of the file from which to retrieve
     *            the password
     * @return
     * @throws ConfigurationException
     */
    private String getPassewordFromFile(String passwordFileLocation)
            throws ConfigurationException
    {
        String password = null;
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new FileReader(passwordFileLocation));

            String line = null;
            if ((line = br.readLine()) != null)
                password = line;
        }
        catch (Exception e)
        {
            String errorMessage = MessageFormat.format(
                    "Could not read password from file: {0} -> {1}",
                    passwordFileLocation, e);
            throw new ConfigurationException(errorMessage);
        }
        finally
        {
            if (br != null)
            {
                try
                {
                    br.close();
                }
                catch (IOException e1)
                {
                    // Nothing to do, best effort
                }
            }
        }

        return password;
    }

    /**
     * Hide password characters by replacing them with a *
     * 
     * @param clearTextPassword the password to hide
     * @return
     */
    private static String getHiddenPassword(String clearTextPassword)
    {
        String hiddenPassword = null;

        if (clearTextPassword != null)
            hiddenPassword = clearTextPassword.replaceAll("(?s).", "*");
        return hiddenPassword;
    }

    /**
     * Get the list of ClientApplicationType as a string
     * 
     * @return String containing all possible lower-cased application types
     */
    private static String getListOfClientApplicationType()
    {
        String listApplicationType = "";

        for (ClientApplicationType appType : ClientApplicationType.values())
        {
            listApplicationType += ((listApplicationType.isEmpty() ? "" : " | ")
                    + appType.toString().toLowerCase());
        }
        listApplicationType.trim();

        return listApplicationType;
    }

    /**
     * Get the application type argument. Populates the
     * <code>clientApplicationType</code> property
     * 
     * @param commandLineTargetApplicationType the argument provided on the
     *            command line
     * @return a ClientApplicationType
     * @throws UnrecognizedOptionException if the cast could not be made
     */
    private static ClientApplicationType getClientApplicationType(
            String commandLineTargetApplicationType)
                    throws UnrecognizedOptionException
    {
        try
        {
            clientApplicationType = ClientApplicationType
                    .fromString(commandLineTargetApplicationType);
        }
        catch (IllegalArgumentException iae)
        {
            throw new UnrecognizedOptionException(MessageFormat.format(
                    "The target application type does not exist: {0}",
                    commandLineTargetApplicationType));
        }

        return clientApplicationType;
    }

    /**
     * Display the program help and exits
     */
    private static void DisplayHelpAndExit(EXIT_CODE exitCode)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp("tpasswd", pwd.options);
        Exit(exitCode);
    }

    private static void Exit(EXIT_CODE exitCode)
    {
        System.exit(exitCode.value);
    }

}
