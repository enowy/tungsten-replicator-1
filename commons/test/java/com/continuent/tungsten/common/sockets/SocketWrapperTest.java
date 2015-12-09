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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.common.sockets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.ssl.SSLHandshakeException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.SecurityConf;
import com.continuent.tungsten.common.security.SecurityHelper;

import org.junit.Assert;

/**
 * Implements a test of client and server socket wrappers using SSL and non-SSL
 * connections.
 * <p/>
 * IMPORTANT NOTE! To run this test in Eclipse set the working directory to
 * commons/build/work. Otherwise you won't be able to find the
 * sample.security.properties file.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SocketWrapperTest
{
    private static Logger logger            = Logger.getLogger(SocketWrapperTest.class);
    private EchoServer    server;
    private SocketHelper  helper            = new SocketHelper();

    Level                 debugLevel_socket = null;

    /**
     * Make sure that tests are terminated if prerequirements are not met.
     * 
     * @throws ConfigurationException
     */
    @Before
    public void checkConfiguration() throws ConfigurationException
    {
        if (!System.getProperty("user.dir").endsWith("commons/build/work"))
        {
            throw new ConfigurationException("\n\tInvalid working directory : "
                    + System.getProperty("user.dir") + ".\n"
                    + "\tWorking directory must be "
                    + "../commons/build/work in order to test work.\n"
                    + "\tHINT: in Eclipse, set working directory to "
                    + "${workspace_loc:commons/build/work} in arguments.");
        }
    }

    /**
     * Terminate echo server if still running.
     * 
     * @throws ClassNotFoundException
     */
    @After
    public void teardown()
    {
        if (server != null)
        {
            logger.info("Shutting down echo server...");
            server.shutdown();
        }
    }

    /**
     * Verify that we can connect using a non-SSL socket and get a value back
     * from a server.
     */
    @Test
    public void testNonSSLConnection() throws Exception
    {
        logger.info("### testNonSSLConnection");
        verifyConnection(2113, false, null, null, null, false);
    }

    /**
     * Verify that we can connect using an SSL socket and get a value back from
     * a server that also speaks SSL.
     */
    @Test
    public void testSSLConnection() throws Exception
    {
        logger.info("### testSSLConnection");
        AuthenticationInfo securityInfo = helper.loadSecurityProperties();
        verifyConnection(2114, true, null, null, securityInfo, null, false);
    }

    /**
     * Verify that we can connect using an SSL socket and a predefined alias
     * inside the keystore and get a value back from a server that also speaks
     * SSL.
     */
    @Test
    public void testSSLConnection_keystoreWithAlias() throws Exception
    {
        logger.info("### testSSLConnection with alias selection");
        AuthenticationInfo securityInfo = helper
                .loadSecurityProperties_keystoreWithAlias();
        String server_masterAlias = securityInfo
                .getKeystoreAliasForConnectionType(SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);
        try
        {
            verifyConnection(2114, true, server_masterAlias,
                    server_masterAlias, securityInfo, securityInfo, false);
        }
        catch (Exception e)
        {
            assertFalse("No exception should have been thrown", true);
        }

    }

    /**
     * Verify that when specifying a wrong server alias, the server cannot
     * start. This confirms that the alias selection works
     */
    @Test
    public void testSSLConnection_keystoreWithAlias_wrong_server_alias()
            throws ConfigurationException, Exception
    {
        logger.info("### testSSLConnection with alias selection: wrong server alias");
        AuthenticationInfo securityInfo = helper
                .loadSecurityProperties_keystoreWithAlias();
        String client_slaveAlias = securityInfo
                .getKeystoreAliasForConnectionType(SecurityConf
                        .KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);
        try
        {
            // Change debug level so that trace messages are shown
            debugLevel_socket = LogManager
                    .getLogger(
                            Class.forName("com.continuent.tungsten.common"
                                    + ".sockets.AliasSelectorKeyManager"))
                    .getLevel();
            LogManager
                    .getLogger(
                            Class.forName("com.continuent.tungsten.common"
                                    + ".sockets.AliasSelectorKeyManager"))
                    .setLevel(Level.TRACE);

            verifyConnection(
                    2114,
                    true,
                    "alias_that_does_not_exist_but it's ok it's the expected result",
                    client_slaveAlias, securityInfo, securityInfo, true);
            assertTrue(
                    "The server should not have started: we used a non existing "
                    + "alias in the keystore",
                    false);
        }
        catch (SSLHandshakeException e)
        {
            assertTrue(
                    "This exception is expected as we're trying to use a non "
                    + "existant server alias in the keystore",
                    true);
        }
        finally
        {
            LogManager
                    .getLogger(
                            Class.forName("com.continuent.tungsten.common.sockets.AliasSelectorKeyManager"))
                    .setLevel(debugLevel_socket);
        }
    }

    /**
     * Verify that a non-SSL connection fails to connect to an SSL server.
     */
    @Test
    public void testSSLConnectionIncompatibility() throws Exception
    {
        // Start an SSL server.
        logger.info("### testSSLConnectionIncompatibility");
        helper.loadSecurityProperties();
        server = new EchoServer("127.0.0.1", 2115, true, null, null, true);
        server.start();

        // Connect with non-SSL socket.
        ClientSocketWrapper clientWrapper = new ClientSocketWrapper();
        clientWrapper.setAddress(new InetSocketAddress("127.0.0.1", 2115));
        clientWrapper.connect();

        // Write data to the server.
        Socket sock = clientWrapper.getSocket();
        OutputStream os = sock.getOutputStream();
        byte[] buf1 = "This is data".getBytes();
        os.write(buf1, 0, buf1.length);

        // Inquire as to the echo server's health. It should not be good.
        Throwable serverError = null;
        for (int i = 0; i < 10; i++)
        {
            serverError = server.getThrowable();
            if (serverError != null)
                break;
            else
                Thread.sleep(500);
        }
        Assert.assertNotNull("Server should have error", serverError);
        logger.info("Found expected server error: " + serverError.toString());

        clientWrapper.close();
    }

    /**
     * Verify that multiple non-SSL clients can connect to the server and that
     * the server can stop when the clients are idle.
     */
    @Test
    public void testNonSSLClientsBasic() throws Exception
    {
        logger.info("### testNonSSLClientsBasic");
        verifyClients(2116, 5, false, false);
    }

    /**
     * Verify that multiple SSL clients can connect to the server and that the
     * server can stop when the clients are idle.
     */
    @Test
    public void testSSLClientsBasic() throws Exception
    {
        logger.info("### testSSLClientsBasic");
        AuthenticationInfo securityInfo = helper.loadSecurityProperties();
        verifyClients(2117, 5, true, securityInfo, false);
    }

    /**
     * Implement verification using a simple echo server.
     */
    private void verifyConnection(int port, boolean useSSL,
            String serverKeystoreAlias, String clientKeystoreAlias,
            AuthenticationInfo securityInfo, boolean silentFail)
            throws Exception
    {
        verifyConnection(port, useSSL, serverKeystoreAlias,
                clientKeystoreAlias, securityInfo, null, silentFail);
    }

    private void verifyConnection(int port, boolean useSSL,
            String serverKeystoreAlias, String clientKeystoreAlias,
            AuthenticationInfo securityInfo,
            AuthenticationInfo serverSecurityInfo, boolean silentFail)
            throws Exception, ConfigurationException
    {
        server = new EchoServer("127.0.0.1", port, useSSL, serverKeystoreAlias,
                serverSecurityInfo, silentFail);
        server.start();

        ClientSocketWrapper clientWrapper = new ClientSocketWrapper();
        clientWrapper.setUseSSL(useSSL);

        if (useSSL)
        {
            Assert.assertTrue(securityInfo != null);
        }
        clientWrapper.setAddress(new InetSocketAddress("127.0.0.1", port));
        clientWrapper.connect();

        Socket sock = clientWrapper.getSocket();
        Assert.assertNotNull("Expected to get a client socket", sock);

        String echoValue = helper.echo(sock, "hello");
        logger.info("Echoed value: " + echoValue);
        Assert.assertEquals("Expect echo to match", "hello", echoValue);
        clientWrapper.close();
    }

    public void verifyClients(int port, int numberOfClients, boolean useSSL,
            boolean silentFail) throws Exception
    {
        verifyClients(port, numberOfClients, useSSL, null, silentFail);
    }

    /**
     * Verify that multiple clients can connect to the server and that the
     * server can stop when the clients are idle.
     */
    public void verifyClients(int port, int numberOfClients, boolean useSSL,
            AuthenticationInfo securityInfo, boolean silentFail)
            throws Exception
    {
        // Start a server.
        server = new EchoServer("127.0.0.1", port, useSSL, null, null,
                silentFail);
        server.start();

        // Launch echo clients with 100ms think time between
        // requests.
        EchoClient[] clients = new EchoClient[numberOfClients];

        if (useSSL)
        {
            Assert.assertTrue(securityInfo != null);
        }

        for (int i = 0; i < clients.length; i++)
        {
            EchoClient client = new EchoClient("127.0.0.1", port, useSSL, 100);
            client.setEnabledCiphers(SecurityHelper.getCiphers());
            client.setEnabledProtocols(SecurityHelper.getProtocols());
            client.start();
            clients[i] = client;
        }

        // Bide a wee.
        Thread.sleep(5000);

        // Stop the threads and confirm that each has processed at least 10 echo
        // requests.
        try
        {
            for (EchoClient client : clients)
            {
                // Ensure we can shut down the client.
                Assert.assertTrue("Shut down client: " + client.getName(),
                        client.shutdown());
                Assert.assertTrue("Expect least 10 operations per client: "
                        + client.getName(), client.getEchoCount() >= 10);
                Assert.assertNull(
                        "Do not expect errors for client: " + client.getName(),
                        client.getThrowable());
            }
        }
        finally
        {
            // Shut down all clients.
            for (EchoClient client : clients)
                client.shutdown();
        }

        // Ensure the echo server is OK.
        Assert.assertNull("Echo server does not have any errors",
                server.getThrowable());
    }
}
