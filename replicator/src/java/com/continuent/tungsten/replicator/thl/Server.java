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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.PasswordManager;
import com.continuent.tungsten.common.security.SecurityConf;
import com.continuent.tungsten.common.security.SecurityHelper;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;
import com.continuent.tungsten.common.sockets.ServerSocketService;
import com.continuent.tungsten.common.sockets.SocketTerminationException;
import com.continuent.tungsten.common.sockets.SocketWrapper;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginLoader;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * This class defines a Server
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class Server implements Runnable
{
    private static Logger logger = Logger.getLogger(Server.class);

    /** Authentication succeeded. */
    public static int AUTH_OK = 1;

    /** Authentication failed because the user could not be found. */
    public static int AUTH_FAILED_NO_SUCH_USER = 2;

    /** Authentication failed because the user password did not match. */
    public static int AUTH_FAILED_BAD_PASSWORD = 3;

    /** Authentication failed for an unknown reason; see logs for more. */
    public static int AUTH_FAILED_UNKNOWN = 4;

    private PluginContext                         context;
    private Thread                                thd;
    private THL                                   thl;
    private String                                host;
    private int                                   port        = 0;
    private boolean                               useSSL;
    private ServerSocketService                   socketService;
    private LinkedList<ConnectorHandler>          clients     = new LinkedList<ConnectorHandler>();
    private LinkedBlockingQueue<ConnectorHandler> deadClients = new LinkedBlockingQueue<ConnectorHandler>();
    private volatile boolean                      stopped     = false;
    private String                                storeName;
    private PasswordManager                       passwordManager;

    /**
     * Creates a new <code>Server</code> object
     */
    public Server(PluginContext context, AtomicCounter sequencer, THL thl)
            throws ReplicatorException
    {
        this.context = context;
        this.thl = thl;
        this.storeName = thl.getName();

        String uriString = thl.getStorageListenerUri();
        URI uri;
        try
        {
            uri = new URI(uriString);

        }
        catch (URISyntaxException e)
        {
            throw new THLException("Malformed URI: " + uriString);
        }
        String protocol = uri.getScheme();
        if (THL.PLAINTEXT_URI_SCHEME.equals(protocol))
        {
            this.useSSL = false;
        }
        else if (THL.SSL_URI_SCHEME.equals(protocol))
        {
            this.useSSL = true;
        }
        else
        {
            throw new THLException("Unsupported scheme " + protocol);
        }
        host = uri.getHost();
        if ((port = uri.getPort()) == -1)
        {
            port = 2112;
        }
    }

    /** Returns true if this server requires encrypted connections. */
    public boolean isUseSSL()
    {
        return useSSL;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        try
        {
            SocketWrapper socket;
            while ((stopped == false)
                    && (socket = this.socketService.accept()) != null)
            {
                ConnectorHandler handler = (ConnectorHandler) PluginLoader
                        .load(context.getReplicatorProperties().getString(
                                ReplicatorConf.THL_PROTOCOL,
                                ReplicatorConf.THL_PROTOCOL_DEFAULT, false)
                                + "Handler");
                handler.configure(context);
                handler.setSocket(socket);
                handler.setServer(this);
                handler.setThl(thl);
                handler.prepare(context);

                clients.add(handler);
                handler.start();
                removeFinishedClients();
            }
        }
        catch (SocketTerminationException e)
        {
            if (stopped)
                logger.info("Server thread cancelled");
            else
                logger.info("THL server cancelled unexpectedly", e);
        }
        catch (IOException e)
        {
            logger.warn("THL server stopped by IOException; thread exiting", e);
        }
        catch (Throwable e)
        {
            logger.error("THL server terminated by unexpected error", e);
        }
        finally
        {
            // Close the connector handlers.
            logger.info("Closing connector handlers for THL Server: store="
                    + storeName);
            for (ConnectorHandler h : clients)
            {
                try
                {
                    h.stop();
                }
                catch (InterruptedException e)
                {
                    logger.warn(
                            "Connector handler close interrupted unexpectedly");
                }
                catch (Throwable t)
                {
                    logger.error("THL Server handler cleanup failed: store="
                            + storeName, t);
                }
            }

            // Remove finished clients.
            removeFinishedClients();
            if (clients.size() > 0)
            {
                logger.warn("One or more clients did not finish: "
                        + clients.size());
            }
            clients = null;

            // Close the socket.
            if (socketService != null)
            {
                logger.info("Closing socket: store=" + storeName + " host="
                        + socketService.getAddress() + " port="
                        + socketService.getLocalPort());
                try
                {
                    socketService.close();
                    socketService = null;
                }
                catch (Throwable t)
                {
                    logger.error("THL Server socket cleanup failed: store="
                            + storeName, t);
                }
            }
            logger.info("THL thread done: store=" + storeName);
        }
    }

    /**
     * Marks a client for removal.
     */
    public void removeClient(ConnectorHandler client)
    {
        deadClients.offer(client);
    }

    /**
     * Clean up terminated clients marked for removal.
     */
    private void removeFinishedClients()
    {
        ConnectorHandler client = null;
        while ((client = deadClients.poll()) != null)
        {
            try
            {
                client.release(context);
            }
            catch (Exception e)
            {
                logger.warn("Failed to release connector handler", e);

            }
            clients.remove(client);
        }
    }

    /**
     * Start up the THL server, which spawns a service thread.
     * 
     * @throws GeneralSecurityException
     * @throws ConfigurationException
     */
    public void start()
            throws IOException, GeneralSecurityException, ConfigurationException
    {
        // --- Retrieve and use SecurityInfo if needed ---
        String keystoreAlias = null;
        AuthenticationInfo authenticationInfo = null;
        if (useSSL)
        {
            authenticationInfo = SecurityHelper.loadAuthenticationInformation(
                    TUNGSTEN_APPLICATION_NAME.REPLICATOR);

            if (authenticationInfo != null)
            {
                keystoreAlias = authenticationInfo
                        .getKeystoreAliasForConnectionType(
                                SecurityConf.KEYSTORE_ALIAS_REPLICATOR_MASTER_TO_SLAVE);

                // Instantiate a password manager to authenticate clients.
                logger.info(
                        "Setting up password manager to authenticate encrypted connections");
                passwordManager = new PasswordManager(
                        authenticationInfo.getParentPropertiesFileLocation());
            }
        }

        logger.info("Opening THL server: store name=" + storeName + " host="
                + host + " port=" + port);

        socketService = new ServerSocketService();
        socketService.setAddress(new InetSocketAddress(host, port));
        socketService.setUseSSL(useSSL, keystoreAlias, authenticationInfo);
        socketService.bind();
        logger.info(
                "Opened socket: host=" + socketService.getAddress() + " port="
                        + socketService.getLocalPort() + " useSSL=" + useSSL);

        thd = new Thread(this,
                "THL Server [" + storeName + ":" + host + ":" + port + "]");
        thd.start();
    }

    /**
     * Stop the THL server, which cancels the service thread.
     */
    public void stop() throws InterruptedException
    {
        // Signal that the server thread should stop.
        stopped = true;
        if (thd != null)
        {
            try
            {
                logger.info("Stopping server thread");
                socketService.close();
                thd.interrupt();
                thd.join();
                thd = null;
            }
            catch (InterruptedException e)
            {
                logger.info("THL stop operation interrupted: " + e);
                throw e;
            }
        }
    }

    /**
     * Returns list of this server's clients.
     */
    public LinkedList<ConnectorHandler> getClients()
    {
        return clients;
    }

    /**
     * Return true if the proposed credentials are authenticated.
     * 
     * @param login A client login
     * @param password A client password
     * @return
     */
    public int authenticate(String login, String password)
    {
        // Make sure authentication is supported.
        if (!useSSL || passwordManager == null)
        {
            throw new UnsupportedOperationException(
                    "Attempt to authenticate unencrypted connection");
        }

        try
        {
            String expectedPassword = passwordManager
                    .getClearTextPasswordForUser(login);
            if (expectedPassword == null)
                return AUTH_FAILED_NO_SUCH_USER;
            else if (expectedPassword.equals(password))
                return AUTH_OK;
            else
                return AUTH_FAILED_BAD_PASSWORD;
        }
        catch (ConfigurationException e)
        {
            logger.warn(
                    "Unable to retrieve password for THL login: login=" + login,
                    e);
            return AUTH_FAILED_UNKNOWN;
        }
    }
}
