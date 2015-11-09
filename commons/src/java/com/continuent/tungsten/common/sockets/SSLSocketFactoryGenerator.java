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

package com.continuent.tungsten.common.sockets;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.SecurityHelper;

/**
 * Implements an class to generate SSLSocketFactory instances. The author
 * gratefully acknowledges the blog article by Alexandre Saudate for providing
 * guidance in the implementation.
 * 
 * @see <a href=
 *      "http://alesaudate.wordpress.com/2010/08/09/how-to-dynamically-select-a-certificate-alias-when-invoking-web-services/">
 *      How to dynamically select a certificate alias when invoking web
 *      services</a>
 */
public class SSLSocketFactoryGenerator
{
    private static final Logger logger                               = Logger.getLogger(SSLSocketFactoryGenerator.class);

    private String     alias                                = null;
    private String     keystoreLocation                     = null;
    private String     trustStoreLocation                   = null;
    AuthenticationInfo securityPropertiesAuthenticationInfo = null;

    public SSLSocketFactoryGenerator(String alias,
            AuthenticationInfo securityPropertiesAuthenticationInfo)
            throws ConfigurationException
    {
        this.alias = alias;
        this.securityPropertiesAuthenticationInfo = securityPropertiesAuthenticationInfo;
        this.keystoreLocation = (securityPropertiesAuthenticationInfo != null)
                ? securityPropertiesAuthenticationInfo.getKeystoreLocation()
                : SecurityHelper.getKeyStoreLocation();
        this.trustStoreLocation = (securityPropertiesAuthenticationInfo != null)
                ? securityPropertiesAuthenticationInfo.getTruststoreLocation()
                : SecurityHelper.getTrustStoreLocation();
        this.checkConsistency();
    }

    /**
     * Ensure that prerequisites are met.
     * 
     * @throws ConfigurationException
     */
    private void checkConsistency() throws ConfigurationException
    {
        if (this.securityPropertiesAuthenticationInfo == null
                && this.alias != null) 
        {
            throw new ConfigurationException("\n\tError : "
                    + "Both securityPropertiesAuthenticationInfo and alias are "
                    + "null which makes it impossible to fetch the associated "
                    + "key from keyStore.");
        }
    }

    /**
     * Creates the SSLContext to be used when creating sockets or server
     * sockets. This uses the custom alias selector
     * 
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     * @throws ConfigurationException 
     */
    private SSLContext getSSLContext() throws IOException,
            GeneralSecurityException, ConfigurationException
    {
        KeyManager[] keyManagers = getKeyManagers();
        TrustManager[] trustManagers = getTrustManagers();

        // For each key manager, check if it is a X509KeyManager (because we
        // will override its //functionality
        for (int i = 0; i < keyManagers.length; i++)
        {
            if (keyManagers[i] instanceof X509KeyManager)
            {
                keyManagers[i] = new AliasSelectorKeyManager(
                        (X509KeyManager) keyManagers[i], alias);
            }
        }
        // Use the first protocol in SSLContext - the fact that there may be
        // multiple configured protocols is not handled here;
        // SSLContext.getInstance only takes one
        SSLContext context = SSLContext.getInstance(SecurityHelper
                .getProtocol());
        context.init(keyManagers, trustManagers, null);

        return context;
    }

    /**
     * Get an SSLSocketFactory with the custom alias selector
     * 
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     * @throws ConfigurationException 
     */
    public SSLSocketFactory getSSLSocketFactory()
            throws IOException, GeneralSecurityException, ConfigurationException
    {
        // --- No alias defined. Use default SSL socket factory ---
        if (this.alias == null || SecurityHelper.getProtocol() == null)
        {
            logger.debug("No keystore alias entry defined. Will use default "
                    + "SSLSocketFactory selecting 1st entry in keystore !");
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        // --- Alias defined. Use custom alias selector ---
        else
        {
            SSLContext context = this.getSSLContext();
            SSLSocketFactory ssf = context.getSocketFactory();
            return ssf;
        }
    }
    
    /**
     * Get an SSLServerSocketFactory with the custom alias selector
     * 
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     * @throws ConfigurationException 
     */
    public SSLServerSocketFactory getSSLServerSocketFactory() throws IOException,
            GeneralSecurityException, ConfigurationException
    {
        // --- No alias defined. Use default SSL socket factory ---
        if (this.alias == null)
        {
            logger.debug("No keystore alias entry defined. Will use default SSLServerSocketFactory selecting 1st entry in keystore !");
            return (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        }
        // --- Alias defined. Use custom alias selector ---
        else
        {
            SSLContext context = this.getSSLContext();
            SSLServerSocketFactory ssf = context.getServerSocketFactory();
            return ssf;
        }
    }

    public String getKeyStorePassword()
    {
        return this.securityPropertiesAuthenticationInfo.getKeystorePassword();
    }

    public String getTrustStorePassword()
    {
        return this.securityPropertiesAuthenticationInfo
                .getTruststorePassword();
    }

    public String getKeyStore()
    {
        return keystoreLocation;
    }

    public String getTrustStore()
    {
        return trustStoreLocation;
    }

    private KeyManager[] getKeyManagers()
            throws IOException, GeneralSecurityException
    {
        // Init a key store with the given file.

        String alg = KeyManagerFactory.getDefaultAlgorithm();
        KeyManagerFactory kmFact = KeyManagerFactory.getInstance(alg);

        FileInputStream fis = new FileInputStream(getKeyStore());
        KeyStore ks = KeyStore.getInstance("jks");
        ks.load(fis, getKeyStorePassword().toCharArray());
        fis.close();

        // Init the key manager factory with the loaded key store
        kmFact.init(ks, getKeyStorePassword().toCharArray());
        KeyManager[] kms = kmFact.getKeyManagers();
        return kms;
    }

    protected TrustManager[] getTrustManagers()
            throws IOException, GeneralSecurityException
    {
        String alg = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmFact = TrustManagerFactory.getInstance(alg);

        FileInputStream fis = new FileInputStream(getTrustStore());
        KeyStore ks = KeyStore.getInstance("jks");
        ks.load(fis, getTrustStorePassword().toCharArray());
        fis.close();

        tmFact.init(ks);

        TrustManager[] tms = tmFact.getTrustManagers();
        return tms;
    }
}
