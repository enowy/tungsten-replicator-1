#################################
# SECURITY.PROPERTIES           #
#################################

# Location of files used for security. 
security.dir=@{SECURITY_DIRECTORY}

# RMI + JMX authentication and encryption parameters
security.rmi.authentication=@{ENABLE_RMI_AUTHENTICATION}
security.rmi.authentication.username=@{RMI_USER}

security.rmi.tungsten.authenticationRealm.encrypted.password=true

security.rmi.encryption=@{ENABLE_RMI_SSL}

# Password and access file
security.password_file.location=@{JAVA_PASSWORDSTORE_PATH}
security.access_file.location=@{JAVA_JMXREMOTE_ACCESS_PATH}

# Keystore and Truststore for SSL and encryption
security.keystore.location=@{JAVA_KEYSTORE_PATH}
security.keystore.alias=@{JAVA_TLS_ENTRY_ALIAS}
security.keystore.password=@{JAVA_KEYSTORE_PASSWORD}
security.truststore.location=@{JAVA_TRUSTSTORE_PATH}
security.truststore.password=@{JAVA_TRUSTSTORE_PASSWORD}

# Minimize brute force attempts
# On a failed login attempt, a random sleep (ms) between min and max 
# in increment of increment.step will be done
security.randomWaitOnFailedLogin.min = 500
security.randomWaitOnFailedLogin.max = 1000
security.randomWaitOnFailedLogin.increment.step = 100

# Transport Protocols and Cipher Suites
# Used for RMI, SSL and HTTPS
# See: https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SUNProvider
# Example:
# protocols = TLSv1.1, TLSv1.2
# cipher suites = TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
# If either field is left empty use the default protocols and/or ciphers 
# for the JVM. 
security.enabled.protocols=TLSv1.1, TLSv1.2
security.enabled.cipher.suites=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDH_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_3DES_EDE_CBC_SHA

#################################
# CONNECTOR.SECURITY.PROPERTIES #
#################################
connector.security.use.ssl=@{ENABLE_CONNECTOR_CLIENT_SSL}
connector.security.keystore.location=@{JAVA_CONNECTOR_KEYSTORE_PATH}
connector.security.keystore.password=@{JAVA_CONNECTOR_KEYSTORE_PASSWORD}
connector.security.truststore.location=@{JAVA_CONNECTOR_TRUSTSTORE_PATH}
connector.security.truststore.password=@{JAVA_CONNECTOR_TRUSTSTORE_PASSWORD}

#####################################
# REST.API.SECURITY.PROPERTIES      #
#####################################

# Keystore and security information for the REST APIs
http.rest.api.security.ssl.useSsl = false
http.rest.api.security.authentication = false
http.rest.api.security.authentication.use.certificate=false
http.rest.api.security.authentication.use.encrypted.password = false

# Server keystore for https
http.rest.api.security.keystore.location = ${security.dir}/restapi_keystore.jks
http.rest.api.security.keystore.password = tungsten

# Server truststore: holds certificates of trusted client: used for certificate based authentication
# Client truststore: holds certificates of trusted servers: used for https
http.rest.api.security.truststore.location = ${security.dir}/restapi_truststore.ts
http.rest.api.security.truststore.password = tungsten

# Client keystore: holds private key for the client
# Used by the client for certificate based authentication
http.rest.api.security.client.keystore.location =  ${security.dir}/client.jks
http.rest.api.security.client.keystore.password = secret
