module ClusterSecurityFiles
  def create_default_security_files(config)
    if Configurator.instance.is_locked?()
      # We do not create these files from inside an installed directory
      return
    end
    
    generate_tls = true
    
    # Backwards compatible section allows for the use of 
    # --java-truststore-path and --java-keystore-path
    
    ts = config.getProperty(JAVA_TRUSTSTORE_PATH)
    ks = config.getProperty(JAVA_KEYSTORE_PATH)
    
    if ts != nil && ks == nil
      Configurator.instance.error("Both --java-truststore-path and --java-keystore-path must be given together or not at all.")
    end
    if ts == nil && ks != nil
      Configurator.instance.error("Both --java-truststore-path and --java-keystore-path must be given together or not at all.")
    end
    if ts != nil && ks != nil
      generate_tls = false
    end
    
    # New section allows for the use of --java-tls-keystore-path
    tls_keystore = config.getProperty(JAVA_TLS_KEYSTORE_PATH)
    if generate_tls == true
      if tls_keystore == nil
        generate_tls_certificate(config, generate_tls_certificate_warning())
      elsif tls_keystore == AUTOGENERATE
        generate_tls_certificate(config)
      end
    end
    
    jgroups_keystore = config.getProperty(JAVA_JGROUPS_KEYSTORE_PATH)
    if jgroups_keystore == nil
      generate_jgroups_certificate(config, generate_jgroups_certificate_warning())
    elsif jgroups_keystore == AUTOGENERATE
      generate_jgroups_certificate(config)
    end
  end
  
  def generate_tls_certificate_warning
    "Generating a certificate for SSL communication. Create your own JKS keystore for --java-tls-keystore-path, or set --java-tls-keystore-path=autogenerate to disable this warning."
  end
  
  def generate_tls_certificate(config, first_time_warning = nil)
    if config.getProperty(REPL_ENABLE_THL_SSL) != "true" && config.getProperty(ENABLE_RMI_SSL) != "true"
      # Do nothing because the TLS certificate is not used
      return
    end
    
    ks_pass = config.getProperty(JAVA_KEYSTORE_PASSWORD)
    lifetime = config.getProperty(JAVA_TLS_KEY_LIFETIME)
    
    tls_ks = HostJavaTLSKeystorePath.build_keystore(
      staging_temp_directory(),
      config.getProperty(JAVA_TLS_ENTRY_ALIAS), ks_pass, lifetime,
      first_time_warning
    )
    local_tls_ks = Tempfile.new("tlssec")
    local_tls_ks.close()
    local_tls_ks_path = "#{staging_temp_directory()}/#{File.basename(local_tls_ks.path())}"
    FileUtils.cp(tls_ks, local_tls_ks_path)
    
    config.override([HOSTS, config.getProperty([DEPLOYMENT_HOST])], {
      JAVA_TLS_KEYSTORE_PATH => "#{config.getProperty(TEMP_DIRECTORY)}/#{config.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(local_tls_ks_path)}",
      GLOBAL_JAVA_TLS_KEYSTORE_PATH => local_tls_ks_path
    })
  end
  
  def generate_jgroups_certificate_warning
    "Generating a certificate for JGroups communication. Create your own JCEKS keystore for --java-jgroups-keystore-path, or set --java-jgroups-keystore-path=autogenerate to disable this warning."
  end
  
  def generate_jgroups_certificate(config, first_time_warning = nil)
    if config.getProperty(ENABLE_JGROUPS_SSL) != "true"
      # Do nothing because the Jgroups certificate is not used
      return
    end
    
    ks_pass = config.getProperty(JAVA_KEYSTORE_PASSWORD)
    
    jgroups_ks = HostJavaJgroupsKeystorePath.build_keystore(
      staging_temp_directory(),
      config.getProperty(JAVA_JGROUPS_ENTRY_ALIAS), ks_pass, 
      first_time_warning
    )
    local_jgroups_ks = Tempfile.new("jgroupssec")
    local_jgroups_ks.close()
    local_jgroups_ks_path = "#{staging_temp_directory()}/#{File.basename(local_jgroups_ks.path())}"
    FileUtils.cp(jgroups_ks, local_jgroups_ks_path)
      
    config.override([HOSTS, config.getProperty([DEPLOYMENT_HOST])], {
      JAVA_JGROUPS_KEYSTORE_PATH => "#{config.getProperty(TEMP_DIRECTORY)}/#{config.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(local_jgroups_ks_path)}",
      GLOBAL_JAVA_JGROUPS_KEYSTORE_PATH => local_jgroups_ks_path
    })
  end
  
  def staging_temp_directory
    Configurator.instance.command.get_temp_directory()
  end
end