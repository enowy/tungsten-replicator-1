class ConfigureDeploymentHandler
  include ConfigureMessages
  
  ADDITIONAL_PROPERTIES_FILENAME = ".additional.cfg"
  
  def initialize()
    super()
    @config = Properties.new()
    @additional_properties = nil
  end
  
  def prepare(configs)
    reset_errors()
    configs.each{
      |config|
      
      begin
        prepare_config(config)
      rescue => e
        exception(e)
      end
    }
    
    is_valid?()
  end
  
  def prepare_config(config)
    @config.import(config)
    
    if (run_locally?() == false)
      if Configurator.instance.command.use_remote_package?()
        validation_temp_directory = get_validation_temp_directory()
        
        user = @config.getProperty(USERID)
        ssh_user = Configurator.instance.get_ssh_user(user)
        if user != ssh_user
          ssh_result("sudo -n chown -R #{ssh_user} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
        end
        
        if @config.getProperty(REMOTE_PACKAGE_PATH) == nil
          # Transfer validation code
          debug("Transfer validation tools to #{@config.getProperty(HOST)}")
          
          ssh_result("mkdir -p #{validation_temp_directory}/#{Configurator.instance.get_basename}; ls -al #{validation_temp_directory}; ls -al #{validation_temp_directory}/#{Configurator.instance.get_basename}", @config.getProperty(HOST), ssh_user)
          cmd_result("rsync -aze 'ssh #{Configurator.instance.get_ssh_command_options()}' --delete --exclude='tungsten-*/' --exclude='gossiprouter' --exclude='bristlecone' #{Configurator.instance.get_base_path()}/ #{ssh_user}@#{@config.getProperty(HOST)}:#{validation_temp_directory}/#{Configurator.instance.get_basename}")
          @config.setProperty(REMOTE_PACKAGE_PATH, "#{get_validation_temp_directory()}/#{Configurator.instance.get_basename()}")
        end
        
        # Transfer the MySQL/J file if it is being used
        if @config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH) != nil
          if File.file?(@config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH))
            debug("Transfer Connector/J to #{@config.getProperty(HOST)}")
            scp_result(@config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH), @config.getProperty(REPL_MYSQL_CONNECTOR_PATH), @config.getProperty(HOST), @config.getProperty(USERID))
          elsif Configurator.instance.is_locked?() == false
            error("Unable to transfer #{@config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH)} because it does not exist or is not a complete file name")
            return
          end
        end
        
        rmi_user = @config.getProperty(RMI_USER)
        ts_pass = @config.getProperty(JAVA_TRUSTSTORE_PASSWORD)
        ks_pass = @config.getProperty(JAVA_KEYSTORE_PASSWORD)
        
        generate_tls = true
        
        # Backwards compatible section allows for the use of 
        # --java-truststore-path and --java-keystore-path
        
        ts = @config.getProperty(JAVA_TRUSTSTORE_PATH)
        ks = @config.getProperty(JAVA_KEYSTORE_PATH)
        
        if ts != nil && ks == nil
          Configurator.instance.error("Both --java-truststore-path and --java-keystore-path must be given together or not at all.")
        end
        if ts == nil && ks != nil
          Configurator.instance.error("Both --java-truststore-path and --java-keystore-path must be given together or not at all.")
        end
        if ts != nil && ks != nil
          generate_tls = false
        end
        
        # New section allows for the use of 
        # --java-tls-key and --java-tls-certificate
        
        tls_keystore = @config.getProperty(JAVA_TLS_KEYSTORE_PATH)
        if tls_keystore != nil
          generate_tls = false
        end
        
        ###
        # Temporary section to generate a keystore and truststore
        # This should be replaced by generated a key and cert
        # to be imported on the target machine.
        ###
        if generate_tls == true
          generate_tls = false
          
          tls_ks = HostJavaTLSKeystorePath.build_keystore(
            config.getProperty(JAVA_TLS_ENTRY_ALIAS), ks_pass, ks_pass
          )
          local_tls_ks = Tempfile.new("tlssec")
          local_tls_ks.close()
          FileUtils.cp(tls_ks, local_tls_ks.path())
          
          config.include([HOSTS, config.getProperty([DEPLOYMENT_HOST])], {
            JAVA_TLS_KEYSTORE_PATH => "#{config.getProperty(TEMP_DIRECTORY)}/#{config.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(local_tls_ks.path())}",
            GLOBAL_JAVA_TLS_KEYSTORE_PATH => local_tls_ks.path()
          })
        end
        
        if @config.getProperty(JAVA_JGROUPS_KEYSTORE_PATH) == nil
          jgroups_ks = HostJavaJgroupsKeystorePath.build_keystore(
            config.getProperty(JAVA_JGROUPS_ENTRY_ALIAS), ks_pass, ks_pass
          )
          local_jgroups_ks = Tempfile.new("jgroupssec")
          local_jgroups_ks.close()
          FileUtils.cp(jgroups_ks, local_jgroups_ks.path())
            
          config.include([HOSTS, config.getProperty([DEPLOYMENT_HOST])], {
            JAVA_JGROUPS_KEYSTORE_PATH => "#{config.getProperty(TEMP_DIRECTORY)}/#{config.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(local_jgroups_ks.path())}",
            GLOBAL_JAVA_JGROUPS_KEYSTORE_PATH => local_jgroups_ks.path()
          })
        end
        
        
        DeploymentFiles.prompts.each{
          |p|
          if config.getProperty(p[:global]) != nil
            if File.exist?(config.getProperty(p[:global]))
              debug("Transfer #{File.basename(config.getProperty(p[:global]))} to #{config.getProperty(HOST)}")
              scp_result(config.getProperty(p[:global]), config.getProperty(p[:local]), config.getProperty(HOST), config.getProperty(USERID))
            elsif Configurator.instance.is_locked?() == false
              error("Unable to transfer #{File.basename(config.getProperty(p[:global]))} because it does not exist or is not a complete file name")
              return
            end
          end
        }

        debug("Transfer host configuration file to #{@config.getProperty(HOST)}")
        config_tempfile = Tempfile.new("tcfg")
        config_tempfile.close()
        config.store(config_tempfile.path())
        scp_result(config_tempfile.path(), "#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}", @config.getProperty(HOST), @config.getProperty(USERID))
        File.unlink(config_tempfile.path())
      
        if user != ssh_user
          ssh_result("sudo -n chown -R #{user} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
        
          gid = ssh_result("id -g #{ssh_user}", @config.getProperty(HOST), ssh_user)
          if gid != ""
            ssh_result("sudo -n chgrp #{gid} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
            ssh_result("sudo -n chmod g+w #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
          end
        end
        
        unless Configurator.instance.command.skip_prompts?()
          command = Escape.shell_command(["#{@config.getProperty(REMOTE_PACKAGE_PATH)}/tools/tpm", "load-config", "--profile=#{get_validation_temp_directory()}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}", "--command-class=#{Configurator.instance.command.class.name}"] + Configurator.instance.get_remote_tpm_options()).to_s
          result_dump = ssh_result(command, @config.getProperty(HOST), @config.getProperty(USERID), true)

          begin
            result = Marshal.load(result_dump)

            add_remote_result(result)
          rescue TypeError => te
            raise "Cannot read the load-single-config result: #{result_dump}"
          rescue ArgumentError => ae
            error("Unable to load the load-single-config result.  This can happen due to a bug in Ruby.  Try updating to a newer version and retry the installation.")
          end
        end
      end
    else
      unless Configurator.instance.command.skip_prompts?()
        prompt_handler = ConfigurePromptHandler.new(@config)
        debug("Validate configuration values for #{@config.getProperty(HOST)}")

        # Validate the values in the configuration file against the prompt validation
        prompt_handler.save_system_defaults()
        prompt_handler.validate()
        
        add_remote_result(prompt_handler.get_remote_result())
        output_property('props', @config.props.dup)
      end
    end
  end
  
  def set_additional_properties(config, additional_properties = nil)
    @config.import(config)
    
    if additional_properties == @additional_properties
      return
    end

    @additional_properties = additional_properties  
    if @additional_properties == nil || @additional_properties.size == 0
      return
    end
    
    if run_locally?()
      return
    else
      debug("Write a temporary additional properties file")
      config_tempfile = Tempfile.new("tcfg")
      config_tempfile.close()
      additional_properties.store(config_tempfile.path())
      
      if Configurator.instance.command.use_remote_package?()
        remote_additional_properties_filename = "#{get_validation_temp_directory()}/#{ADDITIONAL_PROPERTIES_FILENAME}"
      else
        remote_additional_properties_filename = "#{@config.getProperty(TEMP_DIRECTORY)}/#{ADDITIONAL_PROPERTIES_FILENAME}"
      end
      
      debug("Transfer additional properties file to #{@config.getProperty(HOST)}")
      scp_result(config_tempfile.path(), remote_additional_properties_filename, @config.getProperty(HOST), @config.getProperty(USERID))
      File.unlink(config_tempfile.path())
    end
  end
  
  def prepare_deploy_config(config)
    @config.import(config)
    
    if run_locally?()
      return
    else
      # The remote tools have already been copied, nothing left to do
      # if they are the only thing needed
      unless Configurator.instance.command.use_remote_tools_only?()
        # Only do this if we are using the temp directory for the remote
        # package.  Other options are an existing installation or pre-loaded 
        # software package
        if @config.getProperty(REMOTE_PACKAGE_PATH) == "#{get_validation_temp_directory()}/#{Configurator.instance.get_basename()}"
          validation_temp_directory = get_validation_temp_directory()
        
          # Transfer remaining code
          debug("Transfer binaries to #{@config.getProperty(HOST)}")
          user = @config.getProperty(USERID)
          ssh_user = Configurator.instance.get_ssh_user(user)
          if user != ssh_user
            ssh_result("sudo -n chown -R #{ssh_user} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
          end

          cmd_result("rsync -aze 'ssh #{Configurator.instance.get_ssh_command_options()}' --delete #{Configurator.instance.get_base_path()}/ #{ssh_user}@#{@config.getProperty(HOST)}:#{@config.getProperty(REMOTE_PACKAGE_PATH)}")
        
          if user != ssh_user
            ssh_result("sudo -n chown -R #{user} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)

            gid = ssh_result("id -g #{ssh_user}", @config.getProperty(HOST), ssh_user)
            if gid != ""
              ssh_result("sudo -n chgrp #{gid} #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
              ssh_result("sudo -n chmod g+w #{validation_temp_directory}", @config.getProperty(HOST), ssh_user)
            end
          end
        end
      end
    end
  end
  
  def deploy_config_group(config, deployment_method_class_name, deployment_method_group_id = nil)
    @config.import(config)
    
    if run_locally?()
      Configurator.instance.write ""
      Configurator.instance.debug "Local deploy #{deployment_method_class_name}:#{deployment_method_group_id} methods in #{@config.getProperty(HOME_DIRECTORY)}"
      
      result = Configurator.instance.command.get_deployment_object(config).run(deployment_method_class_name, deployment_method_group_id, @additional_properties)
      add_remote_result(result)
    else
      Configurator.instance.write ""
      Configurator.instance.write_header "Remote deploy #{deployment_method_class_name}:#{deployment_method_group_id} methods in #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
      
      if Configurator.instance.command.use_remote_package?()
        remote_additional_properties_filename = "#{get_validation_temp_directory()}/#{ADDITIONAL_PROPERTIES_FILENAME}"
        
        command = Escape.shell_command(["#{@config.getProperty(REMOTE_PACKAGE_PATH)}/tools/tpm", "deploy-single-config", "--profile=#{get_validation_temp_directory()}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}", "--command-class=#{Configurator.instance.command.class.name}", "--deployment-method-class=#{deployment_method_class_name}", "--run-group-id=#{deployment_method_group_id}", "--additional-properties=#{get_validation_temp_directory()}/#{ADDITIONAL_PROPERTIES_FILENAME}"] + Configurator.instance.get_remote_tpm_options()).to_s
      else
        remote_additional_properties_filename = "#{@config.getProperty(TEMP_DIRECTORY)}/#{ADDITIONAL_PROPERTIES_FILENAME}"
        
        command = Escape.shell_command(["#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm", "deploy-single-config", "--command-class=#{Configurator.instance.command.class.name}", "--deployment-method-class=#{deployment_method_class_name}", "--run-group-id=#{deployment_method_group_id}", "--additional-properties=#{remote_additional_properties_filename}"] + Configurator.instance.get_remote_tpm_options()).to_s
      end
      
      result_dump = ssh_result(command, @config.getProperty(HOST), @config.getProperty(USERID), true)
      
      begin
        result = Marshal.load(result_dump)
        
        add_remote_result(result)
      rescue TypeError => te
        raise "Cannot read the deployment result: #{result_dump}"
      rescue ArgumentError => ae
        error("Unable to load the deployment result.  This can happen due to a bug in Ruby.  Try updating to a newer version and retry the installation.")
      end
    end
  end
  
  def cleanup(configs)
    reset_errors()
    configs.each{
      |config|
      
      @config.import(config)
      begin
        if Configurator.instance.command.use_remote_package?()
          remote_additional_properties_filename = "#{get_validation_temp_directory()}/#{ADDITIONAL_PROPERTIES_FILENAME}"
        else
          remote_additional_properties_filename = "#{@config.getProperty(TEMP_DIRECTORY)}/#{ADDITIONAL_PROPERTIES_FILENAME}"
        end
        
        unless run_locally?()
          if Configurator.instance.command.use_remote_package?()
            ssh_result("rm -rf #{get_validation_temp_directory()}", 
              @config.getProperty(HOST), @config.getProperty(USERID))
          end
          
          ssh_result("rm -f #{remote_additional_properties_filename}", @config.getProperty(HOST), @config.getProperty(USERID))
        end
      rescue RemoteError
      end
    }
    
    is_valid?()
  end
  
  def run_locally?
    return Configurator.instance.is_localhost?(@config.getProperty(HOST)) && 
      Configurator.instance.whoami == @config.getProperty(USERID)
  end
  
  def get_validation_temp_directory
    "#{@config.getProperty(TEMP_DIRECTORY)}/#{@config.getProperty(CONFIG_TARGET_BASENAME)}/"
  end
  
  def get_message_hostname
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_message_host_key
    @config.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
  end
end