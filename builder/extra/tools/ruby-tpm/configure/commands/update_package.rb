class UpdateCommand
  include ConfigureCommand
  
  if Configurator.instance.is_locked?() == false
    include RemoteCommand
    include ResetBasenamePackageModule
    include ProvisionNewSlavesPackageModule
    include ClusterSecurityFiles
  end
  
  include ClusterCommandModule
  
  def get_command_name
    'update'
  end
  
  def validate_commit
    super()
   
    if @restart_replicators == false
      override_promotion_setting(RESTART_REPLICATORS, false)
    end
    if @restart_managers == false
      override_promotion_setting(RESTART_MANAGERS, false)
    end
    if @restart_connectors == false
      override_promotion_setting(RESTART_CONNECTORS, false)
    end
    
    is_valid?()
  end
  
  def prepare_config_for_command(config)
    config_type = config.getProperty(DEPLOYMENT_EXTERNAL_CONFIGURATION_TYPE)
    config_source = config.getProperty(DEPLOYMENT_EXTERNAL_CONFIGURATION_SOURCE)
    
    if @replace_tls_certificate == true
      if config_type.to_s() != ""
        error(get_replace_certificate_with_external_source_error("--replace-tls-certificate", config_source))
      else
        generate_tls_certificate(config)
      end
    end
    
    if @replace_jgroups_certificate == true
      if config_type.to_s() != ""
        error(get_replace_certificate_with_external_source_error("--replace-jgroups-certificate", config_source))
      else
        generate_jgroups_certificate(config)
      end
    end
  end

  def parsed_options?(arguments)
    @restart_replicators = nil
    @restart_managers = nil
    @restart_connectors = nil
    @replace_release = false
    @replace_jgroups_certificate = false
    @replace_tls_certificate = false
    
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
    
    if Configurator.instance.is_locked?()
      command_hosts(@config.getProperty([HOSTS, @config.getProperty([DEPLOYMENT_HOST]), HOST]))
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    
    # nil means to bypass all checks and set it to the default
    opts.on("--no-connectors") { @restart_connectors = false }
    
    if Configurator.instance.is_locked?()
      opts.on("--no-restart") { 
        @restart_replicators = false  
        @restart_managers = false
        @restart_connectors = false
      }
    else
      opts.on("--replace-release") { @replace_release = true }
    end
    
    opts.on("--replace-jgroups-certificate") { @replace_jgroups_certificate = true }
    opts.on("--replace-tls-certificate") { @replace_tls_certificate = true }
    
    opts = Configurator.instance.run_option_parser(opts, arguments)
    
    if @replace_jgroups_certificate == true
      if Configurator.instance.is_locked?()
        error(get_replace_certificate_from_installed_error("--replace-jgroups-certificate"))
      end
    end
    
    if @replace_tls_certificate == true
      if Configurator.instance.is_locked?()
        error(get_replace_certificate_from_installed_error("--replace-tls-certificate"))
      end
    end

    # Return options. 
    opts
  end
  
  def get_replace_certificate_from_installed_error(arg)
    return "The #{arg} option is not supported from an installed directory. This option may only be used from the staging directory to replace certificates across all hosts. Communication would be disrupted if the certificate is only replaced on a single host."
  end
  
  def get_replace_certificate_with_external_source_error(arg, source)
    return "The #{arg} option is not supported when using an external configuration source. All configuration changes must be made in #{source} before running `tpm update`."
  end
  
  def get_default_remote_package_path
    if @replace_release == true
      false
    else
      nil
    end
  end
  
  def enable_log?()
    true
  end

  def output_command_usage
    super()
    output_usage_line("--no-connectors", "Do not restart any connectors running on the server")
    if Configurator.instance.is_locked?()
      output_usage_line("--no-restart", "Do not restart any component on the server")
    else
      output_usage_line("--replace-release", "Replace the current release directory on each host with a copy of this directory")
      output_usage_line("--replace-tls-certificate", "Replace the certificate used for RMI and THL encryption")
      output_usage_line("--replace-jgroups-certificate", "Replace the certificate used for JGroups encryption")
    end
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def get_bash_completion_arguments
    super() + ["--no-connectors", "--no-restart", "--replace-release", "--replace-tls-certificate", "--replace-jgroups-certificate"] + get_cluster_bash_completion_arguments()
  end
  
  def output_completion_text
    output_cluster_completion_text()

    super()
  end
  
  def allow_check_current_version?
    true
  end
  
  def use_remote_package?
    true
  end
  
  def enable_release_notes_check?
    true
  end
  
  def self.get_command_name
    'update'
  end
  
  def self.get_command_aliases
    ['upgrade']
  end
  
  def self.get_command_description
    "Updates an existing installation of Tungsten.  If not arguments are specified, the local configuration is used to install.  If you specify --user, --hosts and --directory; this command will get the current configuration from each host and continue."
  end
end

module NotTungstenUpdatePrompt
  def enabled_for_command_line?
    if [ConfigureDataServiceCommand.name].include?(Configurator.instance.command.class.name) && Configurator.instance.is_locked?()
      return false
    end
    
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getNestedProperty([DEPLOYMENT_COMMAND]))
      return false
    else
      return (super() && true)
    end
  end
end

module NotTungstenUpdateCheck
  def enabled?
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getNestedProperty([DEPLOYMENT_COMMAND]))
      return false
    end
    
    if Configurator.instance.is_locked?
      return false
    end
    
    return (super() && true)
  end
end

module TungstenUpdateCheck
  def enabled?
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getProperty(DEPLOYMENT_COMMAND))
      return (super() && true)
    end
    
    return false
  end
end