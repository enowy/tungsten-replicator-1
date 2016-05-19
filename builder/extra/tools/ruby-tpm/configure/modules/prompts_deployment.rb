class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts", "HOST")
      
    ClusterHostPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
  
  def update_deprecated_keys()
    each_member{
      |member|
      
      @config.setProperty([HOSTS, member, 'shell_startup_script'], nil)
    }
    
    super()
  end
end

module ClusterHostPrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_command_line_aliases
    super() + super().collect{|al| al.gsub("repl-", "")} + [@name.gsub("_", "-")]
  end
  
  def get_userid
    @config.getProperty(get_member_key(USERID))
  end
  
  def get_hostname
    @config.getProperty(get_member_key(HOST))
  end
  
  def get_host_alias
    get_member()
  end
  
  def allow_group_default
    true
  end
  
  def get_hash_prompt_key
    ds_alias = @config.getNestedProperty([DEPLOYMENT_DATASERVICE])
    if ds_alias.to_s() == ""
      ds_alias = @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
    end
    
    return [DATASERVICE_HOST_OPTIONS, ds_alias, @name]
  end
end

class DeploymentServicePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_SERVICE, "Deployment Service", 
      PV_ANY)
  end
end

class DeploymentCommandPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoStoredConfigValue
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DEPLOYMENT_COMMAND, "Current command being run", PV_ANY)
  end
end

class DeploymentExternalConfigurationTypePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DEPLOYMENT_EXTERNAL_CONFIGURATION_TYPE, "", PV_ANY)
  end
end

class DeploymentExternalConfigurationSourcePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DEPLOYMENT_EXTERNAL_CONFIGURATION_SOURCE, "", PV_ANY)
  end
end

class DeploymentConfigurationKeyPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DEPLOYMENT_CONFIGURATION_KEY, "", PV_ANY)
  end
end

class StagingHost < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_HOST, "Host being used to install", PV_ANY)
  end
end

class StagingUser < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_USER, "User being used to install", PV_ANY)
  end
end

class StagingDirectory < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_DIRECTORY, "Directory being used to install", PV_ANY)
  end
end

class DeploymentHost < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_HOST, "Host alias for the host to be deployed here", PV_ANY)
  end
end

class RemotePackagePath < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoStoredServerConfigValue
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(REMOTE_PACKAGE_PATH, "Path on the server to use for running tpm commands", PV_FILENAME)
  end
end

class ConfigTargetBasenamePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(CONFIG_TARGET_BASENAME, "", 
      PV_ANY)
  end
  
  def load_default_value
    if "#{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME}/#{Configurator.instance.get_basename()}" == Configurator.instance.get_base_path()
      @default = Configurator.instance.get_basename()
    elsif Configurator.instance.is_locked?()
      @default = Configurator.instance.get_basename()
    else
      @default = Configurator.instance.get_unique_basename()
    end
  end
end

class HostPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST, "DNS hostname", PV_HOSTNAME)
  end
  
  def load_default_value
    @default = get_member()
  end
  
  def allow_group_default
    false
  end
  
  def enabled_for_command_line?
    true
  end
  
  def build_command_line_argument?(member, v)
    if to_identifier(v) == member
      false
    else
      true
    end
  end
end

class UserIDPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(USERID, "System User", 
      PV_ANY, Configurator.instance.whoami())
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('userid'))
    super()
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(HOME_DIRECTORY, "Installation directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("install-directory")
  end
  
  def load_default_value
    @default = Configurator.instance.get_continuent_root()
  end
  
  def validate_value(value)
    super(value)
    
    if value == Configurator.instance.get_base_path()
      error("You must specify a separate location to deploy Continuent Tungsten")
    end
  end
end

class BaseDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(CURRENT_RELEASE_DIRECTORY, "Directory for the latest release", PV_FILENAME)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)) == Configurator.instance.get_base_path()
      @default = @config.getProperty(get_member_key(HOME_DIRECTORY))
    else
      @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
    end
  end
  
  def enabled_for_command_line?()
    false
  end
end

class ReleasesDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(RELEASES_DIRECTORY, "Directory for storing releases", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}"
  end
end

class LogsDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(LOGS_DIRECTORY, "Directory for storing logs", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{LOGS_DIRECTORY_NAME}"
  end
end

class MetadataDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(METADATA_DIRECTORY, "Directory for storing metadata", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{METADATA_DIRECTORY_NAME}"
  end
end

class ConfigDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(CONFIG_DIRECTORY, "Directory for storing the current config", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{CONFIG_DIRECTORY_NAME}"
  end
end

class TempDirectoryPrompt < ConfigurePrompt
  include AdvancedPromptModule
  include ClusterHostPrompt
  include NoConnectorRestart
  include NoReplicatorRestart
  include NoManagerRestart
  
  def initialize
    super(TEMP_DIRECTORY, "Temporary Directory",
      PV_FILENAME, "/tmp")
  end
  
  def load_default_value
    if @default != nil
      return
    end
    
    if get_member() == DEFAULTS
      return
    end
    
    ["TMPDIR", "TMP", "TEMP"].each{|var|      
      begin
        val = ssh_result("echo $#{var}", @config.getProperty(get_member_key(HOST)), @config.getProperty(get_member_key(USERID)))
        if val != ""
          @default = val
        end
      rescue RemoteCommandError
      rescue CommandError
      rescue MessageError
      end
    }
    
    if @default.to_s == ""
      @default = "/tmp"
    end
    
    @config.setProperty([SYSTEM] + get_name().split('.'), @default)
  end
  
  def get_timeout_length
    20
  end
end

class TargetDirectoryPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  
  def initialize
    super(TARGET_DIRECTORY, "Target for committing the deployment",
      PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}/#{@config.getProperty(get_member_key(TARGET_BASENAME))}"
  end
end

class TargetBasenamePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  
  def initialize
    super(TARGET_BASENAME, "Target for writing the deployment",
      PV_ANY)
  end
  
  def load_default_value
    if (@default = @config.getProperty(CONFIG_TARGET_BASENAME))
      return
    end
    
    @default = File.basename(Configurator.instance.get_base_path())
  end
end

class PrepareDirectoryPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  include NoSystemDefault
  
  def initialize
    super(PREPARE_DIRECTORY, "Target for preparing the deployment",
      PV_FILENAME)
  end
  
  def get_default_value
    target_directory = @config.getProperty(get_member_key(TARGET_DIRECTORY))
    if target_directory != nil && File.exist?(target_directory)
      return target_directory
    else
      return "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}/#{PREPARE_RELEASE_DIRECTORY}/#{@config.getProperty(get_member_key(TARGET_BASENAME))}"
    end
  end
end

class JavaMemorySize < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include MigrateFromReplicationServices
  
  def initialize
    super(REPL_JAVA_MEM_SIZE, "Replicator Java heap memory size in Mb (min 128)",
      PV_INTEGER, 1024)
  end
end

class JavaFileEncoding < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include MigrateFromReplicationServices
  
  def initialize
    super(REPL_JAVA_FILE_ENCODING, "Java platform charset (esp. for heterogeneous replication)",
      PV_ANY, "")
  end
  
  def required?
    false
  end
  
  def get_default_value
    has_heterogenous_service = false
    @config.getPropertyOr([REPL_SERVICES], {}).keys().each{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getProperty([REPL_SERVICES, rs_alias, ENABLE_HETEROGENOUS_MASTER]) == "true"
        has_heterogenous_service = true
      end
      
      if @config.getProperty([REPL_SERVICES, rs_alias, ENABLE_HETEROGENOUS_SLAVE]) == "true"
        has_heterogenous_service = true
      end
    }
    
    if has_heterogenous_service == true
      @default = "UTF8"
    else
      super()
    end
  end
end

class JavaUserTimezone < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include MigrateFromReplicationServices

  def initialize
    super(REPL_JAVA_USER_TIMEZONE, "Java VM Timezone (esp. for cross-site replication)",
      PV_ANY, "")
  end
 
  def required?
    false
  end
end

class ReplicationAPI < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API, "Enable the replication API", PV_BOOLEAN, "false")
  end
  
  def get_template_value
    if get_value() == "true"
      ""
    else
      "#"
    end
  end
end

class ReplicationAPIHost < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_HOST, "Hostname that the replication API should listen on", PV_HOSTNAME, "localhost")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPort < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_PORT, "Port that the replication API should bind to", PV_INTEGER, "19999")
  end

  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIUser < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_USER, "HTTP basic auth username for the replication API", PV_ANY, "tungsten")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPassword < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include PrivateArgumentModule

  def initialize
    super(REPL_API_PASSWORD, "HTTP basic auth password for the replication API", PV_ANY, "secret")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class DirectoryLockPrompt < ConfigurePrompt
  include ConstantValueModule
  include ClusterHostPrompt
  include NoSystemDefault
  
  def initialize
    super(DIRECTORY_LOCK_FILE, "Filename for locking a directory from further configuration",
      PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(PREPARE_DIRECTORY))}/#{DIRECTORY_LOCK_FILENAME}"
  end
end

class HostEnableManager < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_MANAGER, "Enable the manager", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = false.to_s
    else
      @config.getPropertyOr([MANAGERS], {}).each_key{
        |rs_alias|
        
        if rs_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([MANAGERS, rs_alias, DEPLOYMENT_HOST]) == get_member()
          @default = true.to_s
        end
      }
    end
  end
end

class HostEnableReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_REPLICATOR, "Enable the replicator", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = false.to_s
    else
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        
        if rs_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
          @default = true.to_s
        end
      }
    end
  end
end

class HostEnableConnector < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_CONNECTOR, "Enable the connector", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = true.to_s
    else
      @config.getPropertyOr([CONNECTORS], {}).each_key{
        |h_alias|
        
        if h_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST]) == get_member()
          @default = true.to_s
        end
      }
    end
  end
end

class HostDefaultDataserviceName < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "Name of the default dataservice for this host", PV_ANY)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    @default = DEFAULT_SERVICE_NAME
    
    non_cluster_ds_alias = nil
    @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
        ds_alias = @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
        if ds_alias != ""
          topology = Topology.build(ds_alias, @config)
          unless topology.enabled?()
            next
          end
          
          if topology.use_management?()
            @default = ds_alias
            return
          else
            non_cluster_ds_alias = ds_alias
          end
        end
      end
    }
    
    if Configurator.instance.is_enterprise?()
      @config.getNestedPropertyOr([MANAGERS], {}).each_key{
        |m_alias|
        if m_alias == DEFAULTS
          next
        end
      
        if @config.getNestedProperty([MANAGERS, m_alias, DEPLOYMENT_HOST]) == get_member()
          ds_alias = @config.getNestedProperty([MANAGERS, m_alias, DEPLOYMENT_DATASERVICE])
          if ds_alias != ""
            @default = ds_alias
            return
          end
        end
      }
    
      @config.getNestedPropertyOr([CONNECTORS], {}).each_key{
        |h_alias|
        if h_alias == DEFAULTS
          next
        end
      
        if @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST]) == get_member()
          ds_aliases = @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_DATASERVICE])
          if ! ds_aliases.kind_of?(Array)
             ds_aliases=Array(ds_aliases);
          end
          if ds_aliases.size() > 0
            @default = ds_aliases.at(0)
            return
          end
        end
      }
    end
    
    if non_cluster_ds_alias != nil
      @default = non_cluster_ds_alias
      return
    end
  end
end

class HostDataserviceAliases < ConfigurePrompt
  include ClusterHostPrompt
  include HiddenValueModule
  include NoSystemDefault
  
  def initialize
    super(DEPLOYMENT_DATASERVICE_ALIASES, "Aliases for all dataservices on this host", PV_ANY)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    @default = {}
    
    @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
        ds_alias = @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
        service_name = @config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
        @default[service_name] = ds_alias
      end
    }
    
    @config.getNestedPropertyOr([MANAGERS], {}).each_key{
      |m_alias|
      if m_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([MANAGERS, m_alias, DEPLOYMENT_HOST]) == get_member()
        ds_alias = @config.getNestedProperty([MANAGERS, m_alias, DEPLOYMENT_DATASERVICE])
        service_name = @config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
        @default[service_name] = ds_alias
      end
    }
    
    @config.getNestedPropertyOr([CONNECTORS], {}).each_key{
      |h_alias|
      if h_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST]) == get_member()
        ds_aliases = @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_DATASERVICE])
        if ! ds_aliases.kind_of?(Array)
           ds_aliases=Array(ds_aliases);
        end
        ds_aliases.each{|ds_alias|
          service_name = @config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          @default[service_name] = ds_alias
        }
      end
    }
  end
end

class DatasourceDBType < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    validator = PropertyValidator.new(ConfigureDatabasePlatform.get_types().join("|"), 
      "Value must be #{ConfigureDatabasePlatform.get_types().join(',')}")
      
    super(REPL_DBTYPE, "Database type (#{ConfigureDatabasePlatform.get_types().join(',')})", 
        validator)
  end
  
  def load_default_value
    case @config.getProperty(get_host_key(USERID))
    when "postgres"
      @default = "postgresql-wal"
    when "enterprisedb"
      @default = "postgresql-wal"
    else
      if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_ARCHIVE
        @default = ""
      else
        @default = "mysql"
      end
    end
  end
  
  def get_disabled_value
    get_default_value()
  end
  
  def enabled_for_load_default_value?
    return enabled?()
  end
end

class RootCommandPrefixPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ROOT_PREFIX, "Run root commands using sudo", 
      PV_BOOLEAN, "false")
  end
  
  def load_default_value
    if @config.getProperty(USERID) != "root" && @config.getProperty(HOST_ENABLE_MANAGER) == "true"
      @default = "true"
    else
      @default = "false"
    end
  end
  
  def get_template_value
    if get_value() == "true"
      "sudo -n"
    else
      ""
    end
  end
  
  def get_command_line_argument()
    "enable-sudo-access"
  end
  
  def get_command_line_aliases
    [@name.gsub("_", "-")]
  end
end

class JavaGarbageCollection < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include MigrateFromReplicationServices
  
  def initialize
    super(REPL_JAVA_ENABLE_CONCURRENT_GC, "Replicator Java uses concurrent garbage collection",
      PV_BOOLEAN, "false")
  end
  
  def get_template_value
    if get_value() == "true"
      ""
    else
      "#"
    end
  end
end

class JavaExternalLibDir < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include MigrateFromReplicationServices
  include OptionalPromptModule

  def initialize
    super(REPL_JAVA_EXTERNAL_LIB_DIR, 
      "Directory for 3rd party Jar files required by replicator",
      PV_FILENAME)
  end
  
  def load_default_value
    lib_directories = []
    @config.getPropertyOr([REPL_SERVICES], {}).keys().each{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      config_prefix = [REPL_SERVICES, rs_alias]
      ds = ConfigureDatabasePlatform.build(config_prefix, @config)
      lib = ds.getExternalLibraryDirectory()
      if lib.to_s() != ""
        lib_directories << lib
      end
      
      ds = ConfigureDatabasePlatform.build(config_prefix, @config, true)
      lib = ds.getExternalLibraryDirectory()
      if lib.to_s() != ""
        lib_directories << lib
      end
    }
    
    lib_directories.uniq!()
    if lib_directories.size() > 1
      raise MessageError.new("Multiple replication services require an external library. This is not supported.")
    elsif lib_directories.size() == 1
      @default = lib_directories[0]
    else
      @default = nil
    end
  end
end

class ReplicationRMIPort < ConfigurePrompt
  include ClusterHostPrompt
  include MigrateFromReplicationServices
  
  def initialize
    super(REPL_RMI_PORT, "Replication RMI listen port", 
      PV_INTEGER, 10000)
  end
  
  PortForManagers.register(HOSTS, REPL_RMI_PORT, REPL_RMI_RETURN_PORT)
end

class ReplicationReturnRMIPort < ConfigurePrompt
  include ClusterHostPrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_RMI_RETURN_PORT, "Replication RMI return port", 
      PV_INTEGER)
  end
  
  def load_default_value
    @default = (@config.getProperty(get_member_key(REPL_RMI_PORT)).to_i() + 1).to_s
  end
end

class ReplicationServiceMetadataDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_METADATA_DIRECTORY, "Replicator metadata directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(METADATA_DIRECTORY)) + "/applier"
  end
end

class THLStorageType < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_LOG_TYPE, "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, "disk")
  end
  
  def get_disabled_value
    "disk"
  end
end

class THLStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/thl"
  end
  
  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/thl"
    else
      super()
    end
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_log_dir'))
    super()
  end
end

class RelayLogStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Directory for logs transferred from the master",
		  PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/relay"
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_relay_log_dir'))
    super()
  end

  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/relay"
    else
      super()
    end
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Permanent backup storage directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/backups"
  end
  
  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/backups"
    else
      super()
    end
  end
end

class InstallServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(SVC_INSTALL, "Install service start scripts", 
      PV_BOOLEAN, "false")
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def enabled?
    (@config.getProperty(get_member_key(USERID)) == "root" || 
      @config.getProperty(get_member_key(ROOT_PREFIX)) == "true")
  end
end

class StartServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_START, "Start the services after configuration", 
      PV_BOOLEAN)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(SVC_REPORT))
  end
end

class ReportServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_REPORT, "Start the services and report out the status after configuration", 
      PV_BOOLEAN, "false")
    self.extend(NotTungstenUpdatePrompt)
  end
end

class ProfileScriptPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include NoConnectorRestart
  include NoReplicatorRestart
  include NoManagerRestart
  
  def initialize
    super(PROFILE_SCRIPT, "Append commands to include env.sh in this profile script", PV_ANY, "")
  end
  
  def required?
    false
  end
end

class ExecutablePrefixPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include NoConnectorRestart
  include NoReplicatorRestart
  include NoManagerRestart
  
  def initialize
    super(EXECUTABLE_PREFIX, "Declare aliases for all scripts with this prefix to support multiple installations per host", PV_ANY, "")
  end
  
  def required?
    false
  end
end

class HostServicePathReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_REPLICATOR, "Path to the replicator service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-replicator/bin/replicator"
  end
end

class HostServicePathManager < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_MANAGER, "Path to the manager service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-manager/bin/manager"
  end
end

class HostServicePathConnector < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_CONNECTOR, "Path to the connector service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-connector/bin/connector"
  end
end

class HostGlobalProperties < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(FIXED_PROPERTY_STRINGS, "Fixed properties for this host")
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    if allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    else
      value = super()
    end
        
    value
  end
  
  def load_default_value
    @default = []
  end
  
  def get_default_value
    if get_member() == DEFAULTS
      return super()
    end
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
    end
    
    return values
  end
  
  def required?
    false
  end
  
  def build_command_line_argument(member, values, public_argument = false)
    args = []
    
    if values.is_a?(Array)
      values.each{
        |v|
        args << "--property=#{v}"
      }
    elsif values.to_s() != ""
      args << "--property=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class HostSkippedChecks < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SKIPPED_VALIDATION_CLASSES, "Skipped validation classes for this host")
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    if allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    else
      value = super()
    end
        
    value
  end
  
  def load_default_value
    @default = []
  end
  
  def get_default_value
    if get_member() == DEFAULTS
      return super()
    end
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
    end
    
    return values
  end
  
  def required?
    false
  end
  
  def build_command_line_argument(member, values, public_argument = false)
    args = []
    
    if values.is_a?(Array)
      values.each{
        |v|
        args << "--skip-validation-check=#{v}"
      }
    elsif values.to_s() != ""
      args << "--skip-validation-check=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class HostSkippedWarnings < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SKIPPED_VALIDATION_WARNINGS, "Skipped validation warnings for this host")
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    if allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    else
      value = super()
    end
        
    value
  end
  
  def load_default_value
    @default = []
  end
  
  def get_default_value
    if get_member() == DEFAULTS
      return super()
    end
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
    end
    
    return values
  end
  
  def required?
    false
  end
  
  def build_command_line_argument(member, values, public_argument = false)
    args = []
    
    if values.is_a?(Array)
      values.each{
        |v|
        args << "--skip-validation-warnings=#{v}"
      }
    elsif values.to_s() != ""
      args << "--skip-validation-warnings=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class HostPreferredPath < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(PREFERRED_PATH, "Additional command path", PV_ANY, "")
  end
end

class TemplateSearchPath < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(TEMPLATE_SEARCH_PATH, "Additional path to find configuration templates", PV_ANY)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/share/templates"
  end
end

class HostSkipStatemap < ConfigurePrompt
  include ClusterHostPrompt
  include CommercialPrompt
  
  def initialize
    super(SKIP_STATEMAP, "Do not copy the cluster-home/conf/statemap.properties from the previous install", PV_BOOLEAN, "false")
  end
end

class HostDataServiceName < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(DATASERVICENAME, "Name of this dataservice", PV_IDENTIFIER, DEFAULT_SERVICE_NAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    @config.getPropertyOr(DATASERVICES, {}).each_key{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      if @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).include_alias?(get_member())
        if Topology.build(ds_alias, @config).use_management?()
          @default = @config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
        end
      end
    }
  end
end

class HostDisableSecurityControls < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(DISABLE_SECURITY_CONTROLS, "Disable all security controls and file protection.", PV_BOOLEAN)
  end
  
  def load_default_value
    if Configurator.instance.default_security?() == true
      @default = "false"
    else
      @default = "true"
    end
  end
end

class HostSecurityDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SECURITY_DIRECTORY, "Storage directory for the Java security/encryption files", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/share"
  end
end

class HostEnableJgroupsSSL < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ENABLE_JGROUPS_SSL, "Enable SSL encryption of JGroups communication on this host", PV_BOOLEAN)
    add_command_line_alias("jgroups-ssl")
  end
  
  def get_template_value
    if get_value() == "true"
      keystore = File.basename(@config.getTemplateValue(get_member_key(JAVA_JGROUPS_KEYSTORE_PATH)))
      key_alias = @config.getProperty(get_member_key(JAVA_JGROUPS_ENTRY_ALIAS))
      ks_pass = @config.getProperty(get_member_key(JAVA_KEYSTORE_PASSWORD))
      return "<ENCRYPT key_store_name=\"#{keystore}\"  store_password=\"#{ks_pass}\" key_password=\"#{ks_pass}\" alias=\"#{key_alias}\" />"
    else
      return ""
    end
  end
  
  def load_default_value
    if get_member_property(DISABLE_SECURITY_CONTROLS) == "true"
      @default = "false"
    else
      @default = "true"
    end
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    is_manager = get_member_property(HOST_ENABLE_MANAGER)
    if is_manager == "false"
      # Do nothing because the TLS certificate is not used
      return "false"
    else
      super(allow_default, allow_disabled)
    end
  end
end

class HostJgroupsTLSAlias < ConfigurePrompt
  include ClusterHostPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_JGROUPS_ENTRY_ALIAS, "The alias to use for the JGroups TLS key in the keystore.", PV_ANY, "jgroups")
  end
end

class HostJavaJgroupsKeystorePath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_JGROUPS_KEYSTORE_PATH, "Local path to the JGroups Java Keystore file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/tungsten_jgroups_keystore.jceks"
  end
  
  def required?
    false
  end
  
  def accept?(raw_value)
    if raw_value == AUTOGENERATE
      raw_value
    else
      super(raw_value)
    end
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != "" && value != AUTOGENERATE
      if value == get_template_value()
        error("The provided JGroups keystore may not be placed at #{get_template_value()} as it will conflict with installation. Move the file to another location and update --java-jgroups-keystore-path.")
      elsif File.exists?(value) != true
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_JGROUPS_KEYSTORE_PATH, GLOBAL_JAVA_JGROUPS_KEYSTORE_PATH)
  
  def self.build_keystore(dest, keyalias, storepass, first_time_warning = nil)
    Configurator.instance.synchronize() {
      @mutex ||= Mutex.new
    }
    
    @mutex.synchronize do
      @keystores ||= {}
      
      if @keystores[keyalias] != nil
        return @keystores[keyalias]
      end
      
      keytool = which("keytool")
      if keytool == nil
        raise "Unable to generate a file for --java-jgroups-keystore-path because the keytool command is not available. Install keytool or disable the feature with '--jgroups-ssl=false'."
      end
      
      if first_time_warning != nil
        Configurator.instance.warning(first_time_warning)
      end
      
      ks = Tempfile.new("jgroupssec")
      ks.close()
      File.unlink(ks.path())
      path = "#{dest}/#{File.basename(ks.path())}"
      
      keystore = JavaKeytool.new(path, JavaKeytool::TYPE_JCEKS)
      keystore.genseckey(keyalias, storepass)
      
      @keystores[keyalias] = path
      
      return @keystores[keyalias]
    end
  end
end

class GlobalHostJavaJgroupsKeystorePath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_JGROUPS_KEYSTORE_PATH, "Staging path to the Java JGroups Keystore file", 
      PV_FILENAME)
  end
end

class HostEnableRMIAuthentication < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ENABLE_RMI_AUTHENTICATION, "Enable RMI authentication for the services running on this host", PV_BOOLEAN)
    add_command_line_alias("rmi-authentication")
  end
  
  def load_default_value
    if @config.getProperty(DISABLE_SECURITY_CONTROLS) == "true"
      @default = "false"
    else
      @default = "true"
    end
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    is_manager = get_member_property(HOST_ENABLE_MANAGER)
    is_replicator = get_member_property(HOST_ENABLE_REPLICATOR)
    if is_manager == "false" && is_replicator == "false"
      # Do nothing because the TLS certificate is not used
      return "false"
    else
      super(allow_default, allow_disabled)
    end
  end
end

class HostEnableRMISSL < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ENABLE_RMI_SSL, "Enable SSL encryption of RMI communication on this host", PV_BOOLEAN)
    add_command_line_alias("rmi-ssl")
  end
  
  def load_default_value
    if @config.getProperty(DISABLE_SECURITY_CONTROLS) == "true"
      @default = "false"
    else
      @default = "true"
    end
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    is_manager = get_member_property(HOST_ENABLE_MANAGER)
    is_replicator = get_member_property(HOST_ENABLE_REPLICATOR)
    if is_manager == "false" && is_replicator == "false"
      # Do nothing because the TLS certificate is not used
      return "false"
    else
      super(allow_default, allow_disabled)
    end
  end
end

class HostRMIUser < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(RMI_USER, "The username for RMI authentication", PV_ANY, "tungsten")
  end
end

class HostJavaKeystorePassword < ConfigurePrompt
  include ClusterHostPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_KEYSTORE_PASSWORD, "The password for unlocking the tungsten_keystore.jks file in the security directory", PV_ANY, "tungsten")
  end
end

class HostJavaTruststorePassword < ConfigurePrompt
  include ClusterHostPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_TRUSTSTORE_PASSWORD, "The password for unlocking the tungsten_truststore.jks file in the security directory", PV_ANY, "tungsten")
  end
end

class HostJavaKeystorePath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_KEYSTORE_PATH, "Local path to the Java Keystore file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/tungsten_keystore.jks"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_KEYSTORE_PATH, GLOBAL_JAVA_KEYSTORE_PATH)
end

class GlobalHostJavaKeystorePath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_KEYSTORE_PATH, "Staging path to the Java Keystore file", 
      PV_FILENAME)
  end
end

class HostJavaTruststorePath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_TRUSTSTORE_PATH, "Local path to the Java Truststore file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/tungsten_truststore.ts"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_TRUSTSTORE_PATH, GLOBAL_JAVA_TRUSTSTORE_PATH)
end

class GlobalHostJavaTruststorePath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_TRUSTSTORE_PATH, "Staging path to the Java Truststore file", 
      PV_FILENAME)
  end
end

class HostJavaJMXRemoteAccessPath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_JMXREMOTE_ACCESS_PATH, "Local path to the Java JMX Remote Access file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/jmxremote.access"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_JMXREMOTE_ACCESS_PATH, GLOBAL_JAVA_JMXREMOTE_ACCESS_PATH)
end

class GlobalHostJavaJMXRemoteAccessPath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_JMXREMOTE_ACCESS_PATH, "Staging path to the Java JMX Remote Access file", 
      PV_FILENAME)
  end
end

class HostJavaPasswordStorePath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_PASSWORDSTORE_PATH, "Local path to the Java Password Store file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/passwords.store"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_PASSWORDSTORE_PATH, GLOBAL_JAVA_PASSWORDSTORE_PATH)
end

class GlobalHostJavaPasswordStorePath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_PASSWORDSTORE_PATH, "Staging path to the Java Password Store file", 
      PV_FILENAME)
  end
end

class HostTLSKeyLifetime < ConfigurePrompt
  include ClusterHostPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_TLS_KEY_LIFETIME, "Lifetime for the Java TLS key", PV_INTEGER, 365)
  end
end

class HostTLSAlias < ConfigurePrompt
  include ClusterHostPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_TLS_ENTRY_ALIAS, "The alias to use for the TLS key/certificate in the keystore and truststore.", PV_ANY, "tls")
  end
end

class HostJavaTLSKeystorePath < ConfigurePrompt
  include ClusterHostPrompt
  include OptionalPromptModule
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_TLS_KEYSTORE_PATH, "The keystore holding a certificate to use for all Continuent TLS encryption.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_member_key(SECURITY_DIRECTORY)) + "/tungsten_tls_keystore.jks"
  end
  
  def accept?(raw_value)
    if raw_value == AUTOGENERATE
      raw_value
    else
      super(raw_value)
    end
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != "" && value != AUTOGENERATE
      if value == get_template_value()
        error("The provided TLS keystore may not be placed at #{get_template_value()} as it will conflict with installation. Move the file to another location and update --java-tls-keystore-path.")
      elsif File.exists?(value) != true
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_TLS_KEYSTORE_PATH, GLOBAL_JAVA_TLS_KEYSTORE_PATH)
  
  def self.build_keystore(dest, keyalias, storepass, lifetime, first_time_warning = nil)
    Configurator.instance.synchronize() {
      @mutex ||= Mutex.new
    }
    
    @mutex.synchronize do
      @keystores ||= {}
      
      if @keystores[keyalias] != nil
        return @keystores[keyalias]
      end
      
      keytool = which("keytool")
      if keytool == nil
        raise "Unable to generate a file for --java-tls-keystore-path because the keytool command is not available. Install keytool or disable the feature with '--rmi-ssl=false --rmi-authentication=false --thl-ssl=false'."
      end
      
      if first_time_warning != nil
        Configurator.instance.warning(first_time_warning)
      end
      
      ks = Tempfile.new("tlssec")
      ks.close()
      File.unlink(ks.path())
      path = "#{dest}/#{File.basename(ks.path())}"
      
      keystore = JavaKeytool.new(path)
      keystore.genkey(keyalias, storepass, lifetime)
      
      @keystores[keyalias] = path
      
      return @keystores[keyalias]
    end
  end
end

class GlobalHostTLSCertificate < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_TLS_KEYSTORE_PATH, "The keystore holding a certificate to use for all Continuent TLS encryption.", PV_FILENAME)
  end
end

class HostSSLCA < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  include HiddenValueModule
  
  def initialize
    super(SSL_CA, "Local path to the SSL certificate authority", PV_FILENAME)
  end
  
  def required?
    false
  end
end

class HostSSLCert < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  include HiddenValueModule
  
  def initialize
    super(SSL_CERT, "Local path to the SSL certificate", PV_FILENAME)
  end
  
  def required?
    false
  end
end

class HostSSLKey < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  include HiddenValueModule
  
  def initialize
    super(SSL_KEY, "Local path to the SSL certificate key", PV_FILENAME)
  end
  
  def required?
    false
  end
end

class HostProtectConfigurationFiles < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(PROTECT_CONFIGURATION_FILES, "Make configuration files readable by only the system user", PV_BOOLEAN)
  end
  
  def load_default_value
    if @config.getProperty(DISABLE_SECURITY_CONTROLS) == "true"
      @default = "false"
    else
      @default = "true"
    end
  end
end

class HostFileProtectionLevel < ConfigurePrompt
  include ClusterHostPrompt
  include NewDirectoryUpdate
  
  def initialize
    validator = PropertyValidator.new("^user|group|none$", 
      "Value must be user, group or none")
    super(FILE_PROTECTION_LEVEL, "Protection level for Continuent files", validator)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(PROTECT_CONFIGURATION_FILES)) == "false"
      @default = "none"
    else
      if @config.getProperty(DISABLE_SECURITY_CONTROLS) == "true"
        @default = "none"
      else
        @default = "group"
      end
    end
  end
  
  def get_template_value
    case get_value()
    when "user"
      return "0077"
    when "group"
      return "0027"
    else
      return nil
    end
  end
end

class HostFileProtectionUmask < ConfigurePrompt
  include ClusterHostPrompt
  include OptionalPromptModule
  
  def initialize
    super(FILE_PROTECTION_UMASK, "Protection umask for Continuent files", PV_ANY)
  end
  
  def load_default_value
    @default = @config.getTemplateValue(FILE_PROTECTION_LEVEL)
  end
end

class HostServiceControlType < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  SYSTEMCTL = "systemctl"
  INITD = "initd"
  
  def initialize
    validator = PropertyValidator.new("^initd|systemctl$", 
      "Value must be initd or systemctl")
    super(DEFAULT_SERVICE_CONTROL_TYPE, "What type of mechanism is used to control the startup and shutdown of system services", validator)
  end
  
  def required?
    true
  end
  
  def load_default_value
    begin
      systemctl = which("systemctl")
    rescue CommandError
      systemctl = nil
    end
    
    if systemctl == nil
      # Catch instances where `systemctl` is not in $PATH
      begin
        systemctl = cmd_result("ls /usr/bin/systemctl")
      rescue CommandError
        systemctl = nil
      end
    end
    
    begin
      service = which("service")
    rescue CommandError
      service = nil
    end

    if service == nil
      # Catch instances where `service` is not in $PATH
      begin
        service = cmd_result("ls /sbin/service")
      rescue CommandError
        service = nil
      end
    end
    
    if systemctl != nil
      @default = SYSTEMCTL
    elsif service != nil
      @default = INITD
    end
  end
end

class HostPortsForUsers < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_USERS, "Ports opened on this server for all users and applications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForUsers.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForConnectors < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_CONNECTORS, "Ports opened on this server for connector communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForConnectors.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForManagers < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_MANAGERS, "Ports opened on this server for manager communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForManagers.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForReplicators < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_REPLICATORS, "Ports opened on this server for replicator communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForReplicators.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostTrialModeLicense < ConfigurePrompt
  include ClusterHostPrompt
  include HiddenValueModule
  include NoSystemDefault
  
  def initialize
    super(ENABLE_TRIAL_MODE, "Use trial-mode for this host", PV_BOOLEAN, "false")
  end
end

class HostLicensesPaths < ConfigurePrompt
  include ClusterHostPrompt
  include DefaultValueOnlyModule
  
  def initialize
    super(HOST_LICENSE_PATHS, "List of paths to search for licenses assigned to this host")
  end
  
  def load_default_value
    @default = [
      "#{@config.getProperty(HOME_DIRECTORY)}/share/continuent.license",
      "#{@config.getProperty(HOME_DIRECTORY)}/share/continuent.licenses",
      "/etc/tungsten/continuent.license",
      "/etc/tungsten/continuent.licenses"
    ]
  end
end

class HostLicenses < ConfigurePrompt
  include ClusterHostPrompt
  include DefaultValueOnlyModule
  
  def initialize
    super(HOST_LICENSES, "List of licenses assigned to this host", PV_ANY)
  end
  
  def load_default_value
    @default = nil
    
    # Allow for the --enable-trial-mode setting to override the default
    # Any continuent.licenses files will override this setting
    if @config.getProperty(ENABLE_TRIAL_MODE) == "true"
      @default = [LICENSE_TRIAL]
    end
    
    search_paths = @config.getPropertyOr(HOST_LICENSE_PATHS, [])
    unless search_paths.is_a?(Array)
      return
    end
    
    search_paths.each{
      |path|
      if ! File.exist?(path)
        next
      end
      
      if ! File.readable?(path)
        warning("Unable to read #{path}")
        next
      end
      
      File.open(path, "r") {
        |f|
        @default = f.readlines().join().strip().split("\n")
        return
      }
    }
  end
end
