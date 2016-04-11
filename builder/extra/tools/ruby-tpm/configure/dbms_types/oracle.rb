DBMS_ORACLE = "oracle"

# Oracle-specific parameters.
REPL_ORACLE_SERVICE = "repl_datasource_oracle_service"
REPL_ORACLE_SID = "repl_datasource_oracle_sid"
REPL_ORACLE_SYSTEM_USER = "repl_datasource_oracle_service_user"
REPL_ORACLE_SYSTEM_GROUP = "repl_datasource_oracle_service_group"
REPL_ORACLE_DSPORT = "repl_oracle_dslisten_port"
REPL_ORACLE_HOME = "repl_oracle_home"
REPL_ORACLE_LICENSE = "repl_oracle_license"
REPL_ORACLE_SCHEMA = "repl_oracle_schema"
REPL_ORACLE_LICENSED_SLAVE = "repl_oracle_licensed_slave"
REPL_ORACLE_SCAN = "repl_datasource_oracle_scan"
REPL_ORACLE_EXTRACTOR_METHOD = "repl_oracle_extractor_method"
REPL_ORACLE_REDO_MINER_DIRECTORY = "repl_oracle_redo_miner_directory"
REPL_ORACLE_REDO_MINER_TRANSACTION_FRAGMENT_SIZE = "repl_oracle_redo_miner_transaction_fragment_size"

EXTRACTOR_REPL_ORACLE_SERVICE = "repl_direct_datasource_oracle_service"
EXTRACTOR_REPL_ORACLE_SID = "repl_direct_datasource_oracle_sid"
EXTRACTOR_REPL_ORACLE_SCAN = "repl_direct_datasource_oracle_scan"

class OracleDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_ORACLE
  end
  
  def get_default_backup_method
    "none"
  end
  
  def get_valid_backup_methods
    "none|script"
  end
  
  def get_applier_template
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SCAN], "") == ""
      "tungsten-replicator/samples/conf/appliers/#{get_uri_scheme()}.tpl"
    else
      "tungsten-replicator/samples/conf/appliers/oracle-scan.tpl"
    end
	end
  
  def get_thl_uri
    getJdbcUrl()
	end
  
  def get_default_port
    "1521"
  end
  
  def get_default_start_script
    nil
  end
  
  def getBasicJdbcUrl()
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SCAN], "") == ""
      host_parameter = "replicator.global.db.host"
    else
  	  host_parameter = "replicator.applier.oracle.scan"
    end
    
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SID], "") == ""
      separator = "@//"
    else
      separator = "@"
    end
    
    "jdbc:oracle:thin:#{separator}${#{host_parameter}}:${replicator.global.db.port}"
  end
  
  def getJdbcUrl()
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SID], "") == ""
      getBasicJdbcUrl() + "/${replicator.applier.oracle.service}"
    else
      getBasicJdbcUrl() + ":${replicator.applier.oracle.sid}"
    end
  end
  
  def getExtractorJdbcUrl()
    if @config.getPropertyOr(@prefix + [EXTRACTOR_REPL_ORACLE_SCAN], "") == ""
      host_parameter = "replicator.global.extract.db.host"
    else
  	  host_parameter = "replicator.extractor.oracle.scan"
    end
    
    if @config.getPropertyOr(@prefix + [EXTRACTOR_REPL_ORACLE_SID], "") == ""
      separator = "@//"
    else
      separator = "@"
    end
    
    basicUrl = "jdbc:oracle:thin:#{separator}${#{host_parameter}}:${replicator.global.extract.db.port}"
    
    if @config.getPropertyOr(@prefix + [EXTRACTOR_REPL_ORACLE_SID], "") == ""
      basicUrl = basicUrl + "/${replicator.extractor.oracle.service}"
    else
      basicUrl = basicUrl + ":${replicator.extractor.oracle.sid}"
    end
    
    basicUrl
  end
  
  def getJdbcDriver()
    "oracle.jdbc.driver.OracleDriver"
  end
  
  def getVendor()
    "oracle"
  end
  
  def needsExternalLibraries()
    found_ojdbc = nil
    base = Configurator.instance.get_base_path()
    Dir.glob("#{base}/tungsten-replicator/lib/ojdbc*.jar").each{
      |lib|
      found_ojdbc = lib
    }
    # There is already an OJDBC driver so we don't need to do anything
    if found_ojdbc != nil
      return false
    end
    
    return true
  end
  
  def getExternalLibraries()
    if needsExternalLibraries() == false
      return {}
    end
    
    java_out = cmd_result("java -version 2>&1")
    if java_out =~ /version \"1\.8\./
      allowed_jars = ["ojdbc8.jar", "ojdbc7.jar", "ojdbc6.jar"]
    elsif java_out =~ /version \"1\.7\./
      allowed_jars = ["ojdbc7.jar", "ojdbc6.jar"]
    else
      allowed_jars = ["ojdbc6.jar"]
    end
    
    oracle_home = @config.getProperty(get_key(REPL_ORACLE_HOME))
    if oracle_home.to_s() != ""
      allowed_jars.each{
        |jar|
        filename = "#{oracle_home}/jdbc/lib/#{jar}"
        if File.exist?(filename)
          return {
            filename => "tungsten-replicator/lib"
          }
        end
      }
    end
    
    {}
  end
  
  def missingExternalLibrariesErrorMessage
    oracle_home = @config.getProperty(get_key(REPL_ORACLE_HOME))
    if oracle_home.to_s() == ""
      oracle_home = "<ORACLE_HOME>"
    end
    "The OJDBC JAR file was not located in #{oracle_home}/jdbc/lib. Update the --oracle-home setting or copy the appropriate file into tungten-replicator/lib before proceeding."
  end

  def get_extractor_template
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SCAN], "") != ""
      # This picks oracle-scan.tpl.
      "tungsten-replicator/samples/conf/extractors/#{get_uri_scheme()}.tpl"
    elsif @config.getPropertyOr(@prefix + [REPL_ORACLE_EXTRACTOR_METHOD], "") != ""
      # This picks oracle-{cdc|redo}
      tpl_base = "oracle-" + 
         @config.getPropertyOr(@prefix + [REPL_ORACLE_EXTRACTOR_METHOD])
      "tungsten-replicator/samples/conf/extractors/#{tpl_base}.tpl"
    else
      "tungsten-replicator/samples/conf/extractors/oracle.tpl"
    end
  end

  def get_default_table_engine
    case @config.getProperty(REPL_ROLE)
    when REPL_ROLE_S
      "redo"
    else
      "redo"
    end
  end

  def get_allowed_table_engines
    ["redo", "CDCASYNC", "CDCSYNC"]
  end
  
  def get_extractor_filters
    # Compute the method. 
    method = @config.getPropertyOr(@prefix + [REPL_ORACLE_EXTRACTOR_METHOD], "")

    # Assign default filters based on type of extraction. 
    if @config.getPropertyOr(@prefix + [REPL_ORACLE_SCAN], "") != ""
      # Oracle scan gets no extra filters. 
      super()
    elsif ["cdc", "cdcasync"].include?(method)
      # CDC gets CDC filter. 
      super() + ["CDC"]
    elsif method == "redo"
      # Redo log reader requires filter to drop catalog data. 
      ["dropcatalogdata"] + super()
    else
      # Anything else gets defaults. 
      super()
    end
  end
  
	def get_applier_filters()
	  if @config.getProperty(@prefix + [ENABLE_HETEROGENOUS_SLAVE]) == "true"
	    ["nocreatedbifnotexists","dbupper"] + super()
	  else
	    super()
	  end
	end
	
	def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def get_replication_schema
    @username
  end
  
  def extractor_provides_colnames?
    true
  end
  
  def applier_supports_parallel_apply?()
    true
  end
  
  def applier_supports_reset?
    true
  end
  
  def can_sql?
    true
  end
  
  def sql_url
    scan = @config.getPropertyOr(@prefix + [REPL_ORACLE_SCAN], "")
    sid = @config.getPropertyOr(@prefix + [REPL_ORACLE_SID], "")
    service = @config.getPropertyOr(@prefix + [REPL_ORACLE_SERVICE], "")
    
    if scan == ""
      host_parameter = @host
    else
  	  host_parameter = scan
    end
    
    if sid == ""
      separator = "@//"
    else
      separator = "@"
    end
    
    if sid == ""
      suffix = "/#{service}"
    else
      suffix = ":#{sid}"
    end
    
    "jdbc:oracle:thin:#{separator}#{host_parameter}:#{@port}#{suffix}"
  end
end

#
# Prompts
#

class OracleConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_oracle_default_value()
    rescue => e
      super()
    end
  end
  
  def get_oracle_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def oracle(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    raise "Update this to build the proper command"
  end
  
  def enabled?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
end

class OracleService < OracleConfigurePrompt
  include DatasourcePrompt
  include OptionalPromptModule
  
  def initialize
    super(REPL_ORACLE_SERVICE, "Oracle Service", PV_ANY)
  end
end

class OracleHome < OracleConfigurePrompt
  include DatasourcePrompt
  include OptionalPromptModule
  
  def initialize
    super(REPL_ORACLE_HOME, "Oracle Home", PV_FILENAME)
  end
  
  def load_default_value
    if ENV.has_key?("ORACLE_HOME")
      @default = ENV["ORACLE_HOME"]
    end
  end
end

class OracleSID < OracleConfigurePrompt
  include DatasourcePrompt
  include OptionalPromptModule
  
  def initialize
    super(REPL_ORACLE_SID, "Oracle SID", PV_ANY)
  end
end

class OracleSCAN < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_SCAN, "Oracle SCAN", PV_HOSTNAME)
  end
  
  def required?
    false
  end
end

class OracleSystemUser < OracleConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(REPL_ORACLE_SYSTEM_USER, "Oracle service system user", 
      PV_ANY, "oracle")
  end
end

class OracleSystemGroup < OracleConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(REPL_ORACLE_SYSTEM_GROUP, "Oracle service system group", PV_ANY)
  end
  
  def load_default_value
    begin
      oracle_user = get_member_property(REPL_ORACLE_SYSTEM_USER)
      @default = cmd_result("id #{oracle_user} -g -n")
    rescue CommandError
    end
  end
end

class OracleExtractorMethod < OracleConfigurePrompt
  include DatasourcePrompt
  include OptionalPromptModule

  def initialize
    super(REPL_ORACLE_EXTRACTOR_METHOD, "Oracle extractor
      method", PV_ANY)
  end
end

class OracleExtractorService < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_ORACLE_SERVICE, "Oracle Service", PV_ANY)
  end
  
  def required?
    false
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_ORACLE_SERVICE))
  end
end

class OracleExtractorSID < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_ORACLE_SID, "Oracle SID", PV_ANY)
  end
  
  def required?
    false
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_ORACLE_SID))
  end
end

class OracleExtractorSCAN < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_ORACLE_SCAN, "Oracle SCAN", PV_HOSTNAME)
  end
  
  def required?
    false
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_ORACLE_SCAN))
  end
end

class OracleExtractorRedoMinerDirectory < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_REDO_MINER_DIRECTORY, "Oracle Redo Miner Directory", PV_FILENAME)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(REPL_ORACLE_EXTRACTOR_METHOD)) == "redo")
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)).to_s == ""
      return
    end
    
    home = @config.getProperty(HOME_DIRECTORY)
    ds = @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
    @default = "#{home}/plog/#{ds}"
  end
end

class OracleExtractorRedoMinerTransactionFragmentSize < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_REDO_MINER_TRANSACTION_FRAGMENT_SIZE, "Oracle Redo Miner Transaction Fragment Size", PV_INTEGER, "1000")
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(REPL_ORACLE_EXTRACTOR_METHOD)) == "redo")
  end
end

#
# Validation
#

module OracleCheck
  def get_variable(name)
    oracle("show #{name}").chomp.strip;
  end
  
  def enabled?
    super() && (
      @config.getProperty(REPL_DBTYPE) == DBMS_ORACLE ||
      @config.getProperty(EXTRACTOR_REPL_DBTYPE) == DBMS_ORACLE
    )
  end
end

module OracleCDCCheck
  def enabled?
    disable_extractor = get_member_value(REPL_DISABLE_EXTRACTOR)
    extractor_method = get_member_value(REPL_ORACLE_EXTRACTOR_METHOD)

    if disable_extractor == "true"
      false
    elsif ["cdc", "cdcasync"].include?(extractor_method) != true
      false
    else
      super()
    end
  end
end

module OracleRedoReaderCheck
  def enabled?
    disable_extractor = get_member_value(REPL_DISABLE_EXTRACTOR)
    extractor_method = get_member_value(REPL_ORACLE_EXTRACTOR_METHOD)

    if disable_extractor == "true"
      false
    elsif ["redo"].include?(extractor_method) != true
      false
    else
      super()
    end
  end
end

class OracleVersionCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck

  def set_vars
    @title = "Oracle version check"
  end
  
  def validate
    supported_versions = []
    if @config.getProperty(get_member_key(REPL_DISABLE_EXTRACTOR)) == "true"
      return true
    else
      extractor_method = get_member_property(REPL_ORACLE_EXTRACTOR_METHOD)
      if extractor_method == "redo"
        supported_versions = [10, 11, 12]
      elsif ["cdc", "cdcasync"].include?(extractor_method)
        supported_versions = [10, 11]
      end
    end
    
    begin
      version = get_applier_datasource.sql_column("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'", "BANNER")
    rescue => e
      debug(e)
    end
    
    if version == nil && which("sqlplus")
      begin
        version = cmd_result("sqlplus -v")
      rescue CommandError => ce
        debug(ce)
      end
    end
    
    if version != nil
      match = version.match(/([0-9]+)\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/)
      if match == nil
        warning("Unable to identify the Oracle version for #{get_applier_datasource.get_connection_summary()}")
      else
        debug("The Oracle server at #{get_applier_datasource.get_connection_summary()} is running version #{match[1]}")
        unless supported_versions.include?(match[1].to_i())
          error("The Oracle #{extractor_method} extractor does not support version #{match[1]}")
        end
      end
    else
      warning("Unable to check the version for #{get_applier_datasource.get_connection_summary()}")
    end
  end
end

class OracleLoginCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck

  def set_vars
    @title = "Oracle replication credentials login check"
    @fatal_on_error = true
  end
  
  def validate
    results = get_applier_datasource.sql_result("SELECT 1 FROM ALL_USERS WHERE ROWNUM = 1")
    login_output = nil
    if results.size() > 0 && results[0].is_a?(Hash)
      login_output = results[0]["1"]
    end
    
    if login_output == 1
      info("Oracle server and login is OK for #{get_applier_datasource.get_connection_summary()}")
    else
      error("Unable to connect to the Oracle server using #{get_applier_datasource.get_connection_summary()}")
      
      if get_applier_datasource().password.to_s() == ""
        help("Try specifying a password for #{get_applier_datasource.get_connection_summary()}")
      end
    end
  end
end

class OraclePermissionsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck
  include OracleRedoReaderCheck

  def set_vars
    @title = "Oracle replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    user = @config.getProperty(get_member_key(REPL_DBLOGIN))
    results = get_applier_datasource.sql_result("SELECT COUNT(*) AS CNT FROM USER_TAB_PRIVS WHERE GRANTEE = UPPER('#{user}') AND TABLE_NAME = 'DBMS_FLASHBACK' AND PRIVILEGE = 'EXECUTE'")
      
    privs_cnt = nil
    if results.size() > 0 && results[0].is_a?(Hash)
      privs_cnt = results[0]["CNT"]
    end

    if privs_cnt == 0
      error("The database user is missing some privileges. Run 'GRANT EXECUTE ON dbms_flashback TO #{user};'")
    else
      info("All privileges configured correctly")
    end
  end
end

class OracleServiceSIDCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck
  
  def set_vars
    @title = "Check for a valid Oracle Service or SID"
  end
  
  def validate
    scan = @config.getProperty(get_member_key(REPL_ORACLE_SCAN))
    service = @config.getProperty(get_member_key(REPL_ORACLE_SERVICE))
    sid = @config.getProperty(get_member_key(REPL_ORACLE_SID))
    
    if service.to_s() == "" && sid.to_s() == ""
      error("You must specify --datasource-oracle-service or --datasource-oracle-sid")
    elsif service.to_s() != "" && sid.to_s() != ""
      error("You may not specify --datasource-oracle-service and --datasource-oracle-sid together")
    elsif sid.to_s() != "" && scan.to_s() != ""
      error("You may not specify --datasource-oracle-sid and --datasource-oracle-scan together")
    end
  end
end

class DirectOracleServiceSIDCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck
  
  def set_vars
    @title = "Check for a valid Oracle Service or SID"
  end
  
  def validate
    scan = @config.getProperty(get_member_key(EXTRACTOR_REPL_ORACLE_SCAN))
    service = @config.getProperty(get_member_key(EXTRACTOR_REPL_ORACLE_SERVICE))
    sid = @config.getProperty(get_member_key(EXTRACTOR_REPL_ORACLE_SID))
    
    if service.to_s() == "" && sid.to_s() == ""
      error("You must specify --direct-datasource-oracle-service or --direct-datasource-oracle-sid")
    elsif service.to_s() != "" && sid.to_s() != ""
      error("You may not specify --direct-datasource-oracle-service and --direct-datasource-oracle-sid together")
    elsif sid.to_s() != "" && scan.to_s() != ""
      error("You may not specify --direct-datasource-oracle-sid and --direct-datasource-oracle-scan together")
    end
  end
  
  def enabled?
    super() && get_topology().is_a?(DirectTopology)
  end
end

class OracleRedoReaderMinerDirectoryCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck
  
  def set_vars
    @title = "Check for a valid Redo Reader"
  end
  
  def validate
    redo_directory = @config.getProperty(get_member_key(REPL_ORACLE_REDO_MINER_DIRECTORY))
    begin
      if File.exist?(redo_directory) != true
        raise MessageError.new("The redo directory #{redo_directory} does not exist")
      end
      
      if File.readable?(redo_directory) != true
        raise MessageError.new("The redo directory #{redo_directory} is not readable")
      end
      
      if File.writable?(redo_directory) != true
        raise MessageError.new("The redo directory #{redo_directory} is not writable")
      end
    rescue MessageError => me
      error(me.message)
    end
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(REPL_ORACLE_EXTRACTOR_METHOD)) == "redo")
  end
end

class HostOracleLibrariesFoundCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include OracleCheck
  
  def set_vars
    @title = "Check for the Oracle JDBC driver on each host"
  end
  
  def validate
    ds = get_applier_datasource()
    needs_libraries = ds.needsExternalLibraries()
    if needs_libraries == true
      libraries = ds.getExternalLibraries()
      output_property("HostOracleLibrariesFoundCheck", libraries)
    end
  end
end

class GlobalHostOracleLibrariesFoundCheck < ConfigureValidationCheck
  include PostValidationCheck
  include ClusterHostPostValidationCheck
  
  def set_vars
    @title = "Global check for Oracle JDBC drivers on each host"
  end
  
  def validate
    ojdbc = nil
    base = Configurator.instance.get_base_path()
    Dir.glob("#{base}/tungsten-replicator/lib/ojdbc*.jar").each{
      |lib|
      ojdbc = lib
    }
    if ojdbc != nil
      return
    end
    
    vh = Configurator.instance.command.get_validation_handler()
    output = vh.output_properties
    Configurator.instance.command.get_deployment_configurations().each{
      |cfg|
      key = cfg.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
      unless output.props.has_key?(key)
        next
      end
      
      unless (cfg.getProperty(REPL_DBTYPE) == DBMS_ORACLE ||
        cfg.getProperty(EXTRACTOR_REPL_DBTYPE) == DBMS_ORACLE)
        next
      end

      libraries = nil
      if output.props[key].has_key?("HostOracleLibrariesFoundCheck")
        libraries = output.props[key]["HostOracleLibrariesFoundCheck"]
      end
      unless libraries.is_a?(Hash)
        next
      end
      
      ojdbc = nil
      libraries.keys().each{
        |lib|
        if lib =~ /ojdbc.*\.jar/
          ojdbc = lib
        end
      }
      
      if ojdbc == nil
        error("Unable to find an OJDBC Jar file for the deployment to #{key}. The OJDBC JAR file was not located in <ORACLE_HOME>/jdbc/lib. Update the --oracle-home setting or copy the appropriate file into tungten-replicator/lib before proceeding.")
      end
    }
  end
end
