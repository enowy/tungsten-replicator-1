class ConfigureDatabasePlatform
  attr_reader :username, :password, :host, :port, :extractor
  
  def initialize(prefix, config, extractor = false)
    if prefix == nil || config == nil
      return
    end
    
    @prefix = prefix
    @extractor = extractor
    
    if @extractor == true
      @host = config.getProperty(prefix + [EXTRACTOR_REPL_DBHOST])
      @port = config.getProperty(prefix + [EXTRACTOR_REPL_DBPORT])
      @username = config.getProperty(prefix + [EXTRACTOR_REPL_DBLOGIN])
      @password = config.getProperty(prefix + [EXTRACTOR_REPL_DBPASSWORD])
    else
      @host = config.getProperty(prefix + [REPL_DBHOST])
      @port = config.getProperty(prefix + [REPL_DBPORT])
      @username = config.getProperty(prefix + [REPL_DBLOGIN])
      @password = config.getProperty(prefix + [REPL_DBPASSWORD])
    end
    @config = config
  end
  
  def get_uri_scheme
    raise "Undefined function: #{self.class.name}.get_uri_scheme"
  end
  
  def run(command)
    Configurator.instance.debug("Unable to run #{command} against #{self.class.name} connection")
    
    nil
  end
  
  def get_value(command, column = nil)
    Configurator.instance.debug("Unable to run #{command} against #{self.class.name} connection")
    
    nil
  end
	
	def get_extractor_template
    "tungsten-replicator/samples/conf/extractors/#{get_uri_scheme()}.tpl"
	end
	
	def get_applier_template
    "tungsten-replicator/samples/conf/appliers/#{get_uri_scheme()}.tpl"
	end
	
	def get_datasource_template
    "tungsten-replicator/samples/conf/datasources/#{get_uri_scheme()}.tpl"
	end
	
	def get_datasource_template_ds_name(ds_name)
    "tungsten-replicator/samples/conf/datasources/#{get_uri_scheme()}_#{ds_name}.tpl"
	end

	def get_extractor_filters()
    filters = []
    
    if @config.getProperty(@prefix + [DROP_STATIC_COLUMNS]) == "true"
      filters << "optimizeupdates"
    end
    
	  if @config.getProperty(@prefix + [ENABLE_HETEROGENOUS_MASTER]) == "true"
	    unless extractor_provides_colnames?()
	      filters << "colnames"
	    end
	    
	    if @config.getProperty(@prefix + [TRACK_SCHEMA_CHANGES]) == "true"
	      filters << "schemachange"
	    end
	    
	    filters << "pkey"
	  end
	  
	  return filters
	end
	
	def get_thl_filters()
	  []
	end
	
	def get_remote_filters()
		filters = []
		unless applier_supports_bytes_for_strings?()
			if @config.getProperty(@prefix + [ENABLE_HETEROGENOUS_SLAVE]) == "true"
				filters << "fixmysqlstrings"
			end
		end
	  
		return filters
	end
	
	def get_applier_filters()
	  filters = []
    
    # If --enable-heterogeneous-slave is true
	  if @config.getProperty(@prefix + [ENABLE_HETEROGENOUS_SLAVE]) == "true"
	    # If --track-schema-changes is true
	    if @config.getProperty(@prefix + [TRACK_SCHEMA_CHANGES]) == "true"
	      filters << "monitorschemachange"
	    end
	    
	    # If the applier doesn't drop or isn't able to apply statements, 
	    # this filter will remove them so the applier doesn't fail
	    if applier_supports_statements?() == false
	      filters << "dropstatementdata"
	    end
	  else
	    filters << "pkey"
	  end
	  
	  return filters
	end
	
	def get_backup_agents()
	  agent = @config.getProperty(REPL_BACKUP_METHOD)
	  
	  if agent == "none"
	    []
	  else
	    [agent]
	  end
	end
	
	def get_default_backup_agent()
	  agents = get_backup_agents()
	  
	  if agents.size > 0
	    agents[0]
	  else
	    ""
	  end
	end
	
	def get_thl_uri
	  raise "Undefined function: #{self.class.name}.get_thl_uri"
	end
	
	def check_thl_schema(thl_schema)
  end
  
  def getJdbcQueryUrl()
    getJdbcUrl()
  end
  
  def getJdbcUrl()
    raise "Undefined function: #{self.class.name}.getJdbcUrl"
  end

  def getJdbcUrlSSLOptions()
    ""
  end
  
  def getExtractorJdbcUrl
    getJdbcUrl()
  end
  
  def getExtractorJdbcUrlSSLOptions()
    getJdbcUrlSSLOptions()
  end
  
  def getJdbcDriver()
    raise "Undefined function: #{self.class.name}.getJdbcDriver"
  end
  
  def getJdbcScheme()
    getVendor()
  end
  
  def getVendor()
    raise "Undefined function: #{self.class.name}.getVendor"
  end
  
  def getVersion()
    ""
  end
  
  def getExternalLibraryDirectory()
    nil
  end
  
  def needsExternalLibraries
    false
  end
  
  def getExternalLibraries()
    nil
  end
  
  def missingExternalLibrariesErrorMessage
    "Unable to find necessary libraries for #{get_connection_summary()}"
  end
  
  def get_default_master_log_directory
    raise "Undefined function: #{self.class.name}.get_default_master_log_directory"
  end
  
  def get_default_master_log_pattern
    raise "Undefined function: #{self.class.name}.get_default_master_log_pattern"
  end
  
  def get_default_port
    raise "Undefined function: #{self.class.name}.get_default_port"
  end
  
  def get_default_start_script
    raise "Undefined function: #{self.class.name}.get_default_start_script"
  end
  
  def get_default_systemctl_service
    raise "Undefined function: #{self.class.name}.get_default_systemctl_service"
  end
  
  def find_systemctl_service(options)
    unless options.is_a?(Array)
      raise "find_systemctl_service requires a single Array argument"
    end
    
    cmd = which("systemctl")
    if cmd == nil
      if File.exist?("/bin/systemctl")
        cmd = "/bin/systemctl"
      else
        Configurator.instance.debug("Unable to search using systemctl because it is not in the $PATH")
        return nil
      end
    end
    
    options.each{
      |o|
      begin
        service = cmd_result("#{cmd} list-unit-files | grep '#{o}\.' | awk -F' ' '{print $1}' | head -n1")
        if service.to_s() != ""
          return service
        end
      rescue CommandError => ce  
        debug("Unable to search systemctl for #{o}")
        debug(ce)
      end
    }
    
    return nil
  end
  
  def create_tungsten_schema(schema_name = nil)
    raise "Undefined function: #{self.class.name}.create_tungsten_schema"
  end
  
  def drop_tungsten_schema(schema_name = nil)
    Configurator.instance.warning("Unable to drop the tungsten schema #{schema_name} for #{self.class.name}")
  end
  
  def get_default_backup_method
    "none"
  end
  
  def get_valid_backup_methods
    "none|script|file-copy-snapshot|ebs-snapshot"
  end
  
  def get_start_command
    get_service_control_command("start")
  end
  
  def get_stop_command
    get_service_control_command("stop")
  end
  
  def get_restart_command
    get_service_control_command("restart")
  end
  
  def get_status_command
    get_service_control_command("status")
  end
  
  def get_service_control_command(subcommand)
    control_type = nil
    default_control_type = @config.getProperty(DEFAULT_SERVICE_CONTROL_TYPE)
    begin
      default_initd_script = get_default_start_script()
    rescue
      default_initd_script = nil
    end
    begin
      default_systemctl_script = get_default_systemctl_service()
    rescue
      default_systemctl_script = nil
    end
    initd_script = @config.getProperty(@prefix + [REPL_BOOT_SCRIPT])
    systemctl_service = @config.getProperty(@prefix + [REPL_SYSTEMCTL_SERVICE])
    
    # For each control type, check if there is a command specified for that 
    # type. The check is skipped if the default control type matches because 
    # that will ultimately be used if no other value is given. This logic will 
    # break down if multiple arguments are given.
    if default_control_type != HostServiceControlType::SYSTEMCTL
      if systemctl_service.to_s() != ""
        control_type = HostServiceControlType::SYSTEMCTL
      end
    end
    if default_control_type != HostServiceControlType::INITD
      if initd_script.to_s() != ""
        control_type = HostServiceControlType::INITD
      end
    end
    
    # Check the default control type to make sure it has a valid script
    if control_type == nil
      case default_control_type
      when HostServiceControlType::INITD
        if default_initd_script.to_s() != ""
          control_type = HostServiceControlType::INITD
          initd_script = default_initd_script
        end
      when HostServiceControlType::SYSTEMCTL
        if default_systemctl_script.to_s() != ""
          control_type = HostServiceControlType::SYSTEMCTL
          systemctl_service = default_systemctl_script
        end
      end
    end
    
    # If no other match has been found, go through all possible 
    # start scripts to find a possible match
    if control_type == nil
      if default_systemctl_script.to_s() != ""
        control_type = HostServiceControlType::SYSTEMCTL
        systemctl_service = default_systemctl_script
      elsif default_initd_script.to_s() != ""
        control_type = HostServiceControlType::INITD
        initd_script = default_initd_script
      end
    end
    
    if control_type == nil
      control_type = default_control_type
    end
    
    # Convert the final control_type and relevant path into a full command
    case control_type
    when HostServiceControlType::INITD
      if initd_script.to_s() == ""
        return nil
      end
      
      return "#{initd_script} #{subcommand}"
    when HostServiceControlType::SYSTEMCTL
      if systemctl_service.to_s() == ""
        return nil
      end
      
      return "systemctl #{subcommand} #{systemctl_service}"
    else
      raise MessageError.new("Unable to generate a #{subcommand} command for  #{get_connection_summary(false)} ")
    end
  end
  
  def get_connection_summary(password = true)
    if password == false
      password = ""
    elsif @password.to_s() == ""
      password = " (NO PASSWORD)"
    else
      password = " (WITH PASSWORD)"
    end
    
    "#{@username}@#{@host}:#{@port}#{password}"
  end
  
  def get_key(key)
    @prefix + [key]
  end
  
  def get_topology
    Topology.build(@config.getProperty(@prefix + [DEPLOYMENT_DATASERVICE]), @config)
  end
  
  def get_batch_load_template
    "LOAD DATA INFILE '%%FILE%%' REPLACE INTO TABLE %%TABLE%% CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
  end
  
  def get_batch_insert_template
    "INSERT INTO %%BASE_TABLE%%(%%BASE_COLUMNS%%) SELECT %%BASE_COLUMNS%% FROM %%STAGE_TABLE%%"
  end
  
  def get_batch_delete_template
    "DELETE FROM %%BASE_TABLE%% WHERE %%BASE_PKEY%% IN (SELECT %%STAGE_PKEY%% FROM %%STAGE_TABLE%%)"
  end
  
  def get_replication_schema
    nil
  end
  
  def get_default_table_engine
    "innodb"
  end
  
  def get_allowed_table_engines
    ["innodb"]
  end
  
  def extractor_provides_colnames?
    false
  end
  
  def applier_supports_parallel_apply?()
    false
  end
  
  def applier_supports_reset?
    false
  end
  
  def applier_supports_bytes_for_strings?
    false
  end
  
  def applier_supports_statements?
    false
  end
  
  def can_sql?
    false
  end
  
  def sql_url
    raise "Undefined function: #{self.class.name}.sql_url"
  end

  def sql_libraries
    libraries = getExternalLibraries()
    if libraries.is_a?(Hash)
      return libraries.keys()
    else
      return []
    end
  end
  
  def sql_results(sql)
    unless can_sql?()
      raise "Unable to run SQL command for the #{get_connection_summary}. The datasource type does not support SQL commands."
    end
    
    sql_results_from_url(sql, sql_url(), @username, @password)
  end
  
  def sql_results_from_url(sql, url, user, password)
    cfg = nil
    command = nil
    begin
      cfg = Tempfile.new("cnf")
      cfg.puts("url=#{url}")
      cfg.puts("user=#{user}")
      cfg.puts("password=#{password}")
      cfg.close()
      
      command = Tempfile.new("query")
      if sql.is_a?(Array)
        command.puts(sql.join("\n"))
      else
        command.puts(sql)
      end
      command.close()
      
      libraries = sql_libraries()
      if libraries.is_a?(Array) && libraries.size() > 0
        set_classpath = "CP=#{libraries.join(':')}"
      else
        if needsExternalLibraries() == true
          raise MessageError.new(missingExternalLibrariesErrorMessage())
        end
        set_classpath = ""
      end
      
      base_path = Configurator.instance.get_base_path()
      output = cmd_result("#{set_classpath} #{base_path}/tungsten-replicator/bin/query -conf #{cfg.path} -file #{command.path}")
    rescue CommandError => ce
      Configurator.instance.debug(ce)
      raise MessageError.new("There was an error processing the query: #{ce.result}")
    ensure
      if cfg != nil
        cfg.close()
        cfg.unlink()
      end
      
      if command != nil
        command.close()
        command.unlink()
      end
    end
    
    begin
      results = JSON.parse(output)
      results.each {
        |result|
        if result["rc"] == 0
          result["error"] = nil
        else
          obj = CommandError.new(result["statement"], result["rc"], result["results"], result["error"])
          result["error"] = obj
        end
      }
      results
    rescue JSON::ParserError => pe
      Configurator.instance.debug("Unable to parse SQL results: #{output}")
      raise MessageError.new("There was an error processing the query: #{output}")
    end
  end
  
  def sql_result(sql)
    results = sql_results(sql)
    
    return_first_result(results)
  end
  
  def sql_column(sql, column)
    column.upcase!()
    result = sql_result(sql)
    
    if result.size() > 0 && result[0].is_a?(Hash)
      if result[0].has_key?(column)
        return result[0][column]
      else
        raise MessageError.new("The SQL result does not contain the '#{column}' column")
      end
    else
      return nil
    end
  end
  
  def sql_result_from_url(sql, url, user, password)
    results = sql_results_from_url(sql, url, user, password)
    
    return_first_result(results)
  end
  
  def return_first_result(results)
    unless results.is_a?(Array)
      raise "No results were returned"
    end
    
    if results.size() == 0
      raise "No results were returned"
    end
    
    if results[0]["error"] == nil
      return results[0]["results"][0]
    else
      raise results[0]["error"]
    end
  end
  
  def check_sql_results(results)
    results.each{
      |r|
      if r["error"] != nil
        raise r["error"]
      end
    }
  end
  
  def self.build(prefix, config, extractor = false)
    if extractor == true
      key = EXTRACTOR_REPL_DBTYPE
    else
      key = REPL_DBTYPE
    end
    klass = self.get_class(config.getProperty(prefix + [key]))
    return klass.new(prefix, config, extractor)
  end
  
  def self.get_class(scheme)
    self.get_classes().each{
      |kscheme, klass|
      
      if kscheme == scheme.to_s
        return klass
      end
    }
    
    raise "Unable to find a database type class for #{scheme}"
  end
  
  def self.get_classes
    unless @database_classes
      @database_classes = {}

      self.subclasses.each{
        |klass|
        o = klass.new(nil, nil)
        @database_classes[o.get_uri_scheme()] = klass
      }
    end
    
    @database_classes
  end
  
  def self.get_types
    return self.get_classes().keys().delete_if{
      |key|
      key.to_s == ""
    }
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end
  
  def self.subclasses
    @subclasses
  end
end
