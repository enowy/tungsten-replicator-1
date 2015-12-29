DBMS_VERTICA = "vertica"

class VerticaDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_VERTICA
  end
 
  def get_thl_uri
	  "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_VERTICA_DBNAME)}"
  end
  
  def get_default_port
    "5433"
  end
  
  def get_default_start_script
    nil
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def enable_applier_filter_pkey?
    false
  end
  
  def enable_applier_filter_bidiSlave?
    false
  end
  
  def enable_applier_filter_colnames?
    false
  end
  
  def getBasicJdbcUrl()
    "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcUrl()
    "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_VERTICA_DBNAME)}"
  end
  
  def getJdbcDriver()
    "com.vertica.Driver"
  end
  
  def getVendor()
    "postgresql"
  end

  # Vertica does not have an extractor. 
  def get_extractor_template
    raise "Unable to use VerticaDatabasePlatform as an extractor"
  end
end

class VerticaUserGroupsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck

  def set_vars
    @title = "Vertica user groups check"
  end

  def validate
    info("Checking that the Vertica user has correct OS group membership")

    error_text = %{
      Unable to determine the user that is running Vertica server. This is needed to check that this user 
      is a member of the tungsten user's primary group.
      If you are sure that this is configured, you may skip
      this check by adding 'skip-validation-check=VerticaGroupsCheck' to the configuration.
    }.gsub(/\s+/, " ").strip

    # Get the user that Vertica is running as
    begin
      vertica_user = cmd_result("ps -o user -p $(pgrep -n -d, -x vertica)").split("\n")[-1].strip()
      if vertica_user.to_s() == '' 
        error(error_text)
      end
    rescue CommandError
      error(error_text)
    end

    # Find the tungsten user from the configuration
    tungsten_user = @config.getProperty("user")

    # Get the tungsten user's primary group
    tungsten_group  = cmd_result("id -ng #{tungsten_user}")

    # Check that the vertica user is a member of the tungsten users primary group
    begin
      cmd_result("id -nG #{vertica_user} | grep -qw #{tungsten_group}")
    rescue CommandError
      error("The OS user that runs Vertica Server needs to be a member of the tungsten user's primary group.")
    end
  end

  def enabled?
      is_enabled = true

      # If DISABLE_SECURITY_CONTROLS is set disable check
      if @config.getProperty(DISABLE_SECURITY_CONTROLS) == "true"
        is_enabled = false
      end
      # If FILE_PROTECTION_LEVEL is set to none disable check
      if @config.getProperty(FILE_PROTECTION_LEVEL) == "none"
        is_enabled = false
      end
    super() && get_applier_datasource().is_a?(VerticaDatabasePlatform) && is_enabled
  end
end

#
# Prompts
#

REPL_VERTICA_DBNAME = "repl_vertica_dbname"
class VerticaDatabaseName < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_VERTICA_DBNAME, "Name of the database to replicate into",
     PV_ANY)
 end
   
 def enabled?
   super() && get_applier_datasource().is_a?(VerticaDatabasePlatform)
 end
 
 def enabled_for_config?
   super() && get_applier_datasource().is_a?(VerticaDatabasePlatform)
 end
end
