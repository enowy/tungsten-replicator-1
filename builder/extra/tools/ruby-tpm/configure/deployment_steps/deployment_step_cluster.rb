module ConfigureDeploymentStepDeployment
  def get_methods
    [
      ConfigureDeploymentMethod.new("create_release", 0, -40),
      ConfigureDeploymentMethod.new("deploy_external_libraries"),
      ConfigureCommitmentMethod.new("commit_release")
    ]
  end
  module_function :get_methods
  
  def create_release
    prepare_dir = get_deployment_basedir()
    mkdir_if_absent("#{@config.getProperty(HOME_DIRECTORY)}/share")
    mkdir_if_absent(@config.getProperty(LOGS_DIRECTORY))
    mkdir_if_absent(@config.getProperty(METADATA_DIRECTORY))
    mkdir_if_absent(@config.getProperty(RELEASES_DIRECTORY))
    
    # Determine if we need to copy this directory into place or
    # if we are updating the existing installed directory
    copy_release = true
    if @config.getProperty(HOME_DIRECTORY) == Configurator.instance.get_base_path()
      copy_release = false
    end
    if "#{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME}/#{Configurator.instance.get_basename()}" == Configurator.instance.get_base_path()
      copy_release = false
    end
    if File.exists?(prepare_dir)
      copy_release = false
    end

    if copy_release == true
      # Copy the software into the home directory
      FileUtils.rmtree(File.dirname(prepare_dir))
      mkdir_if_absent(File.dirname(prepare_dir))
      
      package_path = Configurator.instance.get_base_path()
      debug("Copy #{package_path} to #{prepare_dir}")
      FileUtils.cp_r(package_path, prepare_dir, :preserve => true)
      
      host_transformer("#{File.dirname(prepare_dir)}/commit.sh") {
        |t|
        t.mode(0755)
        t.watch_file?(false)
        t.set_template("cluster-home/samples/bin/commit.sh.tpl")
      }
    end
    
    File.open(@config.getProperty(DIRECTORY_LOCK_FILE), "w") {
      |f|
      f.puts(@config.getProperty(HOME_DIRECTORY))
    }
    
    # Reset the .watchfiles file before rewriting all configuration files
    FileUtils.rm_f("#{prepare_dir}/.watchfiles")
    FileUtils.rm_f("#{prepare_dir}/.changedfiles")
    
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/datasource")
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/service")
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/extension")
    if is_replicator?()
      FileUtils.cp("#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.service.properties", 
        "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/replicator.properties")
    end
    
    DeploymentFiles.prompts.each{
      |p|
      if @config.getProperty(p[:local]) != nil && File.exist?(@config.getProperty(p[:local]))
        target = @config.getTemplateValue(p[:local])
        mkdir_if_absent(File.dirname(target))
    		FileUtils.cp(@config.getProperty(p[:local]), target)
    	end
    }
    
    # Create the share/env.sh script
    script = "#{@config.getProperty(HOME_DIRECTORY)}/#{CONTINUENT_ENVIRONMENT_SCRIPT}"
    debug("Generate environment at #{script}")
    host_transformer(script) {
      |t|
      t.set_template("cluster-home/samples/conf/env.sh.tpl")
      t.mode(0755)
    }
    
    # Create the share/aliases.sh script
    script = "#{@config.getProperty(HOME_DIRECTORY)}/share/aliases.sh"
    if @config.getProperty(EXECUTABLE_PREFIX) != ""
      host_transformer(script) {
        |t|
        t.set_template("cluster-home/samples/conf/aliases.sh.tpl")
        t.mode(0755)
      }
    else
      if File.exists?(script)
        FileUtils.rm_f(script)
      end
    end
    
    # Write the tungsten-cookbook/INSTALLED* files
    write_tungsten_cookbook_installed_recipe()
    
    # Remove any deploy.cfg files in the installed directory
    FileUtils.rm(Dir.glob("#{prepare_dir}/#{Configurator::DATASERVICE_CONFIG}*"))
    
    # Write the tungsten.cfg file to the installed directory as a record
    # of the configuration at this time
    config_file = prepare_dir + '/' + Configurator::HOST_CONFIG
    debug("Write #{config_file}")
    host_config = @config.dup()
    ph = ConfigurePromptHandler.new(host_config)
    ph.prepare_saved_server_config()
    host_config.store(config_file)

    external_type = @config.getNestedProperty([DEPLOYMENT_EXTERNAL_CONFIGURATION_TYPE])
    external_source = @config.getNestedProperty([DEPLOYMENT_EXTERNAL_CONFIGURATION_SOURCE])
    installed_tungsten_ini = "#{prepare_dir}/tungsten.ini"
    if external_type == "ini" && File.exists?(external_source)
      # Create a symlink of the INI file into the installed directory
      if File.exists?(installed_tungsten_ini)
        FileUtils.rm_f(installed_tungsten_ini)
      end
      FileUtils.ln_sf(external_source, installed_tungsten_ini)
    end
  end
  
  def write_tungsten_security_files
    # Write the cluster-home/conf/security.properties file
    transform_host_template("cluster-home/conf/security.properties",
      "cluster-home/samples/conf/security.properties.tpl")
      
    rmi_user = @config.getProperty(RMI_USER)
    
    ks = @config.getTemplateValue(JAVA_KEYSTORE_PATH)
    ks_pass = @config.getProperty(JAVA_KEYSTORE_PASSWORD)
    
    ts = @config.getTemplateValue(JAVA_TRUSTSTORE_PATH)
    ts_pass = @config.getProperty(JAVA_TRUSTSTORE_PASSWORD)
    
    tls_alias = @config.getProperty(JAVA_TLS_ENTRY_ALIAS)
    tls_keystore = @config.getTemplateValue(JAVA_TLS_KEYSTORE_PATH)
    if File.exist?(tls_keystore)
      keystore = JavaKeytool.new(ks)
      keystore.import(tls_keystore, tls_alias, ks_pass)
      WatchFiles.watch_file(ks, @config)
      
      truststore = JavaKeytool.new(ts)
      truststore.trust(tls_keystore, tls_alias, ks_pass, ts_pass)
      WatchFiles.watch_file(ts, @config)
    end
    
    if File.exist?(ks)
      jmxremote = @config.getTemplateValue(JAVA_JMXREMOTE_ACCESS_PATH)
      File.open(jmxremote, "w") {
        |f|
        f.puts("#{rmi_user}        readwrite \\
  create javax.management.monitor.*,javax.management.timer.* \\
  unregister")
      }
      WatchFiles.watch_file(jmxremote, @config)
    
      password_store = @config.getTemplateValue(JAVA_PASSWORDSTORE_PATH)
    
      ks_pass_file = Tempfile.new("ks")
      ks_pass_file.puts(ks_pass)
      ks_pass_file.close()
    
      ts_pass_file = Tempfile.new("ts")
      ts_pass_file.puts(ts_pass)
      ts_pass_file.close()
    
      user_pass_file = Tempfile.new("user")
      user_pass_file.puts(ks_pass)
      user_pass_file.close()
    
      tpasswd = "#{Configurator.instance.get_base_path()}/cluster-home/bin/tpasswd"
      tpasswd_options = [
        "-c #{rmi_user}", "-e",
        "-p #{password_store}",
        "-upf #{ks_pass_file.path()}",
        "-ts #{ts}",
        "-tspf #{ts_pass_file.path()}"
      ]
    
      cmd_result("#{tpasswd} #{tpasswd_options.join(' ')}")
      cmd_result("#{tpasswd} #{tpasswd_options.join(' ')} -target rmi_jmx")
      WatchFiles.watch_file(password_store, @config)
    end
  end
  
  def write_tungsten_cookbook_installed_recipe
    unless Configurator.instance.is_enterprise?()
      return
    end
    
    debug("Write INSTALLED cookbook scripts")
    
    dscount=1
    dsids={}
    dscommands={}
    nodeid=1
    listed_nodes = []
	  
	  # Go through all dataservices and set the composite master to DS_NAME1
    @config.getPropertyOr(DATASERVICES, []).each_key{
      |ds_alias|
      if ds_alias == DEFAULTS
        next
      end
      
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_PARENT_DATASERVICE]) != nil
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE]) == nil
          ds_name=@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          dsids[ds_name] = dscount
          dscount = dscount+1
        end
      end
    }
    
    host_transformer("cookbook/INSTALLED_USER_VALUES.sh") {
      |t|
      t.mode(0755)
      t.set_template("cookbook/samples/INSTALLED_USER_VALUES.tpl")
      
      @config.getPropertyOr(DATASERVICES, []).each_key{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          ds_name=@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          t << "export COMPOSITE_DS=#{ds_name}"
          dscommands[ds_name] = "configure $COMPOSITE_DS"
        else
          ds_name=@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          master=@config.getProperty([DATASERVICES, ds_alias, DATASERVICE_MASTER_MEMBER])
          slaves=@config.getProperty([DATASERVICES, ds_alias, DATASERVICE_REPLICATION_MEMBERS]).split(",")-[master]
          connectors=@config.getProperty([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS])

          if dsids.has_key?(ds_name)
            dsid = dsids[ds_name]
          else
            dsid = dscount
            dsids[ds_name] = dscount
            dscount = dscount+1
          end
          
          dscommands[ds_name] = "configure $DS_NAME#{dsid}"
          
          t << "export DS_NAME#{dsid}=#{ds_name}"
          t << "export MASTER#{dsid}=#{master}"
          t << "export SLAVES#{dsid}=#{slaves.join(",")}"
          t << "export CONNECTORS#{dsid}=#{connectors}"

          [
            DATASERVICE_MASTER_MEMBER,
            DATASERVICE_REPLICATION_MEMBERS,
            DATASERVICE_CONNECTORS,
            DATASERVICE_WITNESSES
          ].each{
            |key|
            @config.getProperty([DATASERVICES, ds_alias, key]).to_s().split(",").each{
              |node|
              if listed_nodes.include?(node)
                next
              end

              t << "export NODE#{nodeid}=#{node}"
              nodeid = nodeid+1
              listed_nodes << node
            }
          }
        end
      }
    }
  
    File.open("#{get_deployment_basedir()}/cookbook/INSTALLED.tmpl", "w") {
      |f|
      f.puts <<EOF
##################################
# DO NOT MODIFY THIS FILE
##################################
# Loads environment variables to fill in the cookbook
# . cookbook/INSTALLED_USER_VALUES.sh

EOF
      rec = ReverseEngineerCommand.new(@config)
      commands = rec.build_commands(@config)
      
      # Update the tpm configure commands to use environment variables
      # that were defined in INSTALLED_USER_VALUES.sh
      commands.map!{
        |cmd|
        cmd.gsub(/configure ([a-zA-Z0-9_]+)/){
          |match|
          if dscommands.has_key?($1)
            dscommands[$1]
          else
            "configure #{$1}"
          end
        }
      }
      f.puts(commands.join("\n"))
    }
    WatchFiles.watch_file("#{get_deployment_basedir()}/cookbook/INSTALLED.tmpl", @config)
  end
  
  def commit_release
    trigger_event(:before_commit_release)
    prepare_dir = @config.getProperty(PREPARE_DIRECTORY)
    target_dir = @config.getProperty(TARGET_DIRECTORY)
    
    if @config.getProperty(PROFILE_SCRIPT).to_s() != ""
      profile_path = File.expand_path(@config.getProperty(PROFILE_SCRIPT), @config.getProperty(HOME_DIRECTORY))

      if File.exist?(profile_path)
        matching_lines = cmd_result("grep 'Tungsten Environment for #{@config.getProperty(HOME_DIRECTORY)}$' #{profile_path} | wc -l")
        case matching_lines.to_i()
        when 2
          debug("Tungsten env.sh is already included in #{profile_path}")
        when 0
          begin
            f = File.open(profile_path, "a")
            f.puts("")
            f.puts("# Begin Tungsten Environment for #{@config.getProperty(HOME_DIRECTORY)}")
            f.puts("# Include the Tungsten variables")
            f.puts("# Anything in this section may be changed during the next operation")
            f.puts("if [ -f \"#{@config.getProperty(HOME_DIRECTORY)}/share/env.sh\" ]; then")
            f.puts("    . \"#{@config.getProperty(HOME_DIRECTORY)}/share/env.sh\"")
            f.puts("fi")
            f.puts("# End Tungsten Environment for #{@config.getProperty(HOME_DIRECTORY)}")
          ensure
            f.close()
          end
        else
          error("Unable to add the Tungsten environment to #{profile_path}.  Remove any lines from '# Begin Tungsten Environment' to '# End Tungsten Environment'.")
        end
        
        # Remove any sections from previous versions that don't list their related directory
        matching_lines = cmd_result("grep 'Begin Tungsten Environment$' #{profile_path} | wc -l")
        if matching_lines.to_i() == 1
          file_contents = nil
          File.open(profile_path, "r") {
            |f|
            file_contents = f.readlines()
          }
          
          # Look for the beginning string and remove the next 6 lines
          idx = file_contents.index("# Begin Tungsten Environment\n")
          if idx != nil
            end_idx = idx+6
            # Make sure this matches the format we are expecting
            if file_contents[end_idx] == "# End Tungsten Environment\n"
              while idx <= end_idx do
                file_contents[idx] = nil
                idx = idx+1
              end
            end
          end
          
          File.open(profile_path, "w") {
            |f|
            f.puts(file_contents.join())
          }
        end
      else
        error("Unable to add the Tungsten environment to #{profile_path} because the file does not exist.")
      end
    end
    
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      current_release_target_dir = File.readlink(current_release_directory)
    else
      current_release_target_dir = nil
    end
    
    # Copy dynamic configuration information from the old directory to this one
    unless target_dir == current_release_target_dir
      if File.exists?(current_release_directory)    
        if current_release_target_dir
          manager_dynamic_properties = current_release_target_dir + '/tungsten-manager/conf/dynamic.properties'
          if File.exists?(manager_dynamic_properties)
            info("Copy the previous manager dynamic properties")
            FileUtils.mv(manager_dynamic_properties, prepare_dir + '/tungsten-manager/conf')
            Configurator.instance.limit_file_permissions(prepare_dir + '/tungsten-manager/conf/dynamic.properties')
          end
          
          Dir.glob("#{current_release_target_dir}/tungsten-replicator/conf/dynamic-*.properties") {
            |replicator_dynamic_properties|
            basename = File.basename(replicator_dynamic_properties)
            info("Copy the previous replicator dynamic properties - #{File.basename(replicator_dynamic_properties)}")
            FileUtils.mv(replicator_dynamic_properties, prepare_dir + "/tungsten-replicator/conf")
            Configurator.instance.limit_file_permissions(prepare_dir + "/tungsten-replicator/conf/#{basename}")
          }
          
          cluster_home_conf = current_release_target_dir + '/cluster-home/conf'
          if File.exists?(cluster_home_conf)
            info("Copy the previous cluster conf properties")
            
            cluster_home_conf_cluster = current_release_target_dir + '/cluster-home/conf/cluster'
            if File.exists?(cluster_home_conf_cluster)
              cmd_result("rsync -Ca --exclude=service/* --exclude=extension/* #{cluster_home_conf_cluster} #{prepare_dir}/cluster-home/conf")
            end
            
            dataservices_properties = current_release_target_dir + '/cluster-home/conf/dataservices.properties'
            if File.exists?(dataservices_properties)
              FileUtils.cp(dataservices_properties, prepare_dir + '/cluster-home/conf')
            end
            
            statemap_properties = current_release_target_dir + '/cluster-home/conf/statemap.properties'
            if File.exists?(statemap_properties) && @config.getProperty(SKIP_STATEMAP) == "false"
              FileUtils.cp(statemap_properties, prepare_dir + '/cluster-home/conf')
            end
          end
          
          connector_user_map = current_release_target_dir + '/tungsten-connector/conf/user.map'
          if File.exists?(connector_user_map) && @config.getProperty(CONN_DELETE_USER_MAP) == "false" && @config.getProperty(ENABLE_CONNECTOR_BRIDGE_MODE) == "false"
            info("Copy the previous connector user map")
            FileUtils.cp(connector_user_map, prepare_dir + '/tungsten-connector/conf')
          end
          
          FileUtils.touch(current_release_target_dir)
        end
      end
      
      if is_manager?()
        # This will create the datasource files for any composite dataservices
        # that weren't copied over in the rsync commands above
        initiate_composite_dataservices()
      end
      
      unless target_dir == prepare_dir
        debug("Move the prepared directory to #{target_dir}")
        FileUtils.mv(prepare_dir, target_dir)
      end
      
      debug("Create symlink to #{target_dir}")
      FileUtils.rm_f(current_release_directory)
      FileUtils.ln_s(target_dir, current_release_directory)
    end
    
    # Update the mtime for the directory so sorting is easier
    FileUtils.touch(target_dir)
    
    # Remove some old directories that are no longer used
    if File.exists?(@config.getProperty(CONFIG_DIRECTORY))
      FileUtils.rmtree(@config.getProperty(CONFIG_DIRECTORY))
    end
    if File.exists?(@config.getProperty(HOME_DIRECTORY) + "/configs")
      FileUtils.rmtree(@config.getProperty(HOME_DIRECTORY) + "/configs")
    end
    if File.exists?(@config.getProperty(HOME_DIRECTORY) + "/service-logs")
      FileUtils.rmtree(@config.getProperty(HOME_DIRECTORY) + "/service-logs")
    end
    
    FileUtils.cp(current_release_directory + '/' + Configurator::HOST_CONFIG, current_release_directory + '/.' + Configurator::HOST_CONFIG + '.orig')
    
    if is_manager?() || is_connector?()
      write_dataservices_properties()
      write_router_properties()
      write_policymgr_properties()
    end
    
    # Update the THL URI in dynamic properties so it uses the correct protocol
    if is_replicator?()
      @config.getPropertyOr(REPL_SERVICES, {}).keys().each{
        |rs_alias|

        if rs_alias == DEFAULTS
          next
        end
        
        dynamic_properties = @config.getProperty([REPL_SERVICES, rs_alias,REPL_SVC_DYNAMIC_CONFIG])
        if File.exists?(dynamic_properties)
          if @config.getProperty([REPL_SERVICES, rs_alias, REPL_ENABLE_THL_SSL]) == "true"
            cmd_result("sed -i 's/uri=thl\\\\/uri=thls\\\\/' #{dynamic_properties}")
          else
            cmd_result("sed -i 's/uri=thls\\\\/uri=thl\\\\/' #{dynamic_properties}")
          end
        end
      }
      
      Dir.glob("#{@config.getProperty(REPL_LOG_DIR)}/**/*") {
        |thl_file|
        Configurator.instance.limit_file_permissions(thl_file)
      }
    end
    trigger_event(:after_commit_release)
    
    if is_replicator?()
      add_service("tungsten-replicator/bin/replicator", 25)
    end
    if is_manager?()
      add_service("tungsten-manager/bin/manager", 50)
    end
    if is_connector?()
      add_service("tungsten-connector/bin/connector", 75)
    end
    trigger_event(:register_services)
    
    
    write_deployall()
    write_undeployall()
    write_startall()
    write_stopall()
    write_tungsten_security_files()
  end
  
  def write_dataservices_properties
    dataservices_file = "#{@config.getProperty(TARGET_DIRECTORY)}/cluster-home/conf/dataservices.properties"
    dataservices = {}
    if (File.exists?(dataservices_file))
      File.open(dataservices_file, "r") {
        |f|
        f.readlines().each{
          |line|
          parts = line.split("=")
          if parts.size() == 2
            dataservices[parts[0]] = parts[1]
          end
        }
      }
    end
    
    @config.getPropertyOr(DATASERVICES, {}).each_key{
      |ds_alias|
      if ds_alias == DEFAULTS
        next
      end
      
      topology = Topology.build(ds_alias, @config)
      unless topology.use_management?()
        next
      end
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        next
      end
      unless include_dataservice?(ds_alias)
        next
      end
      unless (
          @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).include_alias?(get_host_alias()) ||
          @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS]).include_alias?(get_host_alias())
        )
        next
      end

      dataservices[@config.getTemplateValue([DATASERVICES, ds_alias, DATASERVICENAME])] = @config.getTemplateValue([DATASERVICES, ds_alias, DATASERVICE_MEMBERS])
      @config.getPropertyOr(DATASERVICES, {}).each_key{
        |cds_alias|
        unless @config.getProperty([DATASERVICES, cds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end
        
        composite_members = @config.getPropertyOr([DATASERVICES, cds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",")
        if composite_members.include?(ds_alias)
          composite_members.each{
            |cm_alias|
            dataservices[@config.getTemplateValue([DATASERVICES, cm_alias, DATASERVICENAME])] = @config.getTemplateValue([DATASERVICES, cm_alias, DATASERVICE_MEMBERS])
          }
        end
      }
    }

    File.open(dataservices_file, "w") {
      |f|
      dataservices.each{
        |ds_alias,managers|
        f.puts "#{ds_alias}=#{managers}"
      }
    }
  end

  def write_router_properties
    transform_host_template("cluster-home/conf/router.properties",
      "tungsten-connector/samples/conf/router.properties.tpl")
  end
  
  def write_policymgr_properties
    transform_host_template("cluster-home/conf/policymgr.properties",
      "tungsten-connector/samples/conf/policymgr.properties.tpl")
  end
  
  def deploy_external_libraries
    external_libraries = {}
    @config.getPropertyOr(REPL_SERVICES, {}).keys().each{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      rs_prefix = [REPL_SERVICES, rs_alias]
      ds_alias = @config.getProperty(rs_prefix + [DEPLOYMENT_DATASERVICE])
      topology = Topology.build(ds_alias, @config)
      
      if topology.enable_dedicated_extractor_datasource?() == true
        ds = ConfigureDatabasePlatform.build(rs_prefix, @config, true)
        include_external_libraries_in_hash(ds, external_libraries)
      end
      
      ds = ConfigureDatabasePlatform.build(rs_prefix, @config)
      include_external_libraries_in_hash(ds, external_libraries)
    }
    
    external_libraries.each{
      |source, targets|
      targets.each{
        |target|
        target = File.expand_path(target, get_deployment_basedir())
        debug("Copy #{source} to #{target}")
        FileUtils.cp(source, target)
      }
    }
  end
  
  def include_external_libraries_in_hash(ds, external_libraries)
    ds_libraries = ds.getExternalLibraries()
    if ds_libraries.is_a?(Hash)
      ds_libraries.each{
        |source, target|
        if external_libraries.has_key?(source)
          external_libraries[source] << target
          external_libraries[source].uniq!()
        else
          external_libraries[source] = [target]
        end
      }
    elsif ds_libraries != nil
      error("The #{ds.class.name}.getExternalLibraries method did not return a hash")
    end
  end
end