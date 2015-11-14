module MonitorUnsecuredFiles
  def self.find_unsecured_files(dir)
    base = Configurator.instance.get_base_path()
    
    if Configurator.instance.is_locked?()
      max_perms = sprintf("%04d", 777-sprintf("%o", File.umask()).to_i())
    else
      max_perms = sprintf("%o", File.stat(base).mode)
    end
    max_user=max_perms[-3,1].to_i()
    max_group=max_perms[-2,1].to_i()
    max_other=max_perms[-1,1].to_i()
    
    tested_modes = {}
    unsecured_files = []
    
    all_files = Dir.glob("#{dir}/**/*", File::FNM_DOTMATCH)
    all_files.concat(Dir.glob("#{dir}/*", File::FNM_DOTMATCH))
    all_files.each {
      |f|
      mode = File.stat(f).mode
      if tested_modes.has_key?(mode)
        if tested_modes[mode] == false
          unsecured_files << f
          next
        end
      end
      
      begin
        octal_mode = sprintf("%o", mode)
        
        file_user=octal_mode[-3,1].to_i()
        match_user = max_user & file_user
        if match_user != file_user
          raise "Invalid user permission"
        end
        
        file_group=octal_mode[-2,1].to_i()
        match_group = max_group & file_group
        if match_group != file_group
          raise "Invalid group permission"
        end
        
        file_other=octal_mode[-1,1].to_i()
        match_other = max_other & file_other
        if match_other != file_other
          raise "Invalid other permission"
        end
            
        tested_modes[mode] = true
      rescue
        unsecured_files << f
        tested_modes[mode] = false
      end
    }
    
    return unsecured_files
  end
end