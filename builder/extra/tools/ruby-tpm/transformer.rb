#
# VMware Continuent Tungsten Replicator
# Copyright (C) 2015 VMware, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#      
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Simple utility class to transform files using regular expressions
require 'system_require'
system_require 'date'
system_require 'digest/md5'

class Transformer
  DEFAULT_VALUE = "{default}"
  
  def add_fixed_replacement(key, value)
    if value == nil
      raise("Unable to add a fixed replacement using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_replacements.delete(key)
    else
      @fixed_replacements[key] = value
    end
  end
  
  def add_fixed_addition(key, value)
    if value == nil
      raise("Unable to add a fixed addition using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_additions.delete(key)
    else
      @fixed_additions[key] = value
    end
  end
  
  def add_fixed_match(key, value)
    if value == nil
      raise("Unable to add a fixed match using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_matches.delete(key)
      return
    end
      
    match = value.split("/")
    match.shift
    
    if match.size != 2
      raise("Unable to add a fixed match using '#{value}'.  Matches must be in the form of /search/replacement/.")
    end
    
    @fixed_matches[key] = match
  end
  
  def set_fixed_properties(fixed_properties = [])
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    if fixed_properties == nil
      fixed_properties = []
    end
    
    fixed_properties.each{
      |val|
      
      unless val =~ /=/
        raise "Invalid value #{val} given for '--property'.  There should be a key/value pair joined by a single =."
      end
      
      val_parts = val.split("=")
      last_char=val_parts[0][-1,1]
      key=val_parts.shift()
      
      key_parts = key.split(":")
      if key_parts.size() == 2
        unless @outfile && @outfile.include?(key_parts[0])
          next
        end
        
        key = key_parts[1]
      elsif key_parts.size() == 1
        # Do nothing
      else
        raise "Invalid search key #{key} given for '--property'. There may only be a single ':' in the search key."
      end
        
      
      if last_char == "+"
        add_fixed_addition(key[0..-2], val_parts.join("="))
      elsif last_char == "~"
        add_fixed_match(key[0..-2], val_parts.join("="))
      else
        add_fixed_replacement(key, val_parts.join("="))
      end
    }
  end
  
  def initialize(config, outfile = nil)
    @config = config
    @outfile = outfile
    @transform_values_method = nil

    @mode = nil
    @timestamp = true
    @watch_file = true
    
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    @output = []
  end
  
  # Enable/Disable the header timestamp of when the file was created
  def timestamp?(v = nil)
    if v != nil
      @timestamp = v
    end
    
    @timestamp
  end
  
  # Enable/Disable support for identifying user modifications to this file
  def watch_file?(v = nil)
    if v != nil
      @watch_file = v
    end
    
    @watch_file
  end
  
  # Force the file to have a specific mode
  def mode(v = nil)
    if v != nil
      @mode = v
    end
    
    @mode
  end
  
  def config(v = nil)
    if v != nil
      @config = v
    end
    
    @config
  end
  
  def set_transform_values_method(func)
    @transform_values_method = func
  end

  # Evaluate each line in the template by passing it to a code block 
  # and storing the result
  def transform_lines(&block)
    @output.map!{
      |line|
      block.call(line)
    }
  end
    
  # Evaluate a single template line and return the result
  def transform_line(line)
    # Look for --property=key=value options that will apply to this line
    line_keys = line.scan(/^[#]?([a-zA-Z0-9\._-]+)[ ]*=[ ]*.*/)
    if line_keys.length() > 0 && @fixed_replacements.has_key?(line_keys[0][0])
      "#{line_keys[0][0]}=#{@fixed_replacements[line_keys[0][0]]}"
    else
      # Look for template placeholders
      # ([\#|include|includeAll]*\()? -> A function reference to do something besides a simple substitution
      # ([A-Za-z\._]+) -> The actual template variable name
      # (\|)? -> A separator that identifies a default value
      # ([A-Za-z0-9\._\-\=\?\&]*)? -> The default value if the template variable does not have a value
      line.gsub!(/@\{([\#|include|includeAll]*\()?([A-Za-z\._]+)(\|)?([A-Za-z0-9\._\-\=\?\&]*)?[\)]?\}/){
        |match|
        functionMarker = $1
        defaultValueMarker = $3
        defaultValue = $4
        # This must be after the other lines so it doesn't overwrite the special variables
        r = @transform_values_method.call($2.split("."))
        
        # Replace this line with the content of a template 
        # returned by the template variable
        if functionMarker == "include("
          pattern = r
          r = []
          find_templates([pattern]).each {
            |template_files|
            template_files.each{
              |template|
              r << transform_file(template)
              r << ""
            }
          }
          
          r = r.join("\n")
        # Replace this line with the content of all templates found based
        # on a list of search patterns returned by the template variable
        elsif functionMarker == "includeAll("
          patterns = r
          r = []
          find_templates(patterns).each {
            |template_files|
            template_files.each{
              |template|
              r << transform_file(template)
              r << ""
            }
          }
          
          r = r.join("\n")
        else
          if r.is_a?(Array)
            r = r.join(',')
          end
        
          # There is no returned value and a default is available
          if r.to_s() == "" && defaultValueMarker == "|"
            r = defaultValue
          end
        
          # This function will create a comment if the template variable is empty
          if functionMarker == "#("
            if r.to_s() == ""
              r = "#"
            else
              r = ""
            end
          end
        end
        
        r
      }
      
      line_keys = line.scan(/^[#]?([a-zA-Z0-9\._-]+)[ ]*=(.*)/)
      if line_keys.size > 0
        # Append content to this line based on the --property=key+=value
        if @fixed_additions.has_key?(line_keys[0][0])
          line_keys[0][1] += @fixed_additions[line_keys[0][0]]
          line = line_keys[0][0] + "=" + line_keys[0][1]
        end
      
        # Modify content on this line based on the --property=key~=value
        if @fixed_matches.has_key?(line_keys[0][0])
          line_keys[0][1].sub!(Regexp.new(@fixed_matches[line_keys[0][0]][0]), @fixed_matches[line_keys[0][0]][1])
          line = line_keys[0][0] + "=" + line_keys[0][1]
        end
      end
      
      line
    end
  end
  
  # Read the contents of a file and transform each line using the current Transformer
  def transform_file(path)
    out = []
    File.open(path) do |file|
      while line = file.gets
        out << transform_line(line.chomp())
      end
    end
    
    return out.join("\n")
  end
  
  # Evaluate the template and push the contents to the outfile
  # If no file was given then the contents are returned as a string
  def output
    @output.map!{
      |line|
      transform_line(line)
    }
    
    if @outfile
      Configurator.instance.info("Writing " + @outfile)
      ChangedFiles.register(@outfile)
      
      File.open(@outfile, "w") {
        |f|
        if mode() != nil
          f.chmod(mode())
          Configurator.instance.limit_file_permissions(f.path())
        end
        
        if timestamp?()
          f.puts "# AUTO-GENERATED: #{DateTime.now}"
        end

        @output.each{
          |line| 
          f.puts(line) 
        }
      }
      
      if watch_file?()
        WatchFiles.watch_file(@outfile, @config)
      end
      
      ChangedFiles.check(@outfile, @config)
    else
      return self.to_s
    end
  end
  
  # Find a template matching the given pattern and store the contents
  # to be evaluated later
  def set_template(pattern)
    if File.expand_path(pattern) == pattern
      raise MessageError.new("Unable to use '#{pattern}' as a template because it is an absolute path.")
    end
    
    @output = []
    find_templates([pattern]).each{
      |template_files|
      template_files.each{
        |path|
        File.open(path) do |file|
          while line = file.gets
            @output << line.chomp()
          end
        end
      }
    }
  end
  
  # Find additional template content that should be added to the final file
  def find_template_addons(pattern)
    addons = []
    
    # Include a simple search path for use with --template-search-path
    simple_pattern = pattern.gsub(/\/samples\/conf/, "")
    if simple_pattern == pattern
      patterns = [pattern]
    else
      patterns = [simple_pattern, pattern]
    end
    
    get_search_directories().each {
      |dir|
      patterns.each{
        |p|
        Dir.glob("#{dir}/#{p}.addon*").each {
          |file|
          addons << file
        }
      }
    }
    
    addons
  end
  
  # Find a list of files matching the given search patterns among the available
  # template search directories. Only the first file for each basename
  # will be returned.
  def find_templates(search)
    templates = {}
    
    get_search_directories().each {
      |dir|
      search.each {
        |pattern|
        
        # Include a simple search path for use with --template-search-path
        simple_pattern = pattern.gsub(/\/samples\/conf/, "")
        if simple_pattern == pattern
          patterns = [pattern]
        else
          patterns = [simple_pattern, pattern]
        end
        
        patterns.each {
          |p|
          Dir.glob("#{dir}/#{p}") {
            |file|
            base = File.basename(file)

            # Do not store the file if it is a duplicate of another template
            unless templates.has_key?(base)
              templates[base] = [file] + find_template_addons(file[dir.length+1, file.length])
            end
          }
        }
      }
    }
    
    # Sort and return the files as an array
    template_files = []
    templates.keys().sort().each{
      |k|
      template_files << templates[k]
    }
    
    if template_files.size() == 0
      raise MessageError.new("Unable to find a template file for '#{search}'")
    end
    
    template_files
  end
  
  # Get an array of directories that may contain template files
  def get_search_directories
    dirs = []
    
    # Add any additional search paths that may be provided
    @config.getProperty(TEMPLATE_SEARCH_PATH).split(",").each {
      |path|
      if File.exists?(path) && File.directory?(path)
        dirs << path
      end
    }
    
    # These are the fallback locations for templates
    dirs << "#{@config.getProperty(HOME_DIRECTORY)}/share/templates"
    dirs << @config.getProperty(PREPARE_DIRECTORY)
    dirs
  end
  
  def get_filename
    @outfile
  end
  
  def to_s
    return to_a.join("\n")
  end
  
  def to_a
    return @output
  end
  
  def <<(line)
    @output << line
  end
end

class ChangedFiles
  def self.register(file)
    @initial_md5 ||= {}
    @initial_md5[file] = self.get_md5(file)
  end
  
  def self.initial_md5(file)
    @initial_md5 ||= {}
    @initial_md5[file]
  end
  
  def self.check(file, cfg)
    initial_md5 = self.initial_md5(file)
    if initial_md5 == nil
      self.add(file, cfg)
    else
      final_md5 = self.get_md5(file)
      if final_md5 == nil || final_md5 != initial_md5
        Configurator.instance.debug("The md5 for #{file} changed from '#{initial_md5.to_s()}' to '#{final_md5.to_s()}'")
        self.add(file, cfg)
      end
    end
  end
  
  def self.add(file, cfg)
    prepare_dir = cfg.getProperty(PREPARE_DIRECTORY)
    if file =~ /#{prepare_dir}/
      file_to_watch = file.sub(prepare_dir, "")
      if file_to_watch[0, 1] == "/"
        file_to_watch.slice!(0)
      end 
    else
      file_to_watch = file
    end
    File.open("#{prepare_dir}/.changedfiles", "a") {
      |out|
      out.puts file_to_watch
    }
  end
  
  def self.get_md5(file)
    if File.exists?(file)
      begin
        return Digest::MD5.hexdigest(cmd_result("egrep -v '^#' #{file} | egrep '.+'", false, true))
      rescue CommandError
        return nil
      end
    else
      return nil
    end
  end
end

module TransformerMethods
  def transform_host_template(path, template)
    host_transformer(path) {
      |t|
      t.set_template(template)
    }
  end
  
  def host_transformer(path = nil, &block)
    if path != nil
      path = File.expand_path(path, get_deployment_basedir())
    end
    t = Transformer.new(@config, path)
    t.set_transform_values_method(method(:transform_host_values))
    t.set_fixed_properties(@config.getProperty(get_host_key(FIXED_PROPERTY_STRINGS)))
    
    if block
      block.call(t)
      t.output()
    else
      return t
    end
  end
  
  def transform_service_template(path, template)
    service_transformer(path) {
      |t|
      t.set_template(template)
    }
  end
  
  def service_transformer(path = nil, &block)
    if path != nil
      path = File.expand_path(path, get_deployment_basedir())
    end
    t = Transformer.new(@config, path)
    t.set_transform_values_method(method(:transform_service_values))
    t.set_fixed_properties(@config.getProperty(get_service_key(FIXED_PROPERTY_STRINGS)))
    
    if block
      block.call(t)
      t.output()
    else
      return t
    end
  end
  
  def transform_host_values(matches)
	  case matches.at(0)
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      })
    end
    
    return v
	end
	
	def transform_service_values(matches)
	  case matches.at(0)
    when "APPLIER"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])))
    when "EXTRACTOR"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get("EXTRACTOR_" + matches[1])))
    when "SERVICE"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])))
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      })
    end
    
    return v
	end
	
	def get_host_key(key)
    [HOSTS, @config.getProperty(DEPLOYMENT_HOST), key]
  end
  
  def get_service_key(key)
    svc = @config.getProperty(DEPLOYMENT_SERVICE)
    if svc == nil
      raise MessageError.new("Unable to find a service key for #{Configurator.instance.get_constant_symbol(key)}")
    end
    
    [REPL_SERVICES, svc, key]
  end
  
  def get_applier_key(key)
    get_service_key(key)
  end
  
  def get_extractor_key(key)
    get_service_key(key)
  end
  
  def get_deployment_basedir
    @config.getProperty(PREPARE_DIRECTORY)
  end
end