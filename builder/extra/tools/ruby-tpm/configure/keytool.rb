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
# Initial developer(s): Jeff Mace

require "open4"
require "digest"

class JavaKeytool
  NAME = :name
  STARTDATE = :start
  ENDDATE = :end
  OWNER = :owner
  ISSUER = :issuer
  CREATION_DATE = :creation_date
  MD5 = :md5
  SHA1 = :sha1
  SHA256 = :sha256
  
  TYPE_JKS = "JKS"
  TYPE_JCEKS = "JCEKS"
  
  def initialize(keystore, type = nil)
    @keystore = keystore
    
    if type == nil
      type = JavaKeytool::TYPE_JKS
    end
    @type = type
    
    @name_regex ||= Regexp.compile(/^Alias name: ([a-zA-Z0-9]+)\n/)
    @owner_regex ||= Regexp.compile(/^Owner: ([a-zA-Z0-9=, ]+)\n/)
    @issuer_regex ||= Regexp.compile(/^Issuer: ([a-zA-Z0-9=, ]+)\n/)
    @creation_regex ||= Regexp.compile(/^Creation date: ([a-zA-Z0-9, ]+)\n/)
    @md5_regex ||= Regexp.compile(/MD5:[ ]+([A-F0-9:]+)\n/)
    @sha1_regex ||= Regexp.compile(/SHA1:[ ]+([A-F0-9:]+)\n/)
    @sha256_regex ||= Regexp.compile(/SHA256:[ ]+([A-F0-9:]+)\n/)
  end
  
  def count(password)
    # Create a counter
    c = 0
    
    if @keystore.to_s() == ""
      raise "The keystore value is required"
    end
    
    begin
      raw = self.cmd("keytool -list -v -keystore #{@keystore} -storetype #{@type}", password).split("*******************************************\n*******************************************")      
      raw.each{
        |key|
        c = c+1
      }
    end
    
    return c
  end
  
  def list(password)
    # Create an array to hold all listed certs
    r = {}
    
    if @keystore.to_s() == ""
      raise "The keystore value is required"
    end
    
    begin
      raw = self.cmd("keytool -list -v -keystore #{@keystore} -storetype #{@type}", password).split("*******************************************\n*******************************************")      
      raw.each{
        |key|
        entry = self.parse_key_listing(key)
        r[entry[JavaKeytool::NAME]] = entry
      }
    end
    
    return r
  end
  
  def genkey(keyalias, password, lifetime)
    parts = ["keytool -genkey -alias #{keyalias}",
      "-keyalg RSA -keystore #{@keystore}",
      "-validity #{lifetime}",
      "-storetype #{@type}",
      "-dname \"cn=Continuent, ou=IT, o=VMware, c=US\""]
      
    self.cmd(parts.join(" "), [password, password])
    
    listing = self.list(password)
    if listing.has_key?(keyalias)
      return true
    else
      raise "Unable to generate a key pair"
    end
  end
  
  def genseckey(keyalias, password)
    parts = ["keytool -genseckey -alias #{keyalias}",
      "-keyalg Blowfish -keysize 56",
      "-keystore #{@keystore} -storetype #{@type}"]
      
    self.cmd(parts.join(" "), [password, password])
    
    listing = self.list(password)
    if listing.has_key?(keyalias)
      return true
    else
      raise "Unable to generate a secure key"
    end
  end
  
  def test_key_password(store_password, key_alias, key_password)
    begin
      cmd = Configurator.instance.get_base_path() + "/cluster-home/bin/tkeystoremanager"
      parts = ["#{cmd} -c",
        "-ks #{@keystore} -kt #{@type} -ka #{key_alias}"]
      self.cmd(parts.join(" "), [store_password, key_password])
      return true
    rescue CommandError => ce
      Configurator.instance.debug(ce)
      raise "Invalid keystore password, key alias or key password"
    end
  end
  
  def import(source, keyalias, password)
    if File.exist?(@keystore)
      listing = self.list(password)
      if listing.has_key?(keyalias)
        parts = ["keytool -delete -alias #{keyalias}",
      	  "-keystore #{@keystore}"]
      	self.cmd(parts.join(" "), password)
      end
    end

    parts = ["keytool -importkeystore -noprompt",
    	"-srckeystore #{source}",
    	"-destkeystore #{@keystore}",
    	"-srcalias #{keyalias} -destalias #{keyalias}"]
    self.cmd(parts.join(" "), [password, password, password])
    
    listing = self.list(password)
    if listing.has_key?(keyalias)
      return true
    else
      raise "Unable to import keystore key"
    end
  end
  
  def trust(source, keyalias, source_password, password)
    if File.exist?(@keystore)
      listing = self.list(password)
      if listing.has_key?(keyalias)
        parts = ["keytool -delete -alias #{keyalias}",
      	  "-keystore #{@keystore}"]
      	self.cmd(parts.join(" "), password)
      end
    end
    
    local_cert = Tempfile.new("tcfg")
    local_cert.close()
    File.unlink(local_cert.path())
    
    parts = ["keytool -export -alias #{keyalias}",
      "-file #{local_cert.path()}",
      "-keystore #{source}"]
    self.cmd(parts.join(" "), source_password)
    
    parts = ["keytool -import -noprompt -trustcacerts -alias #{keyalias}",
      "-file #{local_cert.path()} -keystore #{@keystore}"]
    self.cmd(parts.join(" "), [password, password])
    
    unless File.exists?(@keystore)
      raise "Unable to trust keystore certificate"
    end
    
    listing = self.list(password)
    if listing.has_key?(keyalias)
      return true
    else
      raise "Unable to import the trusted keystore certificate"
    end
  end
  
  def parse_key_listing(raw)
    entry = {}
    
    case @type
    when JavaKeytool::TYPE_JKS
      self.match_to_entry(raw, entry, @name_regex, 1, JavaKeytool::NAME)
      self.match_to_entry(raw, entry, @creation_regex, 1, JavaKeytool::CREATION_DATE)
      self.match_to_entry(raw, entry, @owner_regex, 1, JavaKeytool::OWNER)
      self.match_to_entry(raw, entry, @issuer_regex, 1, JavaKeytool::ISSUER)
      self.match_to_entry(raw, entry, @md5_regex, 1, JavaKeytool::MD5)
      self.match_to_entry(raw, entry, @sha1_regex, 1, JavaKeytool::SHA1)
      self.match_to_entry(raw, entry, @sha256_regex, 1, JavaKeytool::SHA256)
    when JavaKeytool::TYPE_JCEKS
      self.match_to_entry(raw, entry, @name_regex, 1, JavaKeytool::NAME)
      self.match_to_entry(raw, entry, @creation_regex, 1, JavaKeytool::CREATION_DATE)
      
      require 'digest'
      File.open(@keystore, "r") {
        |f|
        content = f.readlines().join()
        entry[JavaKeytool::MD5] = Digest::MD5.hexdigest(content)
      }
    else
      raise "Unable to support the #{@type} keystore format"
    end
    
    if entry[JavaKeytool::NAME] == nil
      raise "Unable to parse keytool entry because a valid name does not exist"
    end
    
    return entry
  end
  
  def match_to_entry(raw, entry, regex, matchindex, storeas)
    match = regex.match(raw)
    if match != nil
      entry[storeas] = match[matchindex]
    end
  end
  
  def cmd(command, password = nil, ignore_fail = false)
    errors = ""
    result = ""
    threads = []
    
    Configurator.instance.debug("Execute `#{command}`")
    status = Open4::popen4("export LANG=en_US; #{command}") do |pid, stdin, stdout, stderr|
      if password.is_a?(Array)
        stdin.puts(password.join("\n"))
      elsif password != nil
        stdin.puts(password)
      end
      stdin.close
      
      threads << Thread.new{
        while data = stdout.gets()
          if data.to_s() != ""
            result+=data
          end
        end
      }
      threads << Thread.new{
        while edata = stderr.gets()
          if edata.to_s() != ""
            errors+=edata
          end
        end
      }
      
      threads.each{|t| t.join() }
    end
    
    result.strip!()
    errors.strip!()
    
    original_errors = errors
    rc = status.exitstatus
    
    if errors == ""
      errors = "No STDERR"
    else
      errors = "Errors: #{errors}"
    end
    
    Configurator.instance.debug("RC: #{rc}, Result: #{result}, #{errors}")
    
    if rc != 0 && ! ignore_fail
      raise CommandError.new(command, rc, result, original_errors)
    end

    return result
  end
end