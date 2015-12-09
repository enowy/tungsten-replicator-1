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

class DarwinIPParsePlatform < IPParsePlatform
  def self.supports_platform?(platform)
    if platform.downcase() =~ /darwin/
      true
    else
      false
    end
  end
  
  def get_raw_ip_configuration
    path = `which ifconfig 2>/dev/null`.chomp()
    if path == ""
      path = "/sbin/ifconfig"
    end
    
    results = `export LANG=en_US; #{path} -a`
    if results == false
      raise "Unable to collect IP configuration from ifconfig"
    else
      return results
    end
  end
  
  def parse(raw)
    name_regex = Regexp.compile(/^([a-zA-Z0-9]+):/)
    ether_regex = Regexp.compile(/ether (a-f0-9:)*/)
    flags_regex = Regexp.compile(/flags=([0-9]+)\<([A-Z,]+)\>/)
    inet4_regex1 = Regexp.compile(/inet ([0-9\.]+)[ ]+netmask 0x([a-f0-9]{8})[ ]+(broadcast [0-9\.]+)?/)
    inet6_regex1 = Regexp.compile(/inet6 ([a-f0-9:]+)%[a-z0-9]*[ ]*prefixlen ([0-9]+)/)
    
    all_interfaces = []
    interface = []

    raw.split("\n").each{
      |line|
      if name_regex.match(line)
        if interface.size() > 0
          all_interfaces << interface.join("\n")
        end
        interface = [line]
      else
        interface << line
      end
    }
    if interface.size() > 0
      all_interfaces << interface.join("\n")
    end

    all_interfaces.each{
      |ifconfig|
      include_interface = false
      
      begin
        if ether_regex.match(ifconfig)
          include_interface = true
        end
      rescue
        # Catch the exception and move on
      end
      
      if include_interface == false
        next
      end
      
      name = name_regex.match(ifconfig)[1]
      if name == nil
        raise "Unable to parse IP configuration because a valid name does not exist"
      end
      
      m1 = inet4_regex1.match(ifconfig)
      if m1
        netmask = [
          m1[2][0,2].to_i(16),
          m1[2][2,2].to_i(16),
          m1[2][4,2].to_i(16),
          m1[2][6,2].to_i(16),
          ]
        add_ipv4(name, m1[1], netmask.join('.'))
      end
      
      m1 = inet6_regex1.match(ifconfig)
      if m1
        add_ipv6(name, m1[1], m1[2])
      end
    }
    
    return @interfaces
  end
end