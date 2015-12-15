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

# Creates method to load system libraries with a nice error message 
# when there is a failure. 

# Override Kernel.require. 
def system_require(lib)
  require_cmd = "require '#{lib}'"
  begin
    eval require_cmd
  rescue LoadError
    $stderr.printf "=============================================================\n"
    $stderr.printf "ERROR:  Unable to load required Ruby module: #{lib}\n"
    $stderr.print "Please ensure Ruby system modules are properly installed\n"
    $stderr.printf "=============================================================\n"
    $stderr.printf "\nFull error and stack trace follows...\n"
    raise
  end
end
