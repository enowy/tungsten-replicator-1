#!/usr/bin/env ruby 
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

# == Synopsis 
#   Open replicator script for PostgreSQL WAL shipping solution. 
#
# == Examples
#   TBA
#
# == Usage 
#   pg-wal-wrapper.rb [options]
#
# == Options
#   -o, --operation     Operation to perform
#   -c, --config        Configuration file
#   -a, --args          Arguments as a set of name-value pairs
#   -h, --help          Displays help message
#   -V, --verbose       Verbose output
#
# == Author
#   Robert Hodges
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'pg-wal-plugin.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Operation interrupted")
  exit 1
}

# Create and run the WAL manager plugin. 
cmd = ARGV.shift
plugin = PgWalPlugin.new(cmd, ARGV)
plugin.run
