#!/bin/bash
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
# Title: DATA PUMP EXPORT SHELL 
#
# Description: 
#   A wrapper to execute data pump export on Oracle and rsync output to an 
#   output location. The wrapper takes care of 
#
#   To configure script behavior, put default values in expdp-shell.env
#   and they will be sourced on startup.  This is better than altering
#   the script directly.
# 
# Usage: 
#   expdp-shell 
#
# Environmental variables accepted by this script: 
#   DP_SHELL_ENV - File with parameter settings for expdp.  Defaults to 
#     expdp-shell.env in current directory. 

# Source the environment settings.  Unfortunately if this is missing we 
# cannot notify anyone. 
echo "### Loading environmental variables"
if [ -z "$DP_SHELL_ENV" ]; then
  DP_SHELL_ENV=dp-shell.env
fi
if [ ! -f $DP_SHELL_ENV ]; then
  echo "Environment settings not found: $DP_SHELL_ENV"
  echo "Ensure this file exists or set DP_SHELL_ENV variable"
  exit 1
fi
. $DP_SHELL_ENV

# Generate the output name. 
backup_name=${DBMS_SID}_`date +%Y-%m-%d_%H-%M_%S`
backup_dir=$BACKUP_BASE_DIR/$backup_name

# Create clean backup directory. 
echo "### Initializing backup directory: $backup_dir"
if [ -f $backup_dir ]; then
  rm -r $backup_dir
fi
mkdir -p $backup_dir

# Register backup directory with Oracle. 
echo "### Registering backup directory with Oracle"
sqlplus / as sysdba <<END
create or replace directory DATA_PUMP_DIR as '$backup_dir';
grant read,write on directory DATA_PUMP_DIR to $DBMS_LOGIN;
END

# Execute data pump export. 
echo "### Executing data pump command"
set -x
expdp ${DBMS_LOGIN}/${DBMS_PASSWORD}@${DBMS_SID} \
  full=${EXPDP_FULL} \
  exclude=${EXPDP_EXCLUDE} \
  consistent=${EXPDP_CONSISTENT} \
  compression=${EXPDP_COMPRESSION} \
  parallel=12 \
  directory=DATA_PUMP_DIR \
  dumpfile=expdp_%U.dmp

  #logfile=expdp.log \

# Compute checksums and store. 
echo "### Computing checksums on output"
(cd $backup_dir; cksum $backup_dir/* >> expdp.cksum)

# If there is an rsync destination, rsync the backup there. 
if [ -z RSYNC_DESTIATION ]; then
  echo "### Skipping rsync of backup"
else
  echo "### Rsync'ing backup: $RSYNC_DESTINATION"
  rsync -avr $backup_dir $RSYNC_DESTINATION
fi

# All done.  
echo "### Done!"
