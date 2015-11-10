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
# Title: DATA PUMP IMPORT SHELL
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
#   impdp-shell
#
# Environmental variables accepted by this script:
#   DP_SHELL_ENV - File with parameter settings for impdp.  Defaults to
#     expdp-shell.env in current directory.

reportError() {
  echo $1
  if [ $FAILURE_EMAIL_SEND == 'Y' ]
  then
    mail -s "${FAILURE_EMAIL_SUBJECT}"  ${FAILURE_EMAIL} <<< $1
  fi
  exit 1
}
# Source the environment settings.  Unfortunately if this is missing we
# cannot notify anyone.
echo "### Loading environmental variables"
if [ -z "$DP_SHELL_ENV" ]; then
  DP_SHELL_ENV=dp-shell.env
fi
if [ ! -f $DP_SHELL_ENV ]; then
  echo "ERROR : Environment settings not found: $DP_SHELL_ENV "
  echo "ERROR : Ensure this file exists or set DP_SHELL_ENV variable"
  exit 1
fi
. $DP_SHELL_ENV


# Create clean backup directory.
echo "### Initializing backup directory: $backup_dir"
if [ -f $BACKUP_BASE_DIR ]; then
  rm -r $BACKUP_BASE_DIR
fi
mkdir -p $BACKUP_BASE_DIR

# Register backup directory with Oracle.
if [ $EXPDP_CREATE_DIR == 'Y' ]
then
  echo "### Registering backup directory with Oracle"
  sqlplus ${EXPDP_SYS_USER}/${EXPDP_SYS_PASSWORD}@${DBMS_SID} as sysdba <<END
whenever sqlerror exit 2;
create or replace directory ${IMPDP_DIR} as '$BACKUP_BASE_DIR';
grant read,write on directory ${IMPDP_DIR} to $DBMS_LOGIN;
END
  if [ $? -ne 0 ]; then
    reportError "ERROR : SQLPLUS command to create ${EXPDP_DIR} failed"
  fi
fi

cd ${IMPDP_SOURCE}
for i in $(ls -d */);
do
   source_base=`echo ${i%%/}`
   if [ ! -f ${source_base}/data_loaded.txt ] && [ -f ${source_base}/rsync_complete.txt ]
   then
      #Directory has not already been loaded and the rsync is complete
      cp ${source_base}/* ${BACKUP_BASE_DIR}
      touch ${source_base}/data_loaded.txt

      #Clean up schema first
      for schema in $(echo ${IMPDP_SCHEMAS}| tr "," " "); do
        sqlplus ${EXPDP_SYS_USER}/${EXPDP_SYS_PASSWORD}@${DBMS_SID} as sysdba <<END
        whenever sqlerror exit 2;
        drop user ${schema} cascade;
END
      done

      # Execute data pump import.
      echo "### Executing data pump command"
      set -x
      impdp ${DBMS_LOGIN}/${DBMS_PASSWORD}@${DBMS_SID} \
        full=${IMPDP_FULL} \
        schemas=${IMPDP_SCHEMAS} \
        directory=${IMPDP_DIR} \
        dumpfile=${source_base}_%U \
        table_exists_action=replace \
        logfile=impdp.log

      if [ $? -ne 0 ]; then
          reportError "ERROR : Datapump import failed"
      fi
    fi
done

# All done.
echo "### Done!"
