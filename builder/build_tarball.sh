#!/bin/bash

##########################################################################
# NAME:  BUILD_TARBALL
#
# SUMMARY:  Creates the release tarball of Tungsten Replicator from 
#           compiled sources
#
# OVERVIEW: Copies required files in the 'release directory', creates the
#           cluster-home plus manifest files and generates the tarball
#
# USAGE:
#    First check out compile all required sources. The include this file 
#    (source build_tarball.sh) and call 'build_tarball' function. Note 
#    that all variables must be set appropriately prior to calling the 
#    function (see build.sh and config files for more details)
#
##########################################################################



build_tarball() {
  ########################################################################
  # Copy files into the community build.
  ########################################################################
  printHeader "Creating Replicator release"
  reldir=build/${relname}
  if [ -d $reldir ]; then
    echo "### Removing old release directory"
    \rm -rf $reldir
  fi
  
  echo "### Creating release: $reldir"
  mkdir -p $reldir
  
  # Copy everything!
  doCopy tungsten-replicator $source_replicator/build/tungsten-replicator $reldir
  cp LICENSE $reldir
  cp extra/README $reldir
  cp extra/open_source_licenses.txt $reldir
  
  ########################################################################
  # Fix up replicator files.
  ########################################################################
  
  reldir_replicator=$reldir/tungsten-replicator
  replicator_bin=$reldir_replicator/bin/replicator
  
  ########################################################################
  # Create the cluster home.
  ########################################################################
  
  echo "### Creating cluster home"
  cluster_home=$reldir/cluster-home
  mkdir -p $cluster_home/conf/cluster
	# log directory for cluster-home/bin programs
  mkdir -p $cluster_home/log
  
  echo "# Copying cluser-home/conf files"
  cp -r $extra_cluster_home/conf/* $cluster_home/conf
  
  echo "# Copying cluser-home bin scripts"
  cp -r $extra_cluster_home/bin $cluster_home
  
  echo "# Copying in Ruby configuration libraries"
  cp -r $extra_cluster_home/lib $cluster_home
  cp -r $extra_cluster_home/samples $cluster_home
  
  echo "# Copying in oss-commons libraries"
  cp -r $jars_commons/* $cluster_home/lib
  cp -r $lib_commons/* $cluster_home/lib

	# Copy in Tanuki wrapper libraries if they exist
	if [ -d $extra_commercial ]; then
		if [ -d "${extra_commercial}/cluster-home/bin" ]; then
			cp $extra_commercial/cluster-home/bin/wrapper* $cluster_home/bin
		fi
		
		if [ -d "${extra_commercial}/cluster-home/lib" ]; then
			cp $extra_commercial/cluster-home/lib/wrapper* $cluster_home/lib
			cp $extra_commercial/cluster-home/lib/libwrapper* $cluster_home/lib
		fi
		
		if [ -d "${extra_commercial}/replicator/conf" ]; then
			cp $extra_commercial/replicator/conf/* $reldir_replicator/conf
		fi
		
		# Toggle the -e flag in case there aren't wrapper entries 
		set +e
		wrapper_binaries=`ls $cluster_home/bin/wrapper* | wc -l`
		set -e
		if [ "$wrapper_binaries" != "0" ]; then
			echo "### Replicator: use Tanuki Wrapper"
			cp $source_replicator/samples/scripts/tanuki/replicator $reldir_replicator/bin
			cp $source_replicator/samples/scripts/tanuki/log4j.properties $reldir_replicator/conf
		fi
	fi
  
  echo "### Creating tools"
  tools_dir=$reldir/tools
  mkdir -p $tools_dir
  cp $extra_tools/tpm $tools_dir
  rsync -Ca $extra_tools/ruby-tpm $tools_dir

	########################################################################
  # Evaluate any extensions in $SRC_DIR
  ########################################################################
	set +e
	extensions=`ls -d $SRC_DIR/extensions/*`
	set -e
	for ext in $extensions; do
		if [ -d $ext ]; then
			if [ -f "${ext}/build.xml" ]; then
				echo "### Build ${ext}"
				ant -buildfile $ext/build.xml -DTARGET=$PWD/$reldir -DPRODUCT=TR -DVERSION=$VERSION -DBUILD_NUMBER=$BUILD_NUMBER
			fi
		fi
	done
  
  ########################################################################
  # Create manifest file.
  ########################################################################
  
  manifest=${reldir}/.manifest
  echo "### Creating manifest file: $manifest"
  
  echo "# Build manifest file" >> $manifest
  echo "DATE: `date`" >> $manifest
  echo "RELEASE: ${relname}" >> $manifest
  echo "USER ACCOUNT: ${USER}" >> $manifest
  
  # Hudson environment values.  These will be empty in local builds.
  echo "BUILD_NUMBER: ${BUILD_NUMBER}" >> $manifest
  echo "BUILD_ID: ${BUILD_NUMBER}" >> $manifest
  echo "JOB_NAME: ${JOB_NAME}" >> $manifest
  echo "BUILD_TAG: ${BUILD_TAG}" >> $manifest
  echo "HUDSON_URL: ${HUDSON_URL}" >> $manifest
  
  # Local values.
  echo "HOST: `hostname`" >> $manifest
  echo -n "GIT_URL: " >> $manifest
  git config --get remote.origin.url  >> $manifest
  echo -n "GIT_BRANCH: " >> $manifest
  git rev-parse --abbrev-ref HEAD >> $manifest
  echo -n "GIT_REVISION: " >> $manifest
  git rev-parse HEAD >> $manifest

  ########################################################################
  # Create JSON manifest file.
  ########################################################################

  manifestJSON=${reldir}/.manifest.json
  echo "### Creating JSON manifest file: $manifestJSON"
  
  # Local details.
  echo    "{" >> $manifestJSON
  echo    "  \"date\": \"`date`\"," >> $manifestJSON
  echo    "  \"product\": \"${product}\"," >> $manifestJSON
  echo    "  \"version\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"major\": ${VERSION_MAJOR}," >> $manifestJSON
  echo    "    \"minor\": ${VERSION_MINOR}," >> $manifestJSON
  echo    "    \"revision\": ${VERSION_REVISION}" >> $manifestJSON
  echo    "  }," >> $manifestJSON
  echo    "  \"userAccount\": \"${USER}\"," >> $manifestJSON
  echo    "  \"host\": \"`hostname`\"," >> $manifestJSON
  
  # Hudson environment values.  These will be empty in local builds.
  echo    "  \"hudson\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"buildNumber\": ${BUILD_NUMBER-null}," >> $manifestJSON
  echo    "    \"buildId\": ${BUILD_NUMBER-null}," >> $manifestJSON
  echo    "    \"jobName\": \"${JOB_NAME}\"," >> $manifestJSON
  echo    "    \"buildTag\": \"${BUILD_TAG}\"," >> $manifestJSON
  echo    "    \"URL\": \"${HUDSON_URL}\"" >> $manifestJSON
  echo    "  }," >> $manifestJSON

  # Git repo details.
  echo    "  \"git\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"URL\": \"`git config --get remote.origin.url`\"," >> $manifestJSON
  echo    "    \"branch\": \"`git rev-parse --abbrev-ref HEAD`\"," >> $manifestJSON
  echo    "    \"revision\": \"`git rev-parse HEAD`\"" >> $manifestJSON
  echo    "  }" >> $manifestJSON
  echo    "}" >> $manifestJSON
  
  
  ########################################################################
  # Create the bash auto-completion file 
  ########################################################################
  $reldir/tools/tpm write-completion
  
  cat $manifest
  
  echo "### Cleaning up left over files"
  # find and delete directories named .svn and any file named *<sed extension>
  find ${reldir} \( -name '.svn' -a -type d -o -name "*$SEDBAK_EXT" \) -exec \rm -rf {} \; > /dev/null 2>&1
  
  ########################################################################
  # Generate tar file.
  ########################################################################
  rel_tgz=${relname}.tar.gz
  echo "### Creating tar file: ${rel_tgz}"
  (cd ${reldir}/..; tar -czf ${rel_tgz} ${relname})
}
