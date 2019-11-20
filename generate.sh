#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2019 MonetDB Solutions B.V.

# This is a stripped down version of the earlier generate_dataset.sh.
# It needs less machinery because it doesn't go through maven and the
# new java tool takes fewer parameters.

set -e

# LOCATIONS
tool_version="1.0"
home="$(realpath "$(dirname "$0")")"
java_code="$home/tpchbenchmark"
jar_file="$java_code/target/tpchbenchmark-$tool_version.jar"
dbgen_dir="$home/tpch-tools/dbgen"
data_root="$home/tpch-data"

# COMMAND LINE
usage() {
	exec 1>&2
	echo
	if [[ -n "$*" ]]; then
		echo "ERROR: $*"
		echo
	fi
	cat <<EOF
Usage: $0 SCALE_FACTOR [USER[/PASSWORD]@JDBC_URL]
If necessary, this will generate test data somewhere under
	$data_root
then invoke the database-specific data loading code.
EOF
	exit 1
}

scale_factor="$1"
connect_string="$2"

test -n "$scale_factor" || usage
[[ $1 =~ ^[0-9]+\.?[0-9]*$ ]] || usage "Scale factor must be a positive number"

test -n "$connect_string" || usage

test -d "$dbgen_dir" || usage "$dbgen_dir not found"

data_dir="$data_root"/"$scale_factor"

# BUILD DBGEN IF NECESSARY
if [ ! -d "$data_dir" -a ! -x "$dbgen_dir/dbgen" ]; then
	(
		cd "$dbgen_dir" || exit 2
		type cmake 1>/dev/null || usage "CMake not available"
		type make 1>/dev/null || usage "Make not available"
		cmake -DMACHINE=LINUX -DDATABASE=VECTORWISE . || usage "CMake failed"
		test -f Makefile || usage "CMake didn't generate a Makefile"
		make || "Make dbgen failed"
		test -f dbgen -a -x dbgen || usage "No dbgen was created"
	)
fi

# GENERATE THE DATA IF NECESSARY
data_dir_tmp="$data_dir.tmp"
if ! [ -d "$data_dir" ]; then
	rm -rf "$data_dir_tmp"
	mkdir -p "$data_dir_tmp"
	(
		cd "$data_dir_tmp" || exit 3
		"$dbgen_dir/dbgen" -b "$dbgen_dir"/dists.dss -s "$scale_factor" -v || usage "dbgen failed"
	)
	mv -v "$data_dir_tmp" "$data_dir"
fi

# REBUILD TOOL IF NECESSARY
make -C "$home" jar

# INVOKE THE TOOL
exec java -jar "$jar_file" populate "$connect_string" "$data_dir"
