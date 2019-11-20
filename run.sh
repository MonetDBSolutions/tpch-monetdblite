#!/usr/bin/env bash

set -e -x

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2019 MonetDB Solutions B.V.

# This is a stripped down version of the earlier generate_dataset.sh.
#

# LOCATIONS
tool_version="1.0"
home="$(realpath "$(dirname "$0")")"
java_code="$home/tpchbenchmark"
jar_file="$java_code/target/tpchbenchmark-$tool_version.jar"

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
This runs the benchmark queries, assuming the data
has already properly been loaded.
EOF
	exit 1
}

scale_factor="$1"
connect_string="$2"

test -n "$scale_factor" || usage
[[ $1 =~ ^[0-9]+\.?[0-9]*$ ]] || usage "Scale factor must be a positive number"

test -n "$connect_string" || usage

# REBUILD TOOL IF NECESSARY
make -C "$home" jar

# INVOKE THE TOOL
set -x
exec java -jar "$jar_file" evaluate "$connect_string" "$scale_factor"
