#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2017-2018 MonetDB Solutions B.V.

function is_positive_number {
	[[ $1 =~ ^[0-9]+\.?[0-9]*$ ]]
}

function abort_script {
	echo $1 >&2 # error message to stderr
	exit 1
}

function is_in_list {
	local needle="$1"
	shift
	for haystack_item in $@; do
		[[ "$needle" == "$haystack_item" ]] && found=1
	done
	[[ -n "$found" ]]
}

function get_platform {
	if [ "$(uname)" == "Darwin" ]; then echo "MAC"
	elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then echo "LINUX"
	elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then echo "WIN32"
	elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW64_NT" ]; then echo "WIN32"
	fi
}

function mhelp {
	echo "Usage: $0 --sf <scale factor> --database <MonetDBLite-Java | H2> --path <path> "
	echo "scale factor 1 is 1GB database"
	echo "farm path should be an absolute path"
}

scale_factor=
database=
output_path=

while [ "$#" -gt 0 ]; do
	case "$1" in
		-s|--sf)
			scale_factor="$2"
			is_positive_number "$scale_factor" || abort_script "Invalid scale factor size $scale_factor"
			shift
			shift
			;;
		-d|--database)
			database="$2"
			shift
			shift
			;;
		-p|--path)
			output_path="$2"
			shift
			shift
			;;
		-h|--help)
			mhelp
			exit 0
			;;
		*)
			mhelp
			abort_script "$0: Unknown parameter $1"
			;;
	esac
done

platform="$(get_platform)"
if [ "$platform" != "LINUX" ]; then
	abort_script 'Only Linux platform supported'
fi

[[ -n "$scale_factor" ]] || abort_script 'Scale factor not set'

[[ -n "$database" ]] || abort_script 'Database not set'

[[ -n "$output_path" ]] || abort_script 'Output path not set'

if [[ ! -d "$output_path" ]] ; then
	mkdir -p "$output_path" || abort_script "Can't create output directory $output_path for the database"
fi

# Compile TPC-H dbgen

tpch_gen_dir=$(dirname `realpath $0`)/tpch-tools/dbgen

[[ -d "$tpch_gen_dir" ]] || abort_script "$tpch_gen_dir directory not found (Did you forget to clone the git submodule?)"

if [[ ! -f "$tpch_gen_dir/dbgen" ]]; then
	pushd "$tpch_gen_dir"

	type cmake 1>/dev/null || abort_script "CMake build tool unavailable"
	cmake -DMACHINE=LINUX -DDATABASE=VECTORWISE . || abort_script "Failure building the generation utility in $tpch_gen_dir during CMake"

	[[ -f "$tpch_gen_dir/Makefile" ]] || abort_script "Could not find or generate a Makefile in $tpch_gen_dir for building the generator utility"
	type make 1>/dev/null || abort_script "GNU Make unavailable"
	make -C "$tpch_gen_dir" || abort_script "Failure building the generation utility in $tpch_gen_dir - Make failure"
	[[ -f "$tpch_gen_dir/dbgen" ]] || abort_script "Although the build of $tpch_gen_dir/dbgen has supposedly succeeded - the binary is missing."

	popd > /dev/null
fi

# Generate TPC-H database CSV files

tpch_data_dir=$(dirname `realpath $0`)/tpch-data/"$database"/sf"$scale_factor"

if [[ ! -d "$tpch_data_dir" ]] ; then
	mkdir -p "$tpch_data_dir" || abort_script "Can't create staging directory $tpch_data_dir for generating TPC-H data"

	pushd "$tpch_data_dir" >/dev/null
	"$tpch_gen_dir/dbgen" -b "$tpch_gen_dir/dists.dss" -s "$scale_factor" -v || abort_script "Failed generating TPC-H data"

	popd > /dev/null
fi

# Begin importing database

tpch_benchmark_dir=$(dirname `realpath $0`)/tpchbenchmark

pushd "$tpch_benchmark_dir" >/dev/null

type mvn 1>/dev/null || abort_script "maven unavailable"

mvn package || abort_script "Error while compiling tpchbenchmark maven project"

mvn_args="populate $database $scale_factor $output_path $tpch_data_dir"
echo "Mvn args: $mvn_args"
mvn exec:java -Dexec.mainClass="nl.cwi.monetdb.TPCH.TPCHMain" -Dexec.args="$mvn_args" || abort_script "Error while importing to $output_path directory"

popd > /dev/null

echo "TPC-H scale factor $scale_factor was successfully imported to $database database on $output_path directory"
