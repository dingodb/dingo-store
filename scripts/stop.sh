#!/bin/bash

# killall -9 dingodb_server

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'store' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "role: ${FLAGS_role}"
BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
process_no=$(pgrep -f "${BASE_DIR}.*dingodb_server.*${FLAGS_role}" -U `id -u` | xargs)

if [ "${process_no}" != "" ]; then
  echo "pid to kill: ${process_no}"
  kill ${process_no}
else
  echo "not exist ${FLAGS_role} process"
fi
