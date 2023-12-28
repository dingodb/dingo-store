#/bin/bash


mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string xdprocks_dir '' 'xdprocks path'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

if [ "${FLAGS_xdprocks_dir}" == "" ]; then
  echo "xdprocks directory is empty."
  exit 1
fi

echo "prepare xdprocks, dir: ${FLAGS_xdprocks_dir}."

XDPROCKS_INCLUDE_DIR=${FLAGS_xdprocks_dir}/include

if [ ! -d ${XDPROCKS_INCLUDE_DIR}/xdprocks ]; then
  set -ex

  cp -r ${XDPROCKS_INCLUDE_DIR}/rocksdb ${XDPROCKS_INCLUDE_DIR}/xdprocks
  find ${XDPROCKS_INCLUDE_DIR}/xdprocks -name "*.h" | xargs sed -i -e 's/ROCKSDB_NAMESPACE/XDPROCKS_NAMESPACE/' -e 's/rocksdb\//xdprocks\//'
fi

echo "prepare xdprocks header file finish."