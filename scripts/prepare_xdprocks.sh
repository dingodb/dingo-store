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

  if [ -d ${XDPROCKS_INCLUDE_DIR}/rocksdb ]; then
    cp -r ${XDPROCKS_INCLUDE_DIR}/rocksdb ${XDPROCKS_INCLUDE_DIR}/xdprocks
    rm -rf ${XDPROCKS_INCLUDE_DIR}/rocksdb_backup
    mv -f ${XDPROCKS_INCLUDE_DIR}/rocksdb ${XDPROCKS_INCLUDE_DIR}/rocksdb_backup
  elif [ -d ${XDPROCKS_INCLUDE_DIR}/rocksdb_backup ]; then
    cp -r ${XDPROCKS_INCLUDE_DIR}/rocksdb_backup ${XDPROCKS_INCLUDE_DIR}/xdprocks
  else
    echo "not exist rocksdb include directory."
    exit 1
  fi

  find ${XDPROCKS_INCLUDE_DIR}/xdprocks -name "*.h" | xargs sed -i -e 's/ROCKSDB_NAMESPACE/XDPROCKS_NAMESPACE/g'  -e 's/ROCKSDB_MAJOR/XDPROCKS_MAJOR/g'  -e 's/ROCKSDB_MINOR/XDPROCKS_MINOR/g'  -e 's/ROCKSDB_PATCH/XDPROCKS_PATCH/g' -e 's/rocksdb\//xdprocks\//g'
fi

echo "prepare xdprocks header file finish."