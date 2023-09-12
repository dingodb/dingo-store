#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

DINGODB_BIN=$BASE_DIR/build/bin/

echo ${DINGODB_BIN}

cd ${DINGODB_BIN}

DINGODB_HAVE_STORE_AVAILABLE=$(./dingodb_client GetStoreMap |grep -c DINGODB_HAVE_STORE_AVAILABLE)
while [ "${DINGODB_HAVE_STORE_AVAILABLE}" -eq 0 ]; do
    echo "DINGODB_HAVE_STORE_AVAILABLE == 0, wait 2 second"
    sleep 2
    DINGODB_HAVE_STORE_AVAILABLE=$(./dingodb_client GetStoreMap |grep -c DINGODB_HAVE_STORE_AVAILABLE)
done

DINGODB_HAVE_INDEX_AVAILABLE=$(./dingodb_client GetStoreMap |grep -c DINGODB_HAVE_INDEX_AVAILABLE)
while [ "${DINGODB_HAVE_INDEX_AVAILABLE}" -eq 0 ]; do
    echo "DINGODB_HAVE_INDEX_AVAILABLE == 0, wait 2 second"
    sleep 2
    DINGODB_HAVE_INDEX_AVAILABLE=$(./dingodb_client GetStoreMap |grep -c DINGODB_HAVE_INDEX_AVAILABLE)
done

./dingodb_client GetStoreMap

echo "dingo-store is READY"

