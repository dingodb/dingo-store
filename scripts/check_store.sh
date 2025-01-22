#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_integer retry_times 64 'retry times'

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

DINGODB_BIN=$BASE_DIR/build/bin/

echo ${DINGODB_BIN}

cd ${DINGODB_BIN}

times=0
DINGODB_HAVE_STORE_AVAILABLE=0
while [ "${DINGODB_HAVE_STORE_AVAILABLE}" -eq 0 -a ${times} -lt ${FLAGS_retry_times} ]; do
    DINGODB_HAVE_STORE_AVAILABLE=$(./dingodb_cli GetStoreMap |grep -c DINGODB_HAVE_STORE_AVAILABLE)
    times=`expr $times + 1`

    echo "avaiable store count = ${DINGODB_HAVE_STORE_AVAILABLE}, times = ${times}, wait 2 second"
    sleep 2
done

times=0
DINGODB_HAVE_INDEX_AVAILABLE=0
while [ "${DINGODB_HAVE_INDEX_AVAILABLE}" -eq 0 -a ${times} -lt ${FLAGS_retry_times} ]; do
    DINGODB_HAVE_INDEX_AVAILABLE=$(./dingodb_cli GetStoreMap |grep -c DINGODB_HAVE_INDEX_AVAILABLE)
    times=`expr $times + 1`

    echo "avaiable index count = ${DINGODB_HAVE_INDEX_AVAILABLE}, times = ${times}, wait 2 second"
    sleep 2
done

times=0
DINGODB_HAVE_DOCUMENT_AVAILABLE=0
while [ "${DINGODB_HAVE_DOCUMENT_AVAILABLE}" -eq 0 -a ${times} -lt ${FLAGS_retry_times} ]; do
    DINGODB_HAVE_DOCUMENT_AVAILABLE=$(./dingodb_cli GetStoreMap |grep -c DINGODB_HAVE_DOCUMENT_AVAILABLE)
    times=`expr $times + 1`

    echo "avaiable document count = ${DINGODB_HAVE_DOCUMENT_AVAILABLE}, times = ${times}, wait 2 second"
    sleep 2
done

./dingodb_cli GetStoreMap

echo "dingo-store is READY"

