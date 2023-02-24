#!/bin/bash


mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
echo "mydir: ${mydir}"
. $mydir/shflags


DEFINE_string role 'store' 'server role'

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist
echo "DIST_DIR: ${DIST_DIR}"

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

SERVER_NUM=3
INSTANCE_START_ID=1000
SERVER_HOST=127.0.0.1
SERVER_START_PORT=20000
RAFT_HOST=127.0.0.1
RAFT_START_PORT=20100


function deploy_store() {
  role=$1
  srcpath=$2
  dstpath=$3
  server_port=$4
  raft_port=$5
  instance_id=$6

  echo "server ${dstpath}"

  if [ ! -d "$dstpath" ]; then
    mkdir "$dstpath"
  fi

  if [ ! -d "$dstpath/bin" ]; then
    mkdir "$dstpath/bin"
  fi
  if [ ! -d "$dstpath/conf" ]; then
    mkdir "$dstpath/conf"
  fi
  if [ ! -d "$dstpath/log" ]; then
    mkdir "$dstpath/log"
  fi
  if [ ! -d "$dstpath/data" ]; then
    mkdir "$dstpath/data"
  fi
  if [ ! -d "$dstpath/data/store" ]; then
    mkdir "$dstpath/data/store"
  fi
  if [ ! -d "$dstpath/data/store/raft" ]; then
    mkdir "$dstpath/data/store/raft"
  fi
  if [ ! -d "$dstpath/data/store/db" ]; then
    mkdir "$dstpath/data/store/db"
  fi

  cp $srcpath/build/bin/dingodb_server $dstpath/bin/
  cp $srcpath/conf/${role}.yaml $dstpath/conf/

  sed  -i 's,\$INSTANCE_ID\$,'"$instance_id"',g'  $dstpath/conf/${role}.yaml
  sed  -i 's,\$SERVER_HOST\$,'"$SERVER_HOST"',g'  $dstpath/conf/${role}.yaml
  sed  -i 's,\$SERVER_PORT\$,'"$server_port"',g'  $dstpath/conf/${role}.yaml
  sed  -i 's,\$RAFT_HOST\$,'"$RAFT_HOST"',g'  $dstpath/conf/${role}.yaml
  sed  -i 's,\$RAFT_PORT\$,'"$raft_port"',g'  $dstpath/conf/${role}.yaml
  sed  -i 's,\$BASE_PATH\$,'"$dstpath"',g'  $dstpath/conf/${role}.yaml
}

for ((i=1; i<=$SERVER_NUM; ++i)); do
  program_dir=$BASE_DIR/dist/${FLAGS_role}${i}

  deploy_store ${FLAGS_role} $BASE_DIR $program_dir `expr $SERVER_START_PORT + $i` `expr $RAFT_START_PORT + $i` `expr $INSTANCE_START_ID + $i`
done

echo "Finish..."

