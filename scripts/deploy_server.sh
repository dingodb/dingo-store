#!/bin/bash


mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags


DEFINE_string role 'store' 'server role'
DEFINE_integer server_num 3 'server number'
DEFINE_boolean clean_db 1 'clean db'
DEFINE_boolean clean_raft 1 'clean raft'
DEFINE_boolean clean_log 0 'clean log'
DEFINE_boolean replace_conf 0 'replace conf'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "role: ${FLAGS_role}"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

TMP_COORDINATOR_SERVICES=/tmp/coor_list_`whoami`_`date +%s`
SERVER_NUM=${FLAGS_server_num}
COORDINATOR_INSTANCE_START_ID=22000
STORE_INSTANCE_START_ID=33000
SERVER_HOST=127.0.0.1
SERVER_START_PORT=20000
RAFT_HOST=127.0.0.1
RAFT_START_PORT=20100
COORDINATOR_SERVER_START_PORT=22000
COORDINATOR_RAFT_START_PORT=22100

USER=`whoami`
echo "user: ${USER}"

if [ "$USER" == "dylan" ]; then
  SERVER_START_PORT=10000
  RAFT_START_PORT=10100
  COORDINATOR_SERVER_START_PORT=12000
  COORDINATOR_RAFT_START_PORT=12100
fi

function deploy_store() {
  role=$1
  srcpath=$2
  dstpath=$3
  server_port=$4
  raft_port=$5
  instance_id=$6
  coor_srv_peers=$7
  coor_raft_peers=$8
  coor_service_file=$9

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
  if [ ! -d "$dstpath/data/${role}" ]; then
    mkdir $dstpath/data/${role}
  fi
  if [ ! -d "$dstpath/data/${role}/raft" ]; then
    mkdir $dstpath/data/${role}/raft
  fi
  if [ ! -d "$dstpath/data/${role}/db" ]; then
    mkdir $dstpath/data/${role}/db
  fi

  cp ${coor_service_file} $dstpath/conf/coor_list

  cp $srcpath/build/bin/dingodb_server $dstpath/bin/
  if [ "${FLAGS_replace_conf}" == "0" ]; then
    cp $srcpath/conf/${role}.template.yaml $dstpath/conf/${role}.yaml

    sed  -i 's,\$INSTANCE_ID\$,'"$instance_id"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$SERVER_HOST\$,'"$SERVER_HOST"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$SERVER_PORT\$,'"$server_port"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$RAFT_HOST\$,'"$RAFT_HOST"',g'              $dstpath/conf/${role}.yaml
    sed  -i 's,\$RAFT_PORT\$,'"$raft_port"',g'              $dstpath/conf/${role}.yaml
    sed  -i 's,\$BASE_PATH\$,'"$dstpath"',g'                $dstpath/conf/${role}.yaml

    sed  -i 's|\$COORDINATOR_SERVICE_PEERS\$|'"$coor_srv_peers"'|g'    $dstpath/conf/${role}.yaml
    sed  -i 's|\$COORDINATOR_RAFT_PEERS\$|'"$coor_raft_peers"'|g'  $dstpath/conf/${role}.yaml
  fi

  if [ "${FLAGS_clean_db}" == "0" ]; then
    rm -rf $dstpath/data/${role}/db/*
  fi
  if [ "${FLAGS_clean_raft}" == "0" ]; then
    rm -rf $dstpath/data/${role}/raft/*
  fi
  if [ "${FLAGS_clean_log}" == "0" ]; then
    rm -rf $dstpath/log/*
  fi
}

COOR_SRV_PEERS=""
COOR_RAFT_PEERS=""

echo "# dingo-store coordinators">${TMP_COORDINATOR_SERVICES}

for ((i=1; i<=$SERVER_NUM; ++i)); do
    COOR_SRV_PEERS=${COOR_SRV_PEERS}","$SERVER_HOST":"`expr $COORDINATOR_SERVER_START_PORT + $i`
    COOR_RAFT_PEERS=${COOR_RAFT_PEERS}","$SERVER_HOST":"`expr $COORDINATOR_RAFT_START_PORT + $i`

    echo $SERVER_HOST":"`expr $COORDINATOR_SERVER_START_PORT + $i` >> ${TMP_COORDINATOR_SERVICES}
done

COOR_SRV_PEERS=${COOR_SRV_PEERS:1}
COOR_RAFT_PEERS=${COOR_RAFT_PEERS:1}

echo "server peers: ${COOR_SRV_PEERS}"
echo "raft peers: ${COOR_RAFT_PEERS}"

for ((i=1; i<=$SERVER_NUM; ++i)); do
  program_dir=$BASE_DIR/dist/${FLAGS_role}${i}

if [ $FLAGS_role == "coordinator" ]; then
  deploy_store ${FLAGS_role} $BASE_DIR $program_dir `expr $COORDINATOR_SERVER_START_PORT + $i` `expr $COORDINATOR_RAFT_START_PORT + $i` `expr $COORDINATOR_INSTANCE_START_ID + $i` ${COOR_SRV_PEERS} ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
else
  deploy_store ${FLAGS_role} $BASE_DIR $program_dir `expr $SERVER_START_PORT + $i` `expr $RAFT_START_PORT + $i` `expr $STORE_INSTANCE_START_ID + $i` ${COOR_SRV_PEERS} ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
fi

done

echo "unlink..."${TMP_COORDINATOR_SERVICES}

unlink ${TMP_COORDINATOR_SERVICES}

echo "Finish..."

