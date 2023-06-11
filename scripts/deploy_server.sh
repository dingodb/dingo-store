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
DEFINE_string parameters 'deploy_parameters' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "role: ${FLAGS_role}"
echo "parameters: ${FLAGS_parameters}"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

TMP_COORDINATOR_SERVICES=$BASE_DIR/build/bin/coor_list

source $mydir/deploy_func.sh
source $mydir/${FLAGS_parameters}

USER=`whoami`
echo "user: ${USER}"

if [ "$USER" == "dylan" ]; then
  SERVER_START_PORT=10000
  RAFT_START_PORT=10100
  COORDINATOR_SERVER_START_PORT=12000
  COORDINATOR_RAFT_START_PORT=12100
fi

COOR_RAFT_PEERS=""

echo "# dingo-store coordinators">${TMP_COORDINATOR_SERVICES}

if [ -e ${SERVER_NUM} ]; then
  echo "server number is not set"
  exit 0
fi

for ((i=1; i<=$SERVER_NUM; ++i)); do
    COOR_RAFT_PEERS=${COOR_RAFT_PEERS}","$SERVER_HOST":"`expr $COORDINATOR_RAFT_START_PORT + $i`

    echo $SERVER_HOST":"`expr $COORDINATOR_SERVER_START_PORT + $i` >> ${TMP_COORDINATOR_SERVICES}
done

COOR_RAFT_PEERS=${COOR_RAFT_PEERS:1}

echo "raft peers: ${COOR_RAFT_PEERS}"

for ((i=1; i<=$SERVER_NUM; ++i)); do
  program_dir=$BASE_DIR/dist/${FLAGS_role}${i}

if [ ${FLAGS_role} == "coordinator" ]; then
  deploy_store ${FLAGS_role} ${BASE_DIR} ${program_dir} `expr ${COORDINATOR_SERVER_START_PORT} + ${i}` `expr ${COORDINATOR_RAFT_START_PORT} + ${i}` `expr ${COORDINATOR_INSTANCE_START_ID} + ${i}` ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
elif [ ${FLAGS_role} == "index" ]; then
  deploy_store ${FLAGS_role} ${BASE_DIR} ${program_dir} `expr ${INDEX_SERVER_START_PORT} + ${i}` `expr ${INDEX_RAFT_START_PORT} + ${i}` `expr ${INDEX_INSTANCE_START_ID} + ${i}` ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
else
  deploy_store ${FLAGS_role} ${BASE_DIR} ${program_dir} `expr ${SERVER_START_PORT} + ${i}` `expr ${RAFT_START_PORT} + ${i}` `expr ${STORE_INSTANCE_START_ID} + ${i}` ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
fi

done

echo "Finish..."

