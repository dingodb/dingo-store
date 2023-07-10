#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'store' 'server role'
DEFINE_boolean clean_db 1 'clean db'
DEFINE_boolean clean_raft 1 'clean raft'
DEFINE_boolean clean_log 0 'clean log'
DEFINE_boolean replace_conf 0 'replace conf'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist
TMP_COORDINATOR_SERVICES=$BASE_DIR/build/bin/coor_list

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

source $mydir/deploy_func.sh

deploy() {
  # COORDINATOR_SERVER_START_PORT=22001
  # COORDINATOR_RAFT_START_PORT=22101
  # RAFT_START_PORT=20101
  # SERVER_START_PORT=20001
  echo "# dingo-store coordinators" > ${TMP_COORDINATOR_SERVICES}
  echo $COOR_SRV_PEERS | tr ',' '\n' >> ${TMP_COORDINATOR_SERVICES}
  program_dir=$BASE_DIR/dist/${FLAGS_role}1
  if [ $FLAGS_role == "coordinator" ]; then
    deploy_store ${FLAGS_role} $BASE_DIR $program_dir $COORDINATOR_SERVER_START_PORT $COORDINATOR_RAFT_START_PORT $INSTANCE_START_ID ${COOR_RAFT_PEERS} ${TMP_COORDINATOR_SERVICES}
  elif [ ${FLAGS_role} == "index" ]; then
    deploy_store ${FLAGS_role} ${BASE_DIR} ${program_dir} ${INDEX_SERVER_START_PORT} ${INDEX_RAFT_START_PORT} ${INDEX_INSTANCE_START_ID} ${COOR_RAFT_PEERS:-fail} ${TMP_COORDINATOR_SERVICES}
  else
    deploy_store ${FLAGS_role} $BASE_DIR $program_dir $SERVER_START_PORT $RAFT_START_PORT $INSTANCE_START_ID ${COOR_RAFT_PEERS:-fail} ${TMP_COORDINATOR_SERVICES}
  fi
}

function start_program_docker() {
  role=$1
  root_dir=$2
  echo "start server: ${root_dir}"

  cd ${root_dir}

  # nohup ./bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml 2>&1 >./log/out &
  ${root_dir}/bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml --coor_url=file://./conf/coor_list
}

start() {
  #FLAGS_role=${FLAGS_role}
  i=1
  program_dir=$BASE_DIR/dist/${FLAGS_role}${i}
  # clean log
  rm -f ${program_dir}/log/*
  start_program_docker "${FLAGS_role}" "${program_dir}"
}

clean()
{
  rm -rf ../dist/*
  sleep 1
  echo "rm -rf all cluster files"
}

stop()
{
  pgrep dingodb_server | xargs kill
}

usage()
{
  echo "Usage: $0 [deploy|start|stop|restart|cleanstart]"
}

if [ $# -lt 1 ];then
  usage
  exit
fi

if [ "$1" = "deploy" ];then
  deploy
elif [ "$1" = "start" ];then
  start
elif [ "$1" = "stop" ];then
  stop
elif [ "$1" = "restart" ];then
  stop
  start
elif [ "$1" = "cleanstart" ];then
  stop
  clean
  deploy
  start
else
  usage
fi
