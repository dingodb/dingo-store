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

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
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
    mkdir "$dstpath/data/${role}"
  fi
  if [ ! -d "$dstpath/data/${role}/raft" ]; then
    mkdir "$dstpath/data/${role}/raft"
  fi
  if [ ! -d "$dstpath/data/${role}/db" ]; then
    mkdir "$dstpath/data/${role}/db"
  fi

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
    rm -rf $dstpath/data/store/db/*
  fi
  if [ "${FLAGS_clean_raft}" == "0" ]; then
    rm -rf $dstpath/data/store/raft/*
  fi
  if [ "${FLAGS_clean_log}" == "0" ]; then
    rm -rf $dstpath/log/*
  fi
}

deploy() {
  # COORDINATOR_SERVER_START_PORT=22001
  # COORDINATOR_RAFT_START_PORT=22101
  # RAFT_START_PORT=20101
  # SERVER_START_PORT=20001
  program_dir=$BASE_DIR/dist/${FLAGS_role}1
  if [ $FLAGS_role == "coordinator" ]; then
    deploy_store ${FLAGS_role} $BASE_DIR $program_dir $COORDINATOR_SERVER_START_PORT $COORDINATOR_RAFT_START_PORT $INSTANCE_START_ID ${COOR_SRV_PEERS:-fail} ${COOR_RAFT_PEERS}
  else
    deploy_store ${FLAGS_role} $BASE_DIR $program_dir $SERVER_START_PORT $RAFT_START_PORT $INSTANCE_START_ID ${COOR_SRV_PEERS} ${COOR_RAFT_PEERS:-fail}
  fi


}


function start_program() {
  role=$1
  root_dir=$2
  echo "start server: ${root_dir}"

  cd ${root_dir}

  # nohup ./bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml 2>&1 >./log/out &
  ./bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml
}



start() {
  #FLAGS_role=${FLAGS_role}
  i=1
  program_dir=$BASE_DIR/dist/${FLAGS_role}${i}
  # clean log
  rm -f ${program_dir}/log/*
  start_program ${FLAGS_role} ${program_dir}    	
}




clean() 
{
  rm -rf ../dist/*
  sleep 1
  echo "rm -rf all cluster files"
}




stop() 
{
   k_pid=$(ps -ef | grep dingodb_server |grep -v grep|awk '{printf $2 "\n"}')
   for i in $k_pid
   do
   echo $i 
   $(kill -9 $i)
   done
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
