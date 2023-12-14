#!/bin/bash
# The ulimit is setup in start_program, the paramter of ulimit is:
#   ulimit -n 1048576
#   ulimit -u 4194304
#   ulimit -c unlimited
# If set ulimit failed, please use root or sudo to execute sysctl.sh to increase kernal limit.

function deploy_store() {
  role=$1
  srcpath=$2
  dstpath=$3
  server_port=$4
  raft_port=$5
  instance_id=$6
  coor_raft_peers=$7
  coor_service_file=$8

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

  cp ${coor_service_file} $dstpath/conf/coor_list

  unlink "${dstpath}/bin/dingodb_server"
  ln -s  "${srcpath}/build/bin/dingodb_server" "${dstpath}/bin/dingodb_server"
  if [ "${FLAGS_replace_conf}" == "0" ]; then
    cp $srcpath/conf/${role}.template.yaml $dstpath/conf/${role}.yaml

    sed  -i 's,\$INSTANCE_ID\$,'"$instance_id"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$SERVER_HOST\$,'"$SERVER_HOST"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$SERVER_PORT\$,'"$server_port"',g'          $dstpath/conf/${role}.yaml
    sed  -i 's,\$RAFT_HOST\$,'"$RAFT_HOST"',g'              $dstpath/conf/${role}.yaml
    sed  -i 's,\$RAFT_PORT\$,'"$raft_port"',g'              $dstpath/conf/${role}.yaml
    sed  -i 's,\$BASE_PATH\$,'"$dstpath"',g'                $dstpath/conf/${role}.yaml

    sed  -i 's|\$COORDINATOR_RAFT_PEERS\$|'"$coor_raft_peers"'|g'  $dstpath/conf/${role}.yaml

    if [ -f $srcpath/conf/${role}-gflags.conf ]
    then
        echo "cp $srcpath/conf/${role}-gflags.conf $dstpath/conf/gflags.conf"
        cp $srcpath/conf/${role}-gflags.conf $dstpath/conf/gflags.conf
    fi
  fi

  if [ "${FLAGS_clean_db}" == "0" ]; then
    rm -rf $dstpath/data/db
  fi
  if [ "${FLAGS_clean_raft}" == "0" ]; then
    rm -rf $dstpath/data/raft_data/
    rm -rf $dstpath/data/raft_log/
  fi
  if [ "${FLAGS_clean_log}" == "0" ]; then
    rm -rf $dstpath/log/*
  fi
  if [ "${FLAGS_clean_idx}" == "0" ]; then
    rm -rf $dstpath/data/vector_index_snapshot/
  fi
  if [ "${FLAGS_clean_all}" == "0" ]; then
    rm -rf $dstpath/data/*
    rm -rf $dstpath/log/*
    echo "CLEAN ALL $dstpath/data/* $dstpath/log/*"
  fi
}

function set_ulimit() {
    NUM_FILE=1048576
    NUM_PROC=4194304

    # 1. sysctl is the very-high-level hard limit:
    #     fs.nr_open = 1048576
    #     fs.file-max = 4194304
    # 2. /etc/security/limits.conf is the second-level limit for users, this is not required to setup.
    #    CAUTION: values in limits.conf can't bigger than sysctl kernel values, or user login will fail.
    #     * - nofile 1048576
    #     * - nproc  4194304
    # 3. we can use ulimit to set value before start service.
    #     ulimit -n 1048576
    #     ulimit -u 4194304
    #     ulimit -c unlimited

    # ulimit -n
    nfile=$(ulimit -n)
    echo "nfile="${nfile}
    if [ ${nfile} -lt ${NUM_FILE} ]
    then
        echo "try to increase nfile"
        ulimit -n ${NUM_FILE}

        nfile=$(ulimit -n)
        echo "nfile new="${nfile}
        if [ ${nfile} -lt ${NUM_FILE} ]
        then
            echo "need to increase nfile to ${NUM_FILE}, exit!"
            exit -1
        fi
    fi

    # ulimit -c
    ncore=$(ulimit -c)
    echo "ncore="${ncore}
    if [ ${ncore} != "unlimited" ]
    then
        echo "try to set ulimit -c unlimited"
        ulimit -c unlimited

        ncore=$(ulimit -c)
        echo "ncore new="${ncore}
        if [ ${ncore} != "unlimited" ]
        then
            echo "need to set ulimit -c unlimited, exit!"
            exit -1
        fi
    fi

    # ulimit -u
    nproc=$(ulimit -u)
    echo "nproc="${nproc}
    if [ ${nproc} -lt ${NUM_PROC} ]
    then
        echo "try to increase nproc"
        ulimit -u ${NUM_PROC}

        nproc=$(ulimit -u)
        echo "nproc new="${nproc}
        if [ ${nproc} -lt ${NUM_PROC} ]
        then
            echo "need to increase nproc to ${NUM_PROC}, exit!"
            exit -1
        fi
    fi
}

function start_program() {
  role=$1
  root_dir=$2
  echo "set ulimit"
  set_ulimit
  echo "start server: ${root_dir}"

  cd ${root_dir}

  echo "${root_dir}/bin/dingodb_server -role=${role}"

  nohup ${root_dir}/bin/dingodb_server -role=${role} 2>&1 >./log/out &
}
