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

function start_program() {
  role=$1
  root_dir=$2
  echo "start server: ${root_dir}"

  cd ${root_dir}

  sudo ulimit -c unlimited
  sudo ulimit -n 1024000

  echo "${root_dir}/bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml --coor_url=file://./conf/coor_list"

  nohup ${root_dir}/bin/dingodb_server --role ${role}  --conf ./conf/${role}.yaml --coor_url=file://./conf/coor_list 2>&1 >./log/out &
}
