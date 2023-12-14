#!/bin/bash
#
# CAUTION: we setup ulimit parameters in deploy_func.sh, please check that file for
#          how the ulimit options is required.
# The ulimit is setup in start_program, the paramter of ulimit is:
#   ulimit -n 1048576
#   ulimit -u 4194304
#   ulimit -c unlimited
# If set ulimit failed, please use root or sudo to execute sysctl.sh to increase kernal limit.

DEPLOY_PARAMETER=deploy_parameters
DEPLOY_SERVER_NUM=3

# This is for developer, and only can be openned in develop envirement.
export TCMALLOC_SAMPLE_PARAMETER=524288
echo "export TCMALLOC_SAMPLE_PARAMETER=524288, to enable heap profiler"

if [ -n "$1" ]
then
    DEPLOY_PARAMETER=$1
fi

if [ -n "$2" ]
then
    DEPLOY_SERVER_NUM=$2
fi

echo "DEPLOY_PARAMETER="${DEPLOY_PARAMETER}
echo "DEPLOY_SERVER_NUM="${DEPLOY_SERVER_NUM}

./stop.sh --role coordinator --force=1
./stop.sh --role store --force=1
./stop.sh --role index --force=1
echo "force stop all"
sleep 1

./deploy_server.sh --role coordinator --clean_all --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
./deploy_server.sh --role store --clean_all --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
./deploy_server.sh --role index --clean_all --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
sleep 1
echo "deploy all"

./start_server.sh --role coordinator --server_num=${DEPLOY_SERVER_NUM}
./start_server.sh --role store --server_num=${DEPLOY_SERVER_NUM}
./start_server.sh --role index --server_num=${DEPLOY_SERVER_NUM}
echo "start all"

./check_store.sh

