DEPLOY_PARAMETER=deploy_parameters
DEPLOY_SERVER_NUM=3

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

./stop.sh --role coordinator
./stop.sh --role store
./stop.sh --role index
echo "stop all"
sleep 1

./deploy_server.sh --role coordinator --clean_db --clean_raft --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
./deploy_server.sh --role store --clean_db --clean_raft --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
./deploy_server.sh --role index --clean_db --clean_raft --server_num=${DEPLOY_SERVER_NUM} --parameters=${DEPLOY_PARAMETER}
sleep 1
echo "deploy all"

./start_server.sh --role coordinator --server_num=${DEPLOY_SERVER_NUM}
./start_server.sh --role store --server_num=${DEPLOY_SERVER_NUM}
./start_server.sh --role index --server_num=${DEPLOY_SERVER_NUM}
echo "start all"

