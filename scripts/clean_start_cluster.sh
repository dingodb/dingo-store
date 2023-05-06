DEPLOY_PARAMETER=deploy_parameters

if [ -n "$1" ]
then
    DEPLOY_PARAMETER=$1
fi

echo "DEPLOY_PARAMETER="${DEPLOY_PARAMETER}

./stop.sh --role coordinator
./stop.sh --role store
echo "stop all"
sleep 1

./deploy_server.sh --role coordinator --clean_db --clean_raft --server_num=3 --parameters=${DEPLOY_PARAMETER}
./deploy_server.sh --role store --clean_db --clean_raft --server_num=3 --parameters=${DEPLOY_PARAMETER}
sleep 1
echo "deploy all"

./start_server.sh --role coordinator
./start_server.sh --role store
echo "start all"

