./stop.sh --role coordinator
./stop.sh --role store
echo "stop all"
sleep 1

rm -rf ../dist/*
sleep 1
echo "rm -rf all cluster files"

./deploy_server.sh --role coordinator
./deploy_server.sh --role store
sleep 1
echo "deploy all"

./start_server.sh --role coordinator
./start_server.sh --role store
echo "start all"

