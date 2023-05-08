#!/bin/bash
BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
TMP_COORDINATOR_SERVICES=$BASE_DIR/build/bin/coor_list

echo "# dingo-store coordinators" > ${TMP_COORDINATOR_SERVICES}
echo ${COOR_SRV_PEERS} | tr ',' '\n' >> ${TMP_COORDINATOR_SERVICES}


cd /opt/dingo-store/build/bin/ || exit 1
DINGODB_HAVE_STORE_AVAILABLE=0

while [ "${DINGODB_HAVE_STORE_AVAILABLE}" -eq 0 ]; do
    echo "DINGODB_HAVE_STORE_AVAILABLE = 0, wait 1 second"
    sleep 1
    DINGODB_HAVE_STORE_AVAILABLE=$(./dingodb_client_coordinator --method=GetStoreMap 2>&1 >/dev/null |grep -c DINGODB_HAVE_STORE_AVAILABLE)
done

echo "DINGODB_HAVE_STORE_AVAILABLE = 1, start to initialize MySQL"
cd - || exit 1

# Run Java mysql initialize
java -cp /opt/dingo-store/build/bin/dingo-mysql-init-0.6.0-SNAPSHOT.jar io.dingodb.mysql.MysqlInit "${SERVER_HOST}:${COORDINATOR_SERVER_START_PORT}"

# Check if Java mysql initialize success
if [ $? -eq 0 ]
then
  echo "Java MySQL initialize success"
else
  echo "Java MySQL initialize failed"
fi
