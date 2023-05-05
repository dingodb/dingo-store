#!/bin/bash

sleep 60

# 运行 Java 程序
java -cp /opt/dingo-store/build/bin/dingo-mysql-init-0.6.0-SNAPSHOT.jar io.dingodb.mysql.MysqlInit $SERVER_HOST:$COORDINATOR_SERVER_START_PORT

# 检查退出状态
if [ $? -eq 0 ]
then
  echo "Java mysql 初始化成功"
else
  echo "Java mysql 初始化失败"
fi
