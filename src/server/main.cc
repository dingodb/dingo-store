// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>

#include "brpc/server.h"
#include "gflags/gflags.h"

#include "proto/store.pb.h"
#include "proto/coordinator.pb.h"
#include "server/store_service.h"
#include "server/coordinator_service.h"
#include "server/server.h"


DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_string(role, "store", "server role [store|coordinator]");


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  dingodb::Server dingodb_server;

  brpc::Server server;
  dingodb::StoreServiceImpl store_service(dingodb_server.get_storage());
  dingodb::CoordinatorServiceImpl coordinator_service;
  if (FLAGS_role == "coordinator") {
    if (server.AddService(&coordinator_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add coordinator service";
      return -1;
    }

  } else if (FLAGS_role == "store") {
    dingodb_server.InitConfigs(std::vector<std::string>{"/tmp/abc"});
    dingodb_server.InitLog();
    dingodb_server.ValiateCoordinator();
    dingodb_server.InitServerID();
    dingodb_server.InitRaftNodeManager();
    dingodb_server.InitStorage();

    if (server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add store service";
      return -1;
    }
  }

  if (server.Start(FLAGS_port, NULL) != 0) {
    LOG(ERROR) << "Fail to start Server";
    return -1;
  }
  LOG(INFO) << "service is running on" << server.listen_address();

  // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
  LOG(INFO) << "service is going to quit";

  server.Stop(0);
  server.Join();
  dingodb_server.Destroy();

  return 0;
}
