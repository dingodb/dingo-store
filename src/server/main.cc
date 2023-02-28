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
#include "butil/endpoint.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "gflags/gflags.h"
#include "proto/coordinator.pb.h"
#include "proto/store.pb.h"
#include "server/coordinator_service.h"
#include "server/server.h"
#include "server/store_service.h"

DEFINE_string(conf, "", "server config");
DEFINE_string(role, "", "server role [store|coordinator]");

// Get server endpoint from config
butil::EndPoint getServerEndPoint(std::shared_ptr<dingodb::Config> config) {
  const std::string host = config->GetString("server.host");
  const int port = config->GetInt("server.port");

  butil::ip_t ip;
  if (host.empty()) {
    ip = butil::IP_ANY;
  } else {
    if (dingodb::Helper::IsIp(host)) {
      butil::str2ip(host.c_str(), &ip);
    } else {
      butil::hostname2ip(host.c_str(), &ip);
    }
  }

  return butil::EndPoint(ip, port);
}

// Get raft endpoint from config
butil::EndPoint getRaftEndPoint(std::shared_ptr<dingodb::Config> config) {
  const std::string host = config->GetString("raft.host");
  const int port = config->GetInt("raft.port");

  butil::ip_t ip;
  if (host.empty()) {
    ip = butil::IP_ANY;
  } else {
    if (dingodb::Helper::IsIp(host)) {
      butil::str2ip(host.c_str(), &ip);
    } else {
      butil::hostname2ip(host.c_str(), &ip);
    }
  }

  return butil::EndPoint(ip, port);
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_role != "coordinator" && FLAGS_role != "store") {
    LOG(ERROR) << "Invalid server role, just [store|coordinator].";
    return -1;
  }
  if (FLAGS_conf.empty()) {
    LOG(ERROR) << "Missing server config.";
    return -1;
  }

  auto dingodb_server = dingodb::Server::GetInstance();
  dingodb_server->set_role(FLAGS_role);
  dingodb_server->InitConfig(FLAGS_conf);
  dingodb_server->InitLog();
  dingodb_server->InitEngines();

  dingodb_server->set_server_endpoint(getServerEndPoint(
      dingodb::ConfigManager::GetInstance()->GetConfig(FLAGS_role)));
  dingodb_server->set_raft_endpoint(getRaftEndPoint(
      dingodb::ConfigManager::GetInstance()->GetConfig(FLAGS_role)));

  brpc::Server server;
  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::StoreServiceImpl store_service;
  if (FLAGS_role == "coordinator") {
    if (server.AddService(&coordinator_service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add coordinator service";
      return -1;
    }

  } else if (FLAGS_role == "store") {
    dingodb_server->ValiateCoordinator();
    dingodb_server->InitServerID();
    dingodb_server->InitRaftNodeManager();
    dingodb_server->InitStorage();
    dingodb_server->InitStoreMetaManager();
    dingodb_server->InitCrontabManager();

    store_service.set_storage(dingodb_server->get_storage());
    if (server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0) {
      LOG(ERROR) << "Fail to add store service";
      return -1;
    }
  }

  if (server.Start(dingodb_server->get_server_endpoint(), NULL) != 0) {
    LOG(ERROR) << "Fail to start server";
    return -1;
  }
  LOG(INFO) << "Server is running on " << server.listen_address();

  // raft server
  brpc::Server raft_server;
  if (braft::add_service(&raft_server, dingodb_server->get_raft_endpoint()) !=
      0) {
    LOG(ERROR) << "Fail to add raft service";
    return -1;
  }
  if (raft_server.Start(dingodb_server->get_raft_endpoint(), NULL) != 0) {
    LOG(ERROR) << "Fail to start raft server";
    return -1;
  }
  LOG(INFO) << "Raft server is running on " << raft_server.listen_address();

  // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
  LOG(INFO) << "Server is going to quit";

  raft_server.Stop(0);
  server.Stop(0);
  raft_server.Join();
  server.Join();

  dingodb_server->Destroy();

  return 0;
}
