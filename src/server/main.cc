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
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/store.pb.h"
#include "server/coordinator_service.h"
#include "server/meta_service.h"
#include "server/server.h"
#include "server/store_service.h"

DEFINE_string(conf, "", "server config");
DEFINE_string(role, "", "server role [store|coordinator]");

// Get server endpoint from config
butil::EndPoint GetServerEndPoint(std::shared_ptr<dingodb::Config> config) {
  const std::string host = config->GetString("server.host");
  const int port = config->GetInt("server.port");
  return dingodb::Helper::GetEndPoint(host, port);
}

// Get raft endpoint from config
butil::EndPoint GetRaftEndPoint(std::shared_ptr<dingodb::Config> config) {
  const std::string host = config->GetString("raft.host");
  const int port = config->GetInt("raft.port");
  return dingodb::Helper::GetEndPoint(host, port);
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  dingodb::pb::common::ClusterRole role = dingodb::pb::common::COORDINATOR;

  auto is_coodinator = dingodb::Helper::IsEqualIgnoreCase(
      FLAGS_role, dingodb::pb::common::ClusterRole_Name(dingodb::pb::common::ClusterRole::COORDINATOR));
  auto is_store = dingodb::Helper::IsEqualIgnoreCase(
      FLAGS_role, dingodb::pb::common::ClusterRole_Name(dingodb::pb::common::ClusterRole::STORE));

  if (is_store) {
    role = dingodb::pb::common::STORE;
  } else if (is_coodinator) {
    role = dingodb::pb::common::COORDINATOR;
  } else {
    LOG(ERROR) << "Invalid server role[" + FLAGS_role + "]";
    return -1;
  }

  if (FLAGS_conf.empty()) {
    LOG(ERROR) << "Missing server config.";
    return -1;
  }

  auto *dingodb_server = dingodb::Server::GetInstance();
  dingodb_server->SetRole(role);
  if (!dingodb_server->InitConfig(FLAGS_conf)) {
    LOG(ERROR) << "InitConfig failed!";
    return -1;
  }
  if (!dingodb_server->InitLog()) {
    LOG(ERROR) << "InitLog failed!";
    return -1;
  }
  if (!dingodb_server->InitServerID()) {
    LOG(ERROR) << "InitServerID failed!";
    return -1;
  }

  std::shared_ptr<dingodb::Config> const config = dingodb::ConfigManager::GetInstance()->GetConfig(role);
  dingodb_server->SetServerEndpoint(GetServerEndPoint(config));
  dingodb_server->SetRaftEndpoint(GetRaftEndPoint(config));

  if (!dingodb_server->InitEngines()) {
    LOG(ERROR) << "InitEngines failed!";
    return -1;
  }

  brpc::Server server;
  dingodb::CoordinatorControl coordinator_control;
  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::MetaServiceImpl meta_service;
  dingodb::StoreServiceImpl store_service;
  // raft server
  brpc::Server raft_server;
  if (is_coodinator) {
    // init CoordinatorController
    coordinator_control.Init();
    coordinator_service.SetControl(&coordinator_control);
    meta_service.SetControl(&coordinator_control);

    // add service to brpc
    if (server.AddService(&coordinator_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add coordinator service!";
      return -1;
    }
    if (server.AddService(&meta_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add meta service!";
      return -1;
    }

    if (braft::add_service(&raft_server, dingodb_server->RaftEndpoint()) != 0) {
      LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }
    if (raft_server.Start(dingodb_server->RaftEndpoint(), nullptr) != 0) {
      LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    LOG(INFO) << "Raft server is running on " << raft_server.listen_address();

    // start meta region
    auto engine = dingodb_server->GetEngine(dingodb::pb::common::Engine::ENG_RAFT_STORE);
    dingodb::pb::error::Errno status = dingodb_server->StartMetaRegion(config, engine);
    if (status != dingodb::pb::error::Errno::OK) {
      LOG(INFO) << "Init RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    // build in-memory meta cache
    // TODO: load data from kv engine into maps

  } else if (is_store) {
    if (!dingodb_server->InitCoordinatorInteraction()) {
      LOG(ERROR) << "InitCoordinatorInteraction failed!";
      return -1;
    }
    if (!dingodb_server->ValiateCoordinator()) {
      LOG(ERROR) << "ValiateCoordinator failed!";
      return -1;
    }
    if (!dingodb_server->InitStorage()) {
      LOG(ERROR) << "InitStorage failed!";
      return -1;
    }
    if (!dingodb_server->InitStoreMetaManager()) {
      LOG(ERROR) << "InitStoreMetaManager failed!";
      return -1;
    }
    if (!dingodb_server->InitCrontabManager()) {
      LOG(ERROR) << "InitCrontabManager failed!";
      return -1;
    }

    store_service.set_storage(dingodb_server->GetStorage());
    if (server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    // raft server
    if (braft::add_service(&raft_server, dingodb_server->RaftEndpoint()) != 0) {
      LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }
    if (raft_server.Start(dingodb_server->RaftEndpoint(), nullptr) != 0) {
      LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    LOG(INFO) << "Raft server is running on " << raft_server.listen_address();
  }

  if (!dingodb_server->Recover()) {
    LOG(ERROR) << "Recover failed!";
    return -1;
  }

  // Start server after raft server started.
  if (server.Start(dingodb_server->ServerEndpoint(), nullptr) != 0) {
    LOG(ERROR) << "Fail to start server!";
    return -1;
  }
  LOG(INFO) << "Server is running on " << server.listen_address();

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
