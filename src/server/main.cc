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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <libunwind.h>
#include <unistd.h>

#include <csignal>
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
#include "server/node_service.h"
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

static void SignalHandler(int signo) {
  printf("========== handle signal '%d' ==========\n", signo);
  unw_context_t context;
  unw_cursor_t cursor;
  unw_getcontext(&context);
  unw_init_local(&cursor, &context);
  int i = 0;

  do {
    unw_word_t ip, offset;
    char symbol[256];

    unw_word_t pc, sp;
    unw_get_reg(&cursor, UNW_REG_IP, &pc);
    unw_get_reg(&cursor, UNW_REG_SP, &sp);
    Dl_info info = {};

    // Get the instruction pointer and symbol name for this frame
    unw_get_reg(&cursor, UNW_REG_IP, &ip);
    unw_get_proc_name(&cursor, symbol, sizeof(symbol), &offset);

    if (dladdr((void *)pc, &info)) {
      // Print the frame number, instruction pointer, .so filename, and symbol name
      printf("Frame %d: [0x%016zx] %32s : %s + 0x%lx\n", i++, (void *)ip, info.dli_fname, symbol, offset);
    }

  } while (unw_step(&cursor) > 0);

  exit(0);
}

void SetupSignalHandler() {
  sighandler_t s;
  s = signal(SIGTERM, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGTERM\n");
    exit(-1);
  }
  s = signal(SIGSEGV, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGSEGV\n");
    exit(-1);
  }
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  SetupSignalHandler();

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

  auto *dingo_server = dingodb::Server::GetInstance();
  dingo_server->SetRole(role);
  if (!dingo_server->InitConfig(FLAGS_conf)) {
    LOG(ERROR) << "InitConfig failed!";
    return -1;
  }
  if (!dingo_server->InitLog()) {
    LOG(ERROR) << "InitLog failed!";
    return -1;
  }
  if (!dingo_server->InitServerID()) {
    LOG(ERROR) << "InitServerID failed!";
    return -1;
  }

  std::shared_ptr<dingodb::Config> const config = dingodb::ConfigManager::GetInstance()->GetConfig(role);
  dingo_server->SetServerEndpoint(GetServerEndPoint(config));
  dingo_server->SetRaftEndpoint(GetRaftEndPoint(config));

  if (!dingo_server->InitRawEngines()) {
    LOG(ERROR) << "InitRawEngines failed!";
    return -1;
  }
  if (!dingo_server->InitEngines()) {
    LOG(ERROR) << "InitEngines failed!";
    return -1;
  }

  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::MetaServiceImpl meta_service;
  dingodb::StoreServiceImpl store_service;
  dingodb::NodeServiceImpl node_service;

  node_service.SetServer(dingo_server);

  brpc::Server brpc_server;
  brpc::Server raft_server;

  if (brpc_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add node service to brpc_server!";
    return -1;
  }

  if (raft_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add node service raft_server!";
    return -1;
  }

  if (is_coodinator) {
    coordinator_service.SetControl(dingo_server->GetCoordinatorControl());
    meta_service.SetControl(dingo_server->GetCoordinatorControl());

    // the Engine should be init success
    auto engine = dingo_server->GetEngine(dingodb::pb::common::Engine::ENG_RAFT_STORE);
    coordinator_service.SetKvEngine(engine);
    meta_service.SetKvEngine(engine);

    // add service to brpc
    if (brpc_server.AddService(&coordinator_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add coordinator service!";
      return -1;
    }
    if (brpc_server.AddService(&meta_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add meta service!";
      return -1;
    }

    if (braft::add_service(&raft_server, dingo_server->RaftEndpoint()) != 0) {
      LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }
    if (raft_server.Start(dingo_server->RaftEndpoint(), nullptr) != 0) {
      LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    LOG(INFO) << "Raft server is running on " << raft_server.listen_address();

    // start meta region
    butil::Status const status = dingo_server->StartMetaRegion(config, engine);
    if (!status.ok()) {
      LOG(INFO) << "Init RaftNode and StateMachine Failed:" << status;
      return -1;
    }

    // build in-memory meta cache
    // TODO: load data from kv engine into maps
  } else if (is_store) {
    if (!dingo_server->InitCoordinatorInteraction()) {
      LOG(ERROR) << "InitCoordinatorInteraction failed!";
      return -1;
    }
    if (!dingo_server->ValiateCoordinator()) {
      LOG(ERROR) << "ValiateCoordinator failed!";
      return -1;
    }
    if (!dingo_server->InitStorage()) {
      LOG(ERROR) << "InitStorage failed!";
      return -1;
    }
    if (!dingo_server->InitStoreMetaManager()) {
      LOG(ERROR) << "InitStoreMetaManager failed!";
      return -1;
    }
    if (!dingo_server->InitCrontabManager()) {
      LOG(ERROR) << "InitCrontabManager failed!";
      return -1;
    }

    store_service.set_storage(dingo_server->GetStorage());
    if (brpc_server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    // raft server
    if (braft::add_service(&raft_server, dingo_server->RaftEndpoint()) != 0) {
      LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }
    if (raft_server.Start(dingo_server->RaftEndpoint(), nullptr) != 0) {
      LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    LOG(INFO) << "Raft server is running on " << raft_server.listen_address();
  }

  if (!dingo_server->Recover()) {
    LOG(ERROR) << "Recover failed!";
    return -1;
  }

  // Start server after raft server started.
  if (brpc_server.Start(dingo_server->ServerEndpoint(), nullptr) != 0) {
    LOG(ERROR) << "Fail to start server!";
    return -1;
  }
  LOG(INFO) << "Server is running on " << brpc_server.listen_address();

  // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
  LOG(INFO) << "Server is going to quit";

  raft_server.Stop(0);
  brpc_server.Stop(0);
  raft_server.Join();
  brpc_server.Join();

  dingo_server->Destroy();

  return 0;
}
