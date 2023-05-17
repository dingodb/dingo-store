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
#include <cstdio>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "butil/string_printf.h"
#include "gflags/gflags_declare.h"

#endif
#include <backtrace.h>
#include <cxxabi.h>
#include <dlfcn.h>
#include <libunwind.h>
#include <unistd.h>

#include <csignal>
#include <iostream>

#include "brpc/server.h"
#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/store.pb.h"
#include "server/cluster_service.h"
#include "server/coordinator_service.h"
#include "server/meta_service.h"
#include "server/node_service.h"
#include "server/push_service.h"
#include "server/server.h"
#include "server/store_service.h"
#include "server/version_service.h"

DEFINE_string(conf, "", "server config");
DEFINE_string(role, "", "server role [store|coordinator]");
DEFINE_string(git_commit_hash, GIT_VERSION, "current git commit version");
DEFINE_string(git_tag_name, GIT_TAG_NAME, "current dingo version");

namespace bvar {
DECLARE_int32(bvar_max_dump_multi_dimension_metric_number);
}  // namespace bvar

namespace bthread {
DECLARE_int32(bthread_concurrency);
}  // namespace bthread

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

std::vector<butil::EndPoint> GetEndpoints(const std::shared_ptr<dingodb::Config> &config,
                                          const std::string peer_nodes_name) {
  std::vector<butil::EndPoint> peer_nodes;
  std::string coordinator_list = config->GetString(peer_nodes_name);
  return dingodb::Helper::StrToEndpoints(coordinator_list);
}

struct DingoStackTraceInfo {
  char *filename;
  int lineno;
  char *function;
  uintptr_t pc;
};

/* Passed to backtrace callback function.  */
struct DingoBacktraceData {
  struct DingoStackTraceInfo *all;
  size_t index;
  size_t max;
  int failed;
};

int BacktraceCallback(void *vdata, uintptr_t pc, const char *filename, int lineno, const char *function) {
  struct DingoBacktraceData *data = (struct DingoBacktraceData *)vdata;
  struct DingoStackTraceInfo *p;

  if (data->index >= data->max) {
    fprintf(stderr, "callback_one: callback called too many times\n");  // NOLINT
    data->failed = 1;
    return 1;
  }

  p = &data->all[data->index];

  // filename
  if (filename == nullptr)
    p->filename = nullptr;
  else {
    p->filename = strdup(filename);
    assert(p->filename != nullptr);
  }

  // lineno
  p->lineno = lineno;

  // function
  if (function == nullptr)
    p->function = nullptr;
  else {
    p->function = strdup(function);
    assert(p->function != nullptr);
  }

  // pc
  if (pc != 0) {
    p->pc = pc;
  }

  ++data->index;

  return 0;
}

/* An error callback passed to backtrace.  */

void ErrorCallback(void *vdata, const char *msg, int errnum) {
  struct DingoBacktraceData *data = (struct DingoBacktraceData *)vdata;

  fprintf(stderr, "%s", msg);                                 // NOLINT
  if (errnum > 0) fprintf(stderr, ": %s", strerror(errnum));  // NOLINT
  fprintf(stderr, "\n");                                      // NOLINT
  data->failed = 1;
}

// The signal handler
#define MAX_STACKTRACE_SIZE 128
static void SignalHandler(int signo) {
  std::cerr << "Received signal " << signo << std::endl;
  std::cerr << "Stack trace:" << std::endl;
  DINGO_LOG(ERROR) << "Received signal " << signo;
  DINGO_LOG(ERROR) << "Stack trace:";

  struct backtrace_state *state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
  if (state == nullptr) {
    std::cerr << "state is null" << std::endl;
  }

  struct DingoStackTraceInfo all[MAX_STACKTRACE_SIZE];
  struct DingoBacktraceData data;

  data.all = &all[0];
  data.index = 0;
  data.max = MAX_STACKTRACE_SIZE;
  data.failed = 0;

  int i = backtrace_full(state, 0, BacktraceCallback, ErrorCallback, &data);
  if (i != 0) {
    std::cerr << "backtrace_full failed" << std::endl;
    DINGO_LOG(ERROR) << "backtrace_full failed";
  }

  for (size_t x = 0; x < data.index; x++) {
    int status;
    char *nameptr = all[x].function;
    char *demangled = abi::__cxa_demangle(all[x].function, nullptr, nullptr, &status);
    if (status == 0 && demangled) {
      nameptr = demangled;
    }

    Dl_info info = {};

    if (!dladdr((void *)all[x].pc, &info)) {
      auto error_msg = butil::string_printf("#%zu source[%s:%d] symbol[%s] pc[0x%0lx]", x, all[x].filename,
                                            all[x].lineno, nameptr, static_cast<uint64_t>(all[x].pc));
      DINGO_LOG(ERROR) << error_msg;
      std::cout << error_msg << std::endl;
    } else {
      auto error_msg = butil::string_printf(
          "#%zu source[%s:%d] symbol[%s] pc[0x%0lx] fname[%s] fbase[0x%lx] sname[%s] saddr[0x%lx] ", x, all[x].filename,
          all[x].lineno, nameptr, static_cast<uint64_t>(all[x].pc), info.dli_fname, (uint64_t)info.dli_fbase,
          info.dli_sname, (uint64_t)info.dli_saddr);
      DINGO_LOG(ERROR) << error_msg;
      std::cout << error_msg << std::endl;
    }
    if (demangled) {
      free(demangled);
    }
  }

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    DINGO_LOG(ERROR) << "graceful shutdown";
    exit(0);
  } else {
    // abort to generate core dump
    DINGO_LOG(ERROR) << "abort to generate core dump for signo=" << signo << " " << strsignal(signo);
    abort();
  }
}

static void SignalHandlerWithoutLineno(int signo) {
  printf("========== handle signal '%d' ==========\n", signo);
  unw_context_t context;
  unw_cursor_t cursor;
  unw_getcontext(&context);
  unw_init_local(&cursor, &context);
  int i = 0;
  char buffer[2048];

  do {
    unw_word_t ip, offset;
    char symbol[256];

    unw_word_t pc, sp;
    unw_get_reg(&cursor, UNW_REG_IP, &pc);
    unw_get_reg(&cursor, UNW_REG_SP, &sp);
    Dl_info info = {};

    // Get the instruction pointer and symbol name for this frame
    unw_get_reg(&cursor, UNW_REG_IP, &ip);

    if (unw_get_proc_name(&cursor, symbol, sizeof(symbol), &offset) == 0) {
      char *nameptr = symbol;
      // Demangle the symbol name
      int demangle_status;
      char *demangled = abi::__cxa_demangle(symbol, nullptr, nullptr, &demangle_status);
      if (demangled) {
        nameptr = demangled;
      }
      // std::cout << "  " << nameptr << " + " << offset << " (0x" << std::hex << pc << ")" << std::endl;

      if (!dladdr((void *)pc, &info)) {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") ";
        std::string const error_msg = string_stream.str();
        DINGO_LOG(ERROR) << error_msg;
        std::cout << error_msg << std::endl;
      } else {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") "
                      << " fname=[" << info.dli_fname << "] saddr=[" << info.dli_saddr << "] fbase=[" << info.dli_fbase
                      << "]";
        std::string const error_msg = string_stream.str();
        DINGO_LOG(ERROR) << error_msg;
        std::cout << error_msg << std::endl;
      }
      if (demangled) {
        free(demangled);
      }
    }

  } while (unw_step(&cursor) > 0);

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    DINGO_LOG(ERROR) << "graceful shutdown";
    exit(0);
  } else {
    // abort to generate core dump
    DINGO_LOG(ERROR) << "abort to generate core dump for signo=" << signo << " " << strsignal(signo);
    abort();
  }
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
  s = signal(SIGFPE, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGFPE\n");
    exit(-1);
  }
  s = signal(SIGBUS, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGBUS\n");
    exit(-1);
  }
  s = signal(SIGILL, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGILL\n");
    exit(-1);
  }
}

// Modify gflag variable
bool SetGflagVariable() {
  // Open bvar multi dimesion metrics.
  if (bvar::FLAGS_bvar_max_dump_multi_dimension_metric_number == 0) {
    if (google::SetCommandLineOption(
            "bvar_max_dump_multi_dimension_metric_number",
            std::to_string(dingodb::Constant::kBvarMaxDumpMultiDimensionMetricNumberDefault).c_str())
            .empty()) {
      DINGO_LOG(ERROR) << "Fail to set bvar_max_dump_multi_dimension_metric_number";
      return false;
    }
  }

  return true;
}

// Get worker thread num used by config
int GetWorkerThreadNum(std::shared_ptr<dingodb::Config> config) {
  int num = config->GetInt("server.worker_thread_num");
  if (num <= 0) {
    double ratio = config->GetDouble("server.worker_thread_ratio");
    if (ratio > 0) {
      num = std::round(ratio * static_cast<double>(dingodb::Helper::GetCoreNum()));
    }
  }

  if (num < bthread::FLAGS_bthread_concurrency) {
    if (google::SetCommandLineOption("bthread_concurrency", std::to_string(num).c_str()).empty()) {
      DINGO_LOG(ERROR) << "Fail to set bthread_concurrency";
    }
  }

  return num;
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingodb::FLAGS_show_version) {
    printf("Dingo-Store version:[%s] with git commit hash:[%s]\n", FLAGS_git_tag_name.c_str(),
           FLAGS_git_commit_hash.c_str());
    exit(-1);
  }

  SetupSignalHandler();

  if (!SetGflagVariable()) {
    return -1;
  }

  dingodb::pb::common::ClusterRole role = dingodb::pb::common::COORDINATOR;

  auto is_coordinator = dingodb::Helper::IsEqualIgnoreCase(
      FLAGS_role, dingodb::pb::common::ClusterRole_Name(dingodb::pb::common::ClusterRole::COORDINATOR));
  auto is_store = dingodb::Helper::IsEqualIgnoreCase(
      FLAGS_role, dingodb::pb::common::ClusterRole_Name(dingodb::pb::common::ClusterRole::STORE));

  if (is_store) {
    role = dingodb::pb::common::STORE;
  } else if (is_coordinator) {
    role = dingodb::pb::common::COORDINATOR;
  } else {
    DINGO_LOG(ERROR) << "Invalid server role[" + FLAGS_role + "]";
    return -1;
  }

  if (FLAGS_conf.empty()) {
    DINGO_LOG(ERROR) << "Missing server config.";
    return -1;
  }

  auto *dingo_server = dingodb::Server::GetInstance();
  dingo_server->SetRole(role);
  if (!dingo_server->InitConfig(FLAGS_conf)) {
    DINGO_LOG(ERROR) << "InitConfig failed!";
    return -1;
  }

  auto const config = dingodb::ConfigManager::GetInstance()->GetConfig(role);
  auto placeholder = dingo_server->InitLog();
  if (!placeholder) {
    DINGO_LOG(ERROR) << "InitLog failed!";
    return -1;
  }
  if (!dingo_server->InitServerID()) {
    DINGO_LOG(ERROR) << "InitServerID failed!";
    return -1;
  }

  dingo_server->SetServerEndpoint(GetServerEndPoint(config));
  dingo_server->SetRaftEndpoint(GetRaftEndPoint(config));

  if (!dingo_server->InitRawEngine()) {
    DINGO_LOG(ERROR) << "InitRawEngine failed!";
    return -1;
  }
  if (!dingo_server->InitEngine()) {
    DINGO_LOG(ERROR) << "InitEngine failed!";
    return -1;
  }

  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::MetaServiceImpl meta_service;
  dingodb::StoreServiceImpl store_service;
  dingodb::NodeServiceImpl node_service;
  dingodb::PushServiceImpl push_service;
  dingodb::VersionServiceProtoImpl version_service;
  dingodb::ClusterStatImpl cluster_stat_service;

  node_service.SetServer(dingo_server);

  brpc::Server brpc_server;
  brpc::Server raft_server;

  brpc::ServerOptions options;
  options.num_threads = GetWorkerThreadNum(config);
  DINGO_LOG(INFO) << "num_threads: " << options.num_threads;

  if (brpc_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    DINGO_LOG(ERROR) << "Fail to add node service to brpc_server!";
    return -1;
  }

  if (brpc_server.AddService(&version_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add node service to brpc_server!";
    return -1;
  }

  if (raft_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    DINGO_LOG(ERROR) << "Fail to add node service raft_server!";
    return -1;
  }

  if (is_coordinator) {
    if (!dingo_server->InitCoordinatorInteractionForAutoIncrement()) {
      DINGO_LOG(ERROR) << "InitCoordinatorInteractionForAutoIncrement failed!";
      return -1;
    }

    dingo_server->SetEndpoints(GetEndpoints(config, "coordinator.peers"));

    coordinator_service.SetControl(dingo_server->GetCoordinatorControl());
    coordinator_service.SetAutoIncrementControl(dingo_server->GetAutoIncrementControlReference());
    meta_service.SetControl(dingo_server->GetCoordinatorControl());
    meta_service.SetAutoIncrementControl(dingo_server->GetAutoIncrementControlReference());

    // the Engine should be init success
    auto engine = dingo_server->GetEngine();
    coordinator_service.SetKvEngine(engine);
    meta_service.SetKvEngine(engine);

    // add service to brpc
    if (brpc_server.AddService(&coordinator_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add coordinator service!";
      return -1;
    }
    if (brpc_server.AddService(&meta_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add meta service!";
      return -1;
    }

    if (braft::add_service(&raft_server, dingo_server->RaftEndpoint()) != 0) {
      DINGO_LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }

    // Add Cluster Stat Service to get meta information from dingodb cluster
    cluster_stat_service.SetControl(dingo_server->GetCoordinatorControl());
    if (0 != brpc_server.AddService(&cluster_stat_service, brpc::SERVER_OWNS_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add cluster stat service";
      return -1;
    }

    if (raft_server.Start(dingo_server->RaftEndpoint(), &options) != 0) {
      DINGO_LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    DINGO_LOG(INFO) << "Raft server is running on " << raft_server.listen_address();

    // start meta region
    butil::Status status = dingo_server->StartMetaRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Meta RaftNode and StateMachine Failed:" << status;
      return -1;
    }

    status = dingo_server->StartAutoIncrementRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Auto Increment RaftNode and StateMachine Failed:" << status;
      return -1;
    }

    // build in-memory meta cache
    // TODO: load data from kv engine into maps
  } else if (is_store) {
    if (!dingo_server->InitCoordinatorInteraction()) {
      DINGO_LOG(ERROR) << "InitCoordinatorInteraction failed!";
      return -1;
    }
    if (!dingo_server->ValiateCoordinator()) {
      DINGO_LOG(ERROR) << "ValiateCoordinator failed!";
      return -1;
    }
    if (!dingo_server->InitStorage()) {
      DINGO_LOG(ERROR) << "InitStorage failed!";
      return -1;
    }
    if (!dingo_server->InitStoreMetaManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetaManager failed!";
      return -1;
    }
    if (!dingo_server->InitStoreMetricsManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetricsManager failed!";
      return -1;
    }
    if (!dingo_server->InitStoreController()) {
      DINGO_LOG(ERROR) << "InitStoreController failed!";
      return -1;
    }
    if (!dingo_server->InitRegionCommandManager()) {
      DINGO_LOG(ERROR) << "InitRegionCommandManager failed!";
      return -1;
    }
    if (!dingo_server->InitRegionController()) {
      DINGO_LOG(ERROR) << "InitRegionController failed!";
      return -1;
    }

    store_service.SetStorage(dingo_server->GetStorage());
    if (brpc_server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    // add push service to server_location
    if (brpc_server.AddService(&push_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    // raft server
    if (braft::add_service(&raft_server, dingo_server->RaftEndpoint()) != 0) {
      DINGO_LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }

    if (raft_server.Start(dingo_server->RaftEndpoint(), &options) != 0) {
      DINGO_LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    DINGO_LOG(INFO) << "Raft server is running on " << raft_server.listen_address();
  }

  if (!dingo_server->Recover()) {
    DINGO_LOG(ERROR) << "Recover failed!";
    return -1;
  }

  if (!dingo_server->InitHeartbeat()) {
    DINGO_LOG(ERROR) << "InitHeartbeat failed!";
    return -1;
  }

  if (!dingo_server->InitCrontabManager()) {
    DINGO_LOG(ERROR) << "InitCrontabManager failed!";
    return -1;
  }

  // Start server after raft server started.
  if (brpc_server.Start(dingo_server->ServerEndpoint(), &options) != 0) {
    DINGO_LOG(ERROR) << "Fail to start server!";
    return -1;
  }
  DINGO_LOG(INFO) << "Server is running on " << brpc_server.listen_address();
  DINGO_LOG(INFO) << "Current Git Commit version is:" << FLAGS_git_commit_hash;
  DINGO_LOG(INFO) << "Current branch tag is:" << FLAGS_git_tag_name;

  // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
  DINGO_LOG(INFO) << "Server is going to quit";

  raft_server.Stop(0);
  brpc_server.Stop(0);
  raft_server.Join();
  brpc_server.Join();

  dingo_server->Destroy();

  return 0;
}
