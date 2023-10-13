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
#include <cstdint>
#include <cstdio>
#include <filesystem>
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
#include <cstdlib>  // Replace stdlib.h with cstdlib
#include <filesystem>
#include <iostream>

#include "brpc/server.h"
#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/syscheck.h"
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "server/cluster_service.h"
#include "server/coordinator_service.h"
#include "server/file_service.h"
#include "server/index_service.h"
#include "server/meta_service.h"
#include "server/node_service.h"
#include "server/push_service.h"
#include "server/region_control_service.h"
#include "server/server.h"
#include "server/store_service.h"
#include "server/util_service.h"
#include "server/version_service.h"

DEFINE_string(conf, "", "server config");
DEFINE_string(role, "", "server role [store|coordinator]");
DECLARE_string(coor_url);

DEFINE_uint32(h2_server_max_concurrent_streams, UINT32_MAX, "max concurrent streams");
DEFINE_uint32(h2_server_stream_window_size, 1024 * 1024 * 1024, "stream window size");
DEFINE_uint32(h2_server_connection_window_size, 1024 * 1024 * 1024, "connection window size");
DEFINE_uint32(h2_server_max_frame_size, 16384, "max frame size");
DEFINE_uint32(h2_server_max_header_list_size, UINT32_MAX, "max header list size");

DEFINE_uint32(omp_num_threads, 0, "omp num threads");

extern "C" {
extern void omp_set_dynamic(int dynamic_threads) noexcept;  // NOLINT
extern int omp_get_dynamic(void) noexcept;                  // NOLINT
extern void omp_set_num_threads(int) noexcept;              // NOLINT
extern int omp_get_num_threads(void) noexcept;              // NOLINT
extern int omp_get_max_threads(void) noexcept;              // NOLINT
extern int omp_get_thread_num(void) noexcept;               // NOLINT
extern int omp_get_num_procs(void) noexcept;                // NOLINT
}

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
  std::cerr << "Received signal " << signo << '\n';
  std::cerr << "Stack trace:" << '\n';
  DINGO_LOG(ERROR) << "Received signal " << signo;
  DINGO_LOG(ERROR) << "Stack trace:";

  struct backtrace_state *state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
  if (state == nullptr) {
    std::cerr << "state is null" << '\n';
  }

  struct DingoStackTraceInfo all[MAX_STACKTRACE_SIZE];
  struct DingoBacktraceData data;

  data.all = &all[0];
  data.index = 0;
  data.max = MAX_STACKTRACE_SIZE;
  data.failed = 0;

  int i = backtrace_full(state, 0, BacktraceCallback, ErrorCallback, &data);
  if (i != 0) {
    std::cerr << "backtrace_full failed" << '\n';
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
      std::cout << error_msg << '\n';
    } else {
      auto error_msg = butil::string_printf(
          "#%zu source[%s:%d] symbol[%s] pc[0x%0lx] fname[%s] fbase[0x%lx] sname[%s] saddr[0x%lx] ", x, all[x].filename,
          all[x].lineno, nameptr, static_cast<uint64_t>(all[x].pc), info.dli_fname, (uint64_t)info.dli_fbase,
          info.dli_sname, (uint64_t)info.dli_saddr);
      DINGO_LOG(ERROR) << error_msg;
      std::cout << error_msg << '\n';
    }
    if (demangled) {
      free(demangled);
    }
  }

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    // clean temp directory
    dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance()->GetCheckpointPath());
    DINGO_LOG(ERROR) << "graceful shutdown, clean up checkpoint dir: "
                     << dingodb::Server::GetInstance()->GetCheckpointPath();
    exit(0);
  } else {
    // call abort() to generate core dump
    DINGO_LOG(ERROR) << "call abort() to generate core dump for signo=" << signo << " " << strsignal(signo);
    auto s = signal(SIGABRT, SIG_DFL);
    if (s == SIG_ERR) {
      std::cerr << "Failed to set signal handler to SIG_DFL for SIGABRT" << '\n';
    }
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
      // std::cout << "  " << nameptr << " + " << offset << " (0x" << std::hex << pc << ")" << '\n';

      if (!dladdr((void *)pc, &info)) {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") ";
        std::string const error_msg = string_stream.str();
        DINGO_LOG(ERROR) << error_msg;
        std::cout << error_msg << '\n';
      } else {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") "
                      << " fname=[" << info.dli_fname << "] saddr=[" << info.dli_saddr << "] fbase=[" << info.dli_fbase
                      << "]";
        std::string const error_msg = string_stream.str();
        DINGO_LOG(ERROR) << error_msg;
        std::cout << error_msg << '\n';
      }
      if (demangled) {
        free(demangled);
      }
    }

  } while (unw_step(&cursor) > 0);

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    // clean temp directory
    dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance()->GetCheckpointPath());
    DINGO_LOG(ERROR) << "graceful shutdown, clean up checkpoint dir: "
                     << dingodb::Server::GetInstance()->GetCheckpointPath();
    exit(0);
  } else {
    // call abort() to generate core dump
    DINGO_LOG(ERROR) << "call abort() to generate core dump for signo=" << signo << " " << strsignal(signo);
    auto s = signal(SIGABRT, SIG_DFL);
    if (s == SIG_ERR) {
      std::cerr << "Failed to set signal handler to SIG_DFL for SIGABRT" << '\n';
    }
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
  s = signal(SIGABRT, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGABRT\n");
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

// setup default conf and coor_list
bool SetDefaultConfAndCoorList(const dingodb::pb::common::ClusterRole &role) {
  if (FLAGS_conf.empty()) {
    if (role == dingodb::pb::common::COORDINATOR && std::filesystem::exists("./conf/coordinator.yaml")) {
      FLAGS_conf = "./conf/coordinator.yaml";
    } else if (role == dingodb::pb::common::STORE && std::filesystem::exists("./conf/store.yaml")) {
      FLAGS_conf = "./conf/store.yaml";
    } else if (role == dingodb::pb::common::INDEX && std::filesystem::exists("./conf/index.yaml")) {
      FLAGS_conf = "./conf/index.yaml";
    } else {
      DINGO_LOG(ERROR) << "unknown role:" << role;
      return false;
    }
  }

  if (FLAGS_coor_url.empty() && std::filesystem::exists("./conf/coor_list")) {
    FLAGS_coor_url = "file://./conf/coor_list";
  }

  return true;
}

int main(int argc, char *argv[]) {
  if (dingodb::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingodb::FLAGS_show_version || FLAGS_role.empty()) {
    dingodb::DingoShowVerion();

    printf(
        "Usage: %s --role=[coordinator|store|index] --conf ./conf/[coordinator|store|index].yaml "
        "--coor_url=[file://./conf/coor_list]\n",
        argv[0]);
    printf("Example: \n");
    printf("         bin/dingodb_server --role [coordinator|store|index]\n");
    printf(
        "         bin/dingodb_server --role coordinator --conf ./conf/coordinator.yaml "
        "--coor_url=file://./conf/coor_list\n");
    printf("         bin/dingodb_server --role store --conf ./conf/store.yaml --coor_url=file://./conf/coor_list\n");
    printf("         bin/dingodb_server --role index --conf ./conf/index.yaml --coor_url=file://./conf/coor_list\n");
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
  auto is_index = dingodb::Helper::IsEqualIgnoreCase(
      FLAGS_role, dingodb::pb::common::ClusterRole_Name(dingodb::pb::common::ClusterRole::INDEX));

  if (is_store) {
    role = dingodb::pb::common::STORE;
  } else if (is_coordinator) {
    role = dingodb::pb::common::COORDINATOR;
  } else if (is_index) {
    role = dingodb::pb::common::INDEX;
  } else {
    DINGO_LOG(ERROR) << "Invalid server role[" + FLAGS_role + "]";
    return -1;
  }

  SetDefaultConfAndCoorList(role);

  if (FLAGS_conf.empty()) {
    DINGO_LOG(ERROR) << "Missing server config.";
    return -1;
  } else if (!std::filesystem::exists(FLAGS_conf)) {
    DINGO_LOG(ERROR) << "server config file not exist.";
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

  if (!dingo_server->InitDirectory()) {
    DINGO_LOG(ERROR) << "InitDirectory failed!";
    return -1;
  }

#ifdef LINK_TCMALLOC
  DINGO_LOG(INFO) << "LINK_TCMALLOC is ON";
#ifdef BRPC_ENABLE_CPU_PROFILER
  DINGO_LOG(INFO) << "BRPC_ENABLE_CPU_PROFILER is ON";
#endif
#else
  DINGO_LOG(INFO) << "LINK_TCMALLOC is OFF";
#endif

  // check system env
  auto ret = dingodb::DoSystemCheck();
  if (ret < 0) {
    DINGO_LOG(ERROR) << "DoSystemCheck failed, DingoDB may run with unexpected behavior.";
  }
  DINGO_LOG(INFO) << "DoSystemCheck ret:" << ret;

  const char *omp_num_threads = "OMP_NUM_THREADS";
  const char *omp_num_threads_value = std::getenv(omp_num_threads);
  if (omp_num_threads_value == nullptr) {
    DINGO_LOG(INFO) << "Environment variable " << omp_num_threads << " is not set";
  }
  DINGO_LOG(INFO) << "Environment variable " << omp_num_threads << " is set to " << omp_num_threads_value;

  // setup omp_num_threads
  if (FLAGS_omp_num_threads > 0) {
    omp_set_dynamic(FLAGS_omp_num_threads);
    omp_set_num_threads(FLAGS_omp_num_threads);
    DINGO_LOG(INFO) << "omp_set_num_threads: " << FLAGS_omp_num_threads;

    // const char *value = std::to_string(FLAGS_omp_num_threads).c_str();
    int overwrite = 1;  // set to 1 to overwrite existing value, 0 to keep existing value

    int result = setenv(omp_num_threads, std::to_string(FLAGS_omp_num_threads).c_str(), overwrite);
    if (result != 0) {
      DINGO_LOG(ERROR) << "Failed to set environment variable " << omp_num_threads;
    }
  }

  // setup omp_num_threads
  DINGO_LOG(INFO) << "omp_get_num_threads: " << omp_get_num_threads();
  DINGO_LOG(INFO) << "omp_get_max_threads: " << omp_get_max_threads();
  DINGO_LOG(INFO) << "omp_get_thread_num: " << omp_get_thread_num();
  DINGO_LOG(INFO) << "omp_get_dynamic: " << omp_get_dynamic();
  DINGO_LOG(INFO) << "omp_get_num_procs: " << omp_get_num_procs();

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
  if (!dingo_server->InitLogStorageManager()) {
    DINGO_LOG(ERROR) << "InitLogStorageManager failed!";
    return -1;
  }

  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::MetaServiceImpl meta_service;
  dingodb::StoreServiceImpl store_service;
  dingodb::IndexServiceImpl index_service;
  dingodb::UtilServiceImpl util_service;
  dingodb::RegionControlServiceImpl region_control_service;
  dingodb::NodeServiceImpl node_service;
  dingodb::PushServiceImpl push_service;
  dingodb::VersionServiceProtoImpl version_service;
  dingodb::ClusterStatImpl cluster_stat_service;
  dingodb::FileServiceImpl file_service;

  node_service.SetServer(dingo_server);

  brpc::Server brpc_server;
  brpc::Server raft_server;

  brpc::ServerOptions options;

  options.h2_settings.max_concurrent_streams = FLAGS_h2_server_max_concurrent_streams;
  options.h2_settings.stream_window_size = FLAGS_h2_server_stream_window_size;
  options.h2_settings.connection_window_size = FLAGS_h2_server_connection_window_size;
  options.h2_settings.max_frame_size = FLAGS_h2_server_max_frame_size;
  options.h2_settings.max_header_list_size = FLAGS_h2_server_max_header_list_size;

  DINGO_LOG(INFO) << "h2_settings.max_concurrent_streams: " << options.h2_settings.max_concurrent_streams;
  DINGO_LOG(INFO) << "h2_settings.stream_window_size: " << options.h2_settings.stream_window_size;
  DINGO_LOG(INFO) << "h2_settings.connection_window_size: " << options.h2_settings.connection_window_size;
  DINGO_LOG(INFO) << "h2_settings.max_frame_size: " << options.h2_settings.max_frame_size;
  DINGO_LOG(INFO) << "h2_settings.max_header_list_size: " << options.h2_settings.max_header_list_size;

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
    coordinator_service.SetTsoControl(dingo_server->GetTsoControl());
    meta_service.SetControl(dingo_server->GetCoordinatorControl());
    meta_service.SetAutoIncrementControl(dingo_server->GetAutoIncrementControlReference());
    meta_service.SetTsoControl(dingo_server->GetTsoControl());
    version_service.SetControl(dingo_server->GetCoordinatorControl());

    // the Engine should be init success
    auto engine = dingo_server->GetEngine();
    coordinator_service.SetKvEngine(engine);
    meta_service.SetKvEngine(engine);
    version_service.SetKvEngine(engine);

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
    DINGO_LOG(INFO) << "Meta region start";

    status = dingo_server->StartAutoIncrementRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Auto Increment RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Auto Increment region start";

    status = dingo_server->StartTsoRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Tso RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Tso region start";

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
    if (!dingo_server->InitPreSplitChecker()) {
      DINGO_LOG(ERROR) << "InitPreSplitChecker failed!";
      return -1;
    }

    store_service.SetStorage(dingo_server->GetStorage());
    if (brpc_server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    if (brpc_server.AddService(&region_control_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add region control service!";
      return -1;
    }

    // add push service to server_location
    if (brpc_server.AddService(&push_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add push service!";
      return -1;
    }

    // add file service
    if (brpc_server.AddService(&file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add push service!";
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
  } else if (is_index) {
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
    if (!dingo_server->InitVectorIndexManager()) {
      DINGO_LOG(ERROR) << "InitVectorIndexManager failed!";
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
    if (!dingo_server->InitPreSplitChecker()) {
      DINGO_LOG(ERROR) << "InitPreSplitChecker failed!";
      return -1;
    }

    index_service.SetStorage(dingo_server->GetStorage());
    if (brpc_server.AddService(&index_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add index service!";
      return -1;
    }

    util_service.SetStorage(dingo_server->GetStorage());
    if (brpc_server.AddService(&util_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add util service!";
      return -1;
    }

    if (brpc_server.AddService(&region_control_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add region control service!";
      return -1;
    }

    // add push service to server_location
    if (brpc_server.AddService(&push_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add push service!";
      return -1;
    }

    // add file service
    if (brpc_server.AddService(&file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add push service!";
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
