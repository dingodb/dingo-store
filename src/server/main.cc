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
#include "common/role.h"
#include "gflags/gflags_declare.h"
#include "server/store_metrics_service.h"
#include "server/store_operation_service.h"
#include "server/task_list_service.h"

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
#include "common/role.h"
#include "common/syscheck.h"
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "server/cluster_service.h"
#include "server/coordinator_service.h"
#include "server/debug_service.h"
#include "server/file_service.h"
#include "server/index_service.h"
#include "server/meta_service.h"
#include "server/node_service.h"
#include "server/push_service.h"
#include "server/region_service.h"
#include "server/server.h"
#include "server/store_metrics_service.h"
#include "server/store_operation_service.h"
#include "server/store_service.h"
#include "server/table_service.h"
#include "server/task_list_service.h"
#include "server/util_service.h"
#include "server/version_service.h"

DEFINE_string(conf, "", "server config");
DECLARE_string(coor_url);

DEFINE_uint32(h2_server_max_concurrent_streams, UINT32_MAX, "max concurrent streams");
DEFINE_uint32(h2_server_stream_window_size, 1024 * 1024 * 1024, "stream window size");
DEFINE_uint32(h2_server_connection_window_size, 1024 * 1024 * 1024, "connection window size");
DEFINE_uint32(h2_server_max_frame_size, 16384, "max frame size");
DEFINE_uint32(h2_server_max_header_list_size, UINT32_MAX, "max header list size");

DEFINE_bool(use_pthread_prior_worker_set, true, "use pthread prior worker set");
DEFINE_int32(brpc_common_worker_num, 10, "brpc common worker num");
DEFINE_int32(read_worker_num, 10, "read service worker num");
DEFINE_int32(write_worker_num, 10, "write service worker num");
DEFINE_int64(read_worker_max_pending_num, 0, "read service worker num");
DEFINE_int64(write_worker_max_pending_num, 0, "write service worker num");

DEFINE_int32(raft_apply_worker_num, 10, "raft apply worker num");
DECLARE_int32(raft_apply_worker_max_pending_num);

DEFINE_int32(coordinator_service_worker_num, 10, "service worker num");
DEFINE_int64(coordinator_service_worker_max_pending_num, 0, "service worker num");
DEFINE_int32(meta_service_worker_num, 10, "service worker num");
DEFINE_int64(meta_service_worker_max_pending_num, 0, "service worker num");
DEFINE_int32(version_service_worker_num, 10, "service worker num");
DEFINE_int64(version_service_worker_max_pending_num, 0, "service worker num");

extern "C" {
extern void openblas_set_num_threads(int num_threads);  // NOLINT
}

namespace bvar {
DECLARE_int32(bvar_max_dump_multi_dimension_metric_number);
}  // namespace bvar

namespace bthread {
DECLARE_int32(bthread_concurrency);
}  // namespace bthread

namespace dingodb {
DECLARE_int32(vector_background_worker_num);
DECLARE_int32(vector_fast_background_worker_num);
DECLARE_int64(vector_max_background_task_count);
DECLARE_int32(vector_operation_parallel_thread_num);
}  // namespace dingodb

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
  printf("========== handle signal '%d' ==========\n", signo);

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    // clean temp directory
    dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance().GetCheckpointPath());
    dingodb::Helper::RemoveFileOrDirectory(dingodb::Server::GetInstance().PidFilePath());
    DINGO_LOG(WARNING) << "GRACEFUL SHUTDOWN, clean up checkpoint dir: "
                       << dingodb::Server::GetInstance().GetCheckpointPath()
                       << ", clean up pid_file: " << dingodb::Server::GetInstance().PidFilePath();
    _exit(0);
  }

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

  // call abort() to generate core dump
  DINGO_LOG(ERROR) << "call abort() to generate core dump for signo=" << signo << " " << strsignal(signo);
  auto s = signal(SIGABRT, SIG_DFL);
  if (s == SIG_ERR) {
    std::cerr << "Failed to set signal handler to SIG_DFL for SIGABRT" << '\n';
  }
  abort();
}

static void SignalHandlerWithoutLineno(int signo) {
  printf("========== handle signal '%d' ==========\n", signo);

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    // clean temp directory
    dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance().GetCheckpointPath());
    dingodb::Helper::RemoveFileOrDirectory(dingodb::Server::GetInstance().PidFilePath());
    DINGO_LOG(ERROR) << "GRACEFUL SHUTDOWN, clean up checkpoint dir: "
                     << dingodb::Server::GetInstance().GetCheckpointPath()
                     << ", clean up pid_file: " << dingodb::Server::GetInstance().PidFilePath();
    _exit(0);
  }

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

  // call abort() to generate core dump
  DINGO_LOG(ERROR) << "call abort() to generate core dump for signo=" << signo << " " << strsignal(signo);
  auto s = signal(SIGABRT, SIG_DFL);
  if (s == SIG_ERR) {
    std::cerr << "Failed to set signal handler to SIG_DFL for SIGABRT" << '\n';
  }
  abort();
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
  // ignore SIGPIPE
  s = signal(SIGPIPE, SIG_IGN);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGPIPE\n");
    exit(-1);
  }
}

// Modify gflag variable
bool SetGflagVariable() {
  // Open bvar multi dimesion metrics.
  if (bvar::FLAGS_bvar_max_dump_multi_dimension_metric_number <
      dingodb::Constant::kBvarMaxDumpMultiDimensionMetricNumberDefault) {
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
int InitBthreadWorkerThreadNum(std::shared_ptr<dingodb::Config> config) {
  int32_t num = config->GetInt("server.worker_thread_num");
  if (num <= 0) {
    double ratio = config->GetDouble("server.worker_thread_ratio");
    if (ratio > 0) {
      num = std::round(ratio * static_cast<double>(dingodb::Helper::GetCoreNum()));
    }
  }

  if (num > 4) {
    bthread::FLAGS_bthread_concurrency = num;
  }

  int brpc_common_worker_num = config->GetInt("server.brpc_common_worker_num");
  if (brpc_common_worker_num <= 0) {
    DINGO_LOG(WARNING) << "server.brpc_common_worker_num is not set, use dingodb::FLAGS_brpc_common_worker_num";
  } else {
    FLAGS_brpc_common_worker_num = brpc_common_worker_num;
  }
  if (FLAGS_brpc_common_worker_num <= 0) {
    DINGO_LOG(ERROR) << "server.brpc_common_worker_num is less than 0";
    return -1;
  }
  DINGO_LOG(INFO) << "server.brpc_common_worker_num is set to " << FLAGS_brpc_common_worker_num;

  return bthread::FLAGS_bthread_concurrency;
}

int InitServiceWorkerParameters(std::shared_ptr<dingodb::Config> config, dingodb::pb::common::ClusterRole role) {
  // init service_worker_num
  {
    int read_worker_num = config->GetInt("server.read_worker_num");
    if (read_worker_num <= 0) {
      DINGO_LOG(WARNING) << "server.read_worker_num is not set, use dingodb::FLAGS_read_worker_num";
    } else {
      FLAGS_read_worker_num = read_worker_num;
    }
    if (FLAGS_read_worker_num <= 0) {
      DINGO_LOG(ERROR) << "server.read_worker_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.read_worker_num is set to " << FLAGS_read_worker_num;

    int write_worker_num = config->GetInt("server.write_worker_num");
    if (write_worker_num <= 0) {
      DINGO_LOG(WARNING) << "server.write_worker_num is not set, use dingodb::FLAGS_write_worker_num";
    } else {
      FLAGS_write_worker_num = write_worker_num;
    }
    if (FLAGS_write_worker_num <= 0) {
      DINGO_LOG(ERROR) << "server.write_worker_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.write_worker_num is set to " << FLAGS_write_worker_num;

    // if raft_apply_worker_num is zero, means do not use raft apply worker
    int raft_apply_worker_num = config->GetInt("server.raft_apply_worker_num");
    if (raft_apply_worker_num < 0) {
      DINGO_LOG(WARNING) << "server.raft_apply_worker_num is not set, use dingodb::FLAGS_raft_apply_worker_num";
    } else {
      FLAGS_raft_apply_worker_num = raft_apply_worker_num;
    }
    if (FLAGS_raft_apply_worker_num < 0) {
      DINGO_LOG(ERROR) << "server.raft_apply_worker_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.raft_apply_worker_num is set to " << FLAGS_raft_apply_worker_num;
  }

  // init max pending num
  {
    auto read_max_pending_num = config->GetInt64("server.read_worker_max_pending_num");
    if (read_max_pending_num <= 0) {
      DINGO_LOG(WARNING)
          << "server.read_worker_max_pending_num is not set, use dingodb::FLAGS_read_worker_max_pending_num";
    } else {
      FLAGS_read_worker_max_pending_num = read_max_pending_num;
    }
    if (FLAGS_read_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "server.read_worker_max_pending_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.read_worker_max_pending_num is set to " << FLAGS_read_worker_max_pending_num;

    auto write_max_pending_num = config->GetInt64("server.write_worker_max_pending_num");
    if (write_max_pending_num <= 0) {
      DINGO_LOG(WARNING)
          << "server.write_worker_max_pending_num is not set, use dingodb::FLAGS_write_worker_max_pending_num";
    } else {
      FLAGS_write_worker_max_pending_num = write_max_pending_num;
    }
    if (FLAGS_write_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "server.write_worker_max_pending_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.write_worker_max_pending_num is set to " << FLAGS_write_worker_max_pending_num;

    auto raft_apply_max_pending_num = config->GetInt64("server.raft_apply_worker_max_pending_num");
    if (raft_apply_max_pending_num <= 0) {
      DINGO_LOG(WARNING) << "server.raft_apply_worker_max_pending_num is not set, use "
                            "dingodb::FLAGS_raft_apply_worker_max_pending_num";
    } else {
      FLAGS_raft_apply_worker_max_pending_num = raft_apply_max_pending_num;
    }
    if (FLAGS_raft_apply_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "server.raft_apply_worker_max_pending_num is less than 0";
      return -1;
    }
    DINGO_LOG(INFO) << "server.raft_apply_worker_max_pending_num is set to " << FLAGS_raft_apply_worker_max_pending_num;
  }

  // calc new bthread_concurrency by brpc_common_worker_num
  if (role != dingodb::pb::common::ClusterRole::INDEX) {
    if (FLAGS_use_pthread_prior_worker_set) {
      if (FLAGS_brpc_common_worker_num > bthread::FLAGS_bthread_concurrency) {
        bthread::FLAGS_bthread_concurrency = FLAGS_brpc_common_worker_num;

        DINGO_LOG(INFO) << "server.brpc_common_worker_num[" << FLAGS_brpc_common_worker_num
                        << "] is greater than server.worker_thread_num, bump up to ["
                        << bthread::FLAGS_bthread_concurrency << "]";
      }
    } else {
      if (FLAGS_read_worker_num + FLAGS_write_worker_num + FLAGS_raft_apply_worker_num + FLAGS_brpc_common_worker_num >
          bthread::FLAGS_bthread_concurrency) {
        bthread::FLAGS_bthread_concurrency =
            FLAGS_read_worker_num + FLAGS_write_worker_num + FLAGS_raft_apply_worker_num + FLAGS_brpc_common_worker_num;

        DINGO_LOG(INFO) << "server.read_worker_num[" << FLAGS_read_worker_num << "] + server.write_worker_num["
                        << FLAGS_write_worker_num << "] + server.raft_apply_worker_num[" << FLAGS_raft_apply_worker_num
                        << "] + server.brpc_common_worker_num[" << FLAGS_brpc_common_worker_num
                        << "] is greater than server.worker_thread_num, bump up to ["
                        << bthread::FLAGS_bthread_concurrency << "]";
      }
    }
  } else {
    // This is VectorIndex process, need to calc vector background worker num

    // init vector operation parallel thread num
    auto vector_operation_parallel_thread_num = config->GetInt("vector.operation_parallel_thread_num");
    if (vector_operation_parallel_thread_num <= 0) {
      vector_operation_parallel_thread_num = dingodb::FLAGS_vector_operation_parallel_thread_num;
      DINGO_LOG(WARNING) << fmt::format("[config] vector.vector_operation_parallel_thread_num is too small, set default value({})",
                                        dingodb::FLAGS_vector_operation_parallel_thread_num);
    }
    dingodb::FLAGS_vector_operation_parallel_thread_num = vector_operation_parallel_thread_num;
    DINGO_LOG(INFO) << "vector.vector_operation_parallel_thread_num is set to " << dingodb::FLAGS_vector_operation_parallel_thread_num;

    // init vector index manager background worker num
    auto vector_background_worker_num = config->GetInt("vector.background_worker_num");
    if (vector_background_worker_num <= 0) {
      vector_background_worker_num = dingodb::FLAGS_vector_background_worker_num;
      DINGO_LOG(WARNING) << fmt::format("[config] vector.background_worker_num is too small, set default value({})",
                                        dingodb::FLAGS_vector_background_worker_num);
    }
    dingodb::FLAGS_vector_background_worker_num = vector_background_worker_num;

    auto vector_fast_background_worker_num = config->GetInt("vector.fast_background_worker_num");
    if (vector_fast_background_worker_num <= 0) {
      vector_fast_background_worker_num = dingodb::FLAGS_vector_fast_background_worker_num;
      DINGO_LOG(WARNING) << fmt::format(
          "[config] vector.fast_background_worker_num is too small, set default value({})",
          dingodb::FLAGS_vector_fast_background_worker_num);
    }
    dingodb::FLAGS_vector_fast_background_worker_num = vector_fast_background_worker_num;

    if (FLAGS_use_pthread_prior_worker_set) {
      if (dingodb::FLAGS_vector_fast_background_worker_num + dingodb::FLAGS_vector_background_worker_num +
              FLAGS_brpc_common_worker_num >
          bthread::FLAGS_bthread_concurrency) {
        bthread::FLAGS_bthread_concurrency = dingodb::FLAGS_vector_fast_background_worker_num +
                                             dingodb::FLAGS_vector_background_worker_num + FLAGS_brpc_common_worker_num;

        DINGO_LOG(ERROR) << "vector.fast_background_worker_num[" << dingodb::FLAGS_vector_fast_background_worker_num
                         << "] + vector.background_worker_num[" << dingodb::FLAGS_vector_background_worker_num
                         << "] + server.brpc_common_worker_num[" << FLAGS_brpc_common_worker_num
                         << "] is greater than server.worker_thread_num, bump up to ["
                         << bthread::FLAGS_bthread_concurrency << "]";
      }
    } else {
      if (FLAGS_read_worker_num + FLAGS_write_worker_num + FLAGS_raft_apply_worker_num +
              dingodb::FLAGS_vector_fast_background_worker_num + dingodb::FLAGS_vector_background_worker_num +
              FLAGS_brpc_common_worker_num >
          bthread::FLAGS_bthread_concurrency) {
        bthread::FLAGS_bthread_concurrency = FLAGS_read_worker_num + FLAGS_write_worker_num +
                                             FLAGS_raft_apply_worker_num +
                                             dingodb::FLAGS_vector_fast_background_worker_num +
                                             dingodb::FLAGS_vector_background_worker_num + FLAGS_brpc_common_worker_num;

        DINGO_LOG(ERROR) << "server.read_worker_num[" << FLAGS_read_worker_num << "] + server.write_worker_num["
                         << FLAGS_write_worker_num << "] + server.raft_apply_worker_num[" << FLAGS_raft_apply_worker_num
                         << "] + vector.fast_background_worker_num[" << dingodb::FLAGS_vector_fast_background_worker_num
                         << "] + vector.background_worker_num[" << dingodb::FLAGS_vector_background_worker_num
                         << "] + server.brpc_common_worker_num[" << FLAGS_brpc_common_worker_num
                         << "] is greater than server.worker_thread_num, bump up to ["
                         << bthread::FLAGS_bthread_concurrency << "]";
      }
    }

    auto vector_max_background_task_count = config->GetInt("vector.max_background_task_count");
    if (vector_max_background_task_count <= 0) {
      vector_max_background_task_count = dingodb::FLAGS_vector_max_background_task_count;
      DINGO_LOG(WARNING) << fmt::format("[config] vector.max_background_task_count is too small, set default value({})",
                                        dingodb::FLAGS_vector_max_background_task_count);
    }
    dingodb::FLAGS_vector_max_background_task_count = vector_max_background_task_count;
  }

  return 0;
}

int InitCoordinatorServiceWorkerParameters(std::shared_ptr<dingodb::Config> config) {
  // coordinator num
  int coordinator_worker_num = config->GetInt("server.coordinator_service_worker_num");
  if (coordinator_worker_num <= 0) {
    DINGO_LOG(WARNING)
        << "server.coordinator_service_worker_num is not set, use dingodb::FLAGS_coordinator_service_worker_num";
  } else {
    FLAGS_coordinator_service_worker_num = coordinator_worker_num;
  }
  DINGO_LOG(INFO) << "server.coordinator_service_worker_num is set to " << FLAGS_coordinator_service_worker_num;

  auto coor_max_pending_num = config->GetInt64("server.coordinator_service_worker_max_pending_num");
  if (coor_max_pending_num <= 0) {
    DINGO_LOG(WARNING) << "server.coordinator_service_worker_max_pending_num is not set, use "
                          "dingodb::FLAGS_coordinator_service_worker_max_pending_num";
  } else {
    FLAGS_coordinator_service_worker_max_pending_num = coor_max_pending_num;
  }
  DINGO_LOG(INFO) << "server.coordinator_service_worker_max_pending_num is set to "
                  << FLAGS_coordinator_service_worker_max_pending_num;

  int meta_worker_num = config->GetInt("server.meta_service_worker_num");
  if (meta_worker_num <= 0) {
    DINGO_LOG(WARNING) << "server.meta_service_worker_num is not set, use dingodb::FLAGS_meta_service_worker_num";
  } else {
    FLAGS_meta_service_worker_num = meta_worker_num;
  }
  DINGO_LOG(INFO) << "server.meta_service_worker_num is set to " << FLAGS_meta_service_worker_num;

  auto meta_max_pending_num = config->GetInt64("server.meta_service_worker_max_pending_num");
  if (meta_max_pending_num <= 0) {
    DINGO_LOG(WARNING) << "server.meta_service_worker_max_pending_num is not set, use "
                          "dingodb::FLAGS_meta_service_worker_max_pending_num";
  } else {
    FLAGS_meta_service_worker_max_pending_num = meta_max_pending_num;
  }
  DINGO_LOG(INFO) << "server.meta_service_worker_max_pending_num is set to "
                  << FLAGS_meta_service_worker_max_pending_num;

  // version num
  int version_worker_num = config->GetInt("server.version_service_worker_num");
  if (version_worker_num <= 0) {
    DINGO_LOG(WARNING) << "server.version_service_worker_num is not set, use dingodb::FLAGS_version_service_worker_num";
  } else {
    FLAGS_version_service_worker_num = version_worker_num;
  }
  DINGO_LOG(INFO) << "server.version_service_worker_num is set to " << FLAGS_version_service_worker_num;

  auto version_max_pending_num = config->GetInt64("server.version_service_worker_max_pending_num");
  if (version_max_pending_num <= 0) {
    DINGO_LOG(WARNING) << "server.version_service_worker_max_pending_num is not set, use "
                          "dingodb::FLAGS_version_service_worker_max_pending_num";
  } else {
    FLAGS_version_service_worker_max_pending_num = version_max_pending_num;
  }
  DINGO_LOG(INFO) << "server.version_service_worker_max_pending_num is set to "
                  << FLAGS_version_service_worker_max_pending_num;

  if (FLAGS_use_pthread_prior_worker_set) {
    if (FLAGS_brpc_common_worker_num >= bthread::FLAGS_bthread_concurrency) {
      bthread::FLAGS_bthread_concurrency = FLAGS_brpc_common_worker_num;
      DINGO_LOG(INFO) << "service_worker_num[" << FLAGS_brpc_common_worker_num
                      << "] is greater than worker_thread_num, bump up to [" << bthread::FLAGS_bthread_concurrency
                      << "]";
    }
  } else {
    if (FLAGS_coordinator_service_worker_num + FLAGS_meta_service_worker_num + FLAGS_version_service_worker_num +
            FLAGS_brpc_common_worker_num >=
        bthread::FLAGS_bthread_concurrency) {
      bthread::FLAGS_bthread_concurrency = FLAGS_coordinator_service_worker_num + FLAGS_meta_service_worker_num +
                                           FLAGS_version_service_worker_num + FLAGS_brpc_common_worker_num;
      DINGO_LOG(INFO) << "service_worker_num["
                      << FLAGS_coordinator_service_worker_num + FLAGS_meta_service_worker_num +
                             FLAGS_version_service_worker_num + FLAGS_brpc_common_worker_num
                      << "] is greater than worker_thread_num, bump up to [" << bthread::FLAGS_bthread_concurrency
                      << "]";
    }
  }

  return 0;
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

  if (dingodb::FLAGS_show_version || dingodb::GetRoleName().empty()) {
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

  dingodb::pb::common::ClusterRole role = dingodb::GetRole();
  if (role == dingodb::pb::common::ClusterRole::ILLEGAL) {
    DINGO_LOG(ERROR) << "Invalid server role[" + dingodb::GetRoleName() + "]";
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

  auto &dingo_server = dingodb::Server::GetInstance();
  if (!dingo_server.InitConfig(FLAGS_conf)) {
    DINGO_LOG(ERROR) << "InitConfig failed!";
    return -1;
  }

  auto const config = dingodb::ConfigManager::GetInstance().GetRoleConfig();
  if (!dingo_server.InitDirectory()) {
    DINGO_LOG(ERROR) << "InitDirectory failed!";
    return -1;
  }

  if (!dingo_server.InitLog()) {
    DINGO_LOG(ERROR) << "InitLog failed!";
    return -1;
  }

  if (!dingo_server.InitServerID()) {
    DINGO_LOG(ERROR) << "InitServerID failed!";
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

#ifdef USE_OPENBLAS
  DINGO_LOG(INFO) << "USE_OPENBLAS is ON";
  openblas_set_num_threads(1);
#endif

  // check system env
  auto ret = dingodb::DoSystemCheck();
  if (ret < 0) {
    DINGO_LOG(ERROR) << "DoSystemCheck failed, DingoDB may run with unexpected behavior.";
  }
  DINGO_LOG(INFO) << "DoSystemCheck ret:" << ret;

  dingo_server.SetServerEndpoint(GetServerEndPoint(config));
  dingo_server.SetRaftEndpoint(GetRaftEndPoint(config));

  if (!dingo_server.InitEngine()) {
    DINGO_LOG(ERROR) << "InitEngine failed!";
    return -1;
  }
  if (!dingo_server.InitLogStorageManager()) {
    DINGO_LOG(ERROR) << "InitLogStorageManager failed!";
    return -1;
  }

  // for all role
  dingodb::NodeServiceImpl node_service;

  // for coordinator only
  dingodb::CoordinatorServiceImpl coordinator_service;
  dingodb::MetaServiceImpl meta_service;
  dingodb::VersionServiceProtoImpl version_service;
  dingodb::ClusterStatImpl cluster_stat_service;
  dingodb::TableImpl table_service;
  dingodb::RegionImpl region_service;
  dingodb::StoreMetricsImpl store_metrics_service;
  dingodb::TaskListImpl task_list_service;
  dingodb::StoreOperationImpl store_operation_service;

  // for store and index
  dingodb::DebugServiceImpl debug_service;
  dingodb::PushServiceImpl push_service;
  dingodb::FileServiceImpl file_service;

  // for store only
  dingodb::StoreServiceImpl store_service;

  // for index only
  dingodb::IndexServiceImpl index_service;
  dingodb::UtilServiceImpl util_service;

  brpc::Server brpc_server;
  brpc::Server raft_server;

  brpc::ServerOptions options;

  options.h2_settings.max_concurrent_streams = FLAGS_h2_server_max_concurrent_streams;
  options.h2_settings.stream_window_size = FLAGS_h2_server_stream_window_size;
  options.h2_settings.connection_window_size = FLAGS_h2_server_connection_window_size;
  options.h2_settings.max_frame_size = FLAGS_h2_server_max_frame_size;
  options.h2_settings.max_header_list_size = FLAGS_h2_server_max_header_list_size;
  // options.idle_timeout_sec = 30;

  DINGO_LOG(INFO) << "h2_settings.max_concurrent_streams: " << options.h2_settings.max_concurrent_streams;
  DINGO_LOG(INFO) << "h2_settings.stream_window_size: " << options.h2_settings.stream_window_size;
  DINGO_LOG(INFO) << "h2_settings.connection_window_size: " << options.h2_settings.connection_window_size;
  DINGO_LOG(INFO) << "h2_settings.max_frame_size: " << options.h2_settings.max_frame_size;
  DINGO_LOG(INFO) << "h2_settings.max_header_list_size: " << options.h2_settings.max_header_list_size;

  if (role == dingodb::pb::common::ClusterRole::COORDINATOR) {
    // setup bthread worker thread num into bthread::FLAGS_bthread_concurrency
    InitBthreadWorkerThreadNum(config);

    // init coordinator service worker parameters
    auto ret2 = InitCoordinatorServiceWorkerParameters(config);
    if (ret2 < 0) {
      DINGO_LOG(ERROR) << "InitCoordinatorServiceWorkerParameters failed!";
      return -1;
    }

    if (!dingo_server.InitCoordinatorInteractionForAutoIncrement()) {
      DINGO_LOG(ERROR) << "InitCoordinatorInteractionForAutoIncrement failed!";
      return -1;
    }

    options.num_threads = bthread::FLAGS_bthread_concurrency;

    DINGO_LOG(INFO) << "bthread worker_thread_num: " << bthread::FLAGS_bthread_concurrency;

    if (brpc_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service to brpc_server!";
      return -1;
    }

    if (raft_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service raft_server!";
      return -1;
    }

    dingo_server.SetEndpoints(GetEndpoints(config, "coordinator.peers"));

    coordinator_service.SetControl(dingo_server.GetCoordinatorControl());
    coordinator_service.SetKvControl(dingo_server.GetKvControl());
    coordinator_service.SetTsoControl(dingo_server.GetTsoControl());
    coordinator_service.SetAutoIncrementControl(dingo_server.GetAutoIncrementControl());
    meta_service.SetControl(dingo_server.GetCoordinatorControl());
    meta_service.SetAutoIncrementControl(dingo_server.GetAutoIncrementControl());
    meta_service.SetTsoControl(dingo_server.GetTsoControl());
    version_service.SetControl(dingo_server.GetKvControl());

    // the Engine should be init success
    auto engine = dingo_server.GetEngine();
    coordinator_service.SetKvEngine(engine);
    meta_service.SetKvEngine(engine);
    version_service.SetKvEngine(engine);

    // get service worker nums
    auto coordinator_service_worker_num = FLAGS_coordinator_service_worker_num;
    if (coordinator_service_worker_num < 0) {
      DINGO_LOG(ERROR) << "GetServiceWorkerNum failed!";
      return -1;
    }

    auto coordinator_service_worker_max_pending_num = FLAGS_coordinator_service_worker_max_pending_num;
    if (coordinator_service_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "GetCoordinatorServiceWorkerMaxPendingNum failed!";
      return -1;
    }

    dingodb::PriorWorkerSetPtr coordinator_worker_set = dingodb::PriorWorkerSet::New(
        "coor_wkr", FLAGS_coordinator_service_worker_num, FLAGS_coordinator_service_worker_max_pending_num,
        FLAGS_use_pthread_prior_worker_set);
    if (!coordinator_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init CoordinatorService PriorWorkerSet failed!";
      return -1;
    }

    auto meta_service_worker_num = FLAGS_meta_service_worker_num;
    if (meta_service_worker_num < 0) {
      DINGO_LOG(ERROR) << "GetServiceWorkerNum failed!";
      return -1;
    }

    auto meta_service_worker_max_pending_num = FLAGS_meta_service_worker_max_pending_num;
    if (meta_service_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "GetMetaServiceWorkerMaxPendingNum failed!";
      return -1;
    }

    dingodb::PriorWorkerSetPtr meta_worker_set =
        dingodb::PriorWorkerSet::New("meta_wkr", FLAGS_meta_service_worker_num,
                                     FLAGS_meta_service_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!meta_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init MetaService PriorWorkerSet failed!";
      return -1;
    }

    auto version_service_worker_num = FLAGS_version_service_worker_num;
    if (version_service_worker_num < 0) {
      DINGO_LOG(ERROR) << "GetServiceWorkerNum failed!";
      return -1;
    }

    auto version_service_worker_max_pending_num = FLAGS_version_service_worker_max_pending_num;
    if (version_service_worker_max_pending_num < 0) {
      DINGO_LOG(ERROR) << "GetVersionServiceWorkerMaxPendingNum failed!";
      return -1;
    }

    dingodb::PriorWorkerSetPtr version_worker_set =
        dingodb::PriorWorkerSet::New("version_wkr", FLAGS_version_service_worker_num,
                                     FLAGS_version_service_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!version_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init VersionService PriorWorkerSet failed!";
      return -1;
    }

    coordinator_service.SetWorkSet(coordinator_worker_set);
    meta_service.SetWorkSet(meta_worker_set);
    version_service.SetWorkSet(version_worker_set);

    // add service to brpc
    if (brpc_server.AddService(&coordinator_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add coordinator service!";
      return -1;
    }
    if (brpc_server.AddService(&meta_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add meta service!";
      return -1;
    }

    if (brpc_server.AddService(&version_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      LOG(ERROR) << "Fail to add node service to brpc_server!";
      return -1;
    }

    // Add Cluster Stat Service to get meta information from dingodb cluster
    cluster_stat_service.SetControl(dingo_server.GetCoordinatorControl(), dingo_server.GetKvControl(),
                                    dingo_server.GetTsoControl(), dingo_server.GetAutoIncrementControl());
    if (0 != brpc_server.AddService(&cluster_stat_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add cluster stat service";
      return -1;
    }
    if (0 != raft_server.AddService(&cluster_stat_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add cluster stat service";
      return -1;
    }
    table_service.SetControl(dingo_server.GetCoordinatorControl());
    if (0 != brpc_server.AddService(&table_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add table service";
      return -1;
    }
    if (0 != raft_server.AddService(&table_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add table service";
      return -1;
    }
    region_service.SetControl(dingo_server.GetCoordinatorControl());
    if (0 != brpc_server.AddService(&region_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add region service";
      return -1;
    }
    if (0 != raft_server.AddService(&region_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add region service";
      return -1;
    }
    store_metrics_service.SetControl(dingo_server.GetCoordinatorControl());
    if (0 != brpc_server.AddService(&store_metrics_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add store metrics service";
      return -1;
    }
    if (0 != raft_server.AddService(&store_metrics_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add store metrics service";
      return -1;
    }
    task_list_service.SetControl(dingo_server.GetCoordinatorControl());
    if (0 != brpc_server.AddService(&task_list_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add task list service";
      return -1;
    }
    if (0 != raft_server.AddService(&task_list_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add task list service";
      return -1;
    }
    store_operation_service.SetControl(dingo_server.GetCoordinatorControl());
    if (0 != brpc_server.AddService(&store_operation_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add store operation service";
      return -1;
    }
    if (0 != raft_server.AddService(&store_operation_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
      DINGO_LOG(ERROR) << "Fail to add store operation service";
      return -1;
    }

    if (braft::add_service(&raft_server, dingo_server.RaftEndpoint()) != 0) {
      DINGO_LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }

    if (raft_server.Start(dingo_server.RaftEndpoint(), &options) != 0) {
      DINGO_LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    DINGO_LOG(INFO) << "Raft server is running on " << raft_server.listen_address();

    // start coordinator/meta region
    butil::Status status = dingo_server.StartMetaRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Meta RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Meta region start";

    // start kv region
    status = dingo_server.StartKvRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Kv RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Kv region start";

    // start tso regioon
    status = dingo_server.StartTsoRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Tso RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Tso region start";

    // start auto-increment region
    status = dingo_server.StartAutoIncrementRegion(config, engine);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Init Auto Increment RaftNode and StateMachine Failed:" << status;
      return -1;
    }
    DINGO_LOG(INFO) << "Auto Increment region start";

  } else if (role == dingodb::pb::common::ClusterRole::STORE) {
    // setup bthread worker thread num into bthread::FLAGS_bthread_concurrency
    InitBthreadWorkerThreadNum(config);

    // init service workers
    auto ret1 = InitServiceWorkerParameters(config, role);
    if (ret1 < 0) {
      DINGO_LOG(ERROR) << "InitServiceWorkerParameters failed!";
      return -1;
    }

    options.num_threads = bthread::FLAGS_bthread_concurrency;

    DINGO_LOG(INFO) << "bthread worker_thread_num: " << bthread::FLAGS_bthread_concurrency;

    if (brpc_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service to brpc_server!";
      return -1;
    }

    if (raft_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service raft_server!";
      return -1;
    }

    dingodb::PriorWorkerSetPtr read_worker_set = dingodb::PriorWorkerSet::New(
        "read_wkr", FLAGS_read_worker_num, FLAGS_read_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!read_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init StoreServiceRead PriorWorkerSet failed!";
      return -1;
    }
    store_service.SetReadWorkSet(read_worker_set);
    dingo_server.SetStoreServiceReadWorkerSet(read_worker_set);

    dingodb::PriorWorkerSetPtr write_worker_set = dingodb::PriorWorkerSet::New(
        "write_wkr", FLAGS_write_worker_num, FLAGS_write_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!write_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init StoreServiceWrite PriorWorkerSet failed!";
      return -1;
    }
    store_service.SetWriteWorkSet(write_worker_set);
    dingo_server.SetStoreServiceWriteWorkerSet(write_worker_set);

    if (FLAGS_raft_apply_worker_num > 0) {
      dingodb::PriorWorkerSetPtr raft_apply_worker_set =
          dingodb::PriorWorkerSet::New("apply_wkr", FLAGS_raft_apply_worker_num, 0, FLAGS_use_pthread_prior_worker_set);
      if (!raft_apply_worker_set->Init()) {
        DINGO_LOG(ERROR) << "Init RaftApply PriorWorkerSet failed!";
        return -1;
      }
      store_service.SetRaftApplyWorkSet(raft_apply_worker_set);
      dingo_server.SetRaftApplyWorkerSet(raft_apply_worker_set);
      DINGO_LOG(INFO) << "RaftApply worker num: " << FLAGS_raft_apply_worker_num;
    } else {
      DINGO_LOG(INFO) << "RaftApply worker num: 0";
    }

    if (!dingo_server.InitCoordinatorInteraction()) {
      DINGO_LOG(ERROR) << "InitCoordinatorInteraction failed!";
      return -1;
    }
    if (!dingo_server.ValiateCoordinator()) {
      DINGO_LOG(ERROR) << "ValiateCoordinator failed!";
      return -1;
    }
    if (!dingo_server.InitStorage()) {
      DINGO_LOG(ERROR) << "InitStorage failed!";
      return -1;
    }
    if (!dingo_server.InitStoreMetaManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetaManager failed!";
      return -1;
    }
    if (!dingo_server.InitStoreMetricsManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetricsManager failed!";
      return -1;
    }
    if (!dingo_server.InitStoreController()) {
      DINGO_LOG(ERROR) << "InitStoreController failed!";
      return -1;
    }
    if (!dingo_server.InitRegionCommandManager()) {
      DINGO_LOG(ERROR) << "InitRegionCommandManager failed!";
      return -1;
    }
    if (!dingo_server.InitRegionController()) {
      DINGO_LOG(ERROR) << "InitRegionController failed!";
      return -1;
    }
    if (!dingo_server.InitPreSplitChecker()) {
      DINGO_LOG(ERROR) << "InitPreSplitChecker failed!";
      return -1;
    }

    store_service.SetStorage(dingo_server.GetStorage());
    if (brpc_server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add store service!";
      return -1;
    }

    if (brpc_server.AddService(&debug_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
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
    if (braft::add_service(&raft_server, dingo_server.RaftEndpoint()) != 0) {
      DINGO_LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }

    if (raft_server.Start(dingo_server.RaftEndpoint(), &options) != 0) {
      DINGO_LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    DINGO_LOG(INFO) << "Raft server is running on " << raft_server.listen_address();
  } else if (role == dingodb::pb::common::ClusterRole::INDEX) {
    // setup bthread worker thread num into bthread::FLAGS_bthread_concurrency
    InitBthreadWorkerThreadNum(config);

    // init service workers
    auto ret1 = InitServiceWorkerParameters(config, role);
    if (ret1 < 0) {
      DINGO_LOG(ERROR) << "InitServiceWorkerParameters failed!";
      return -1;
    }

    options.num_threads = bthread::FLAGS_bthread_concurrency;

    DINGO_LOG(INFO) << "bthread worker_thread_num: " << bthread::FLAGS_bthread_concurrency;

    if (brpc_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service to brpc_server!";
      return -1;
    }

    if (raft_server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add node service raft_server!";
      return -1;
    }

    dingodb::PriorWorkerSetPtr read_worker_set = dingodb::PriorWorkerSet::New(
        "read_wkr", FLAGS_read_worker_num, FLAGS_read_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!read_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init IndexServiceRead PriorWorkerSet failed!";
      return -1;
    }
    index_service.SetReadWorkSet(read_worker_set);
    util_service.SetReadWorkSet(read_worker_set);
    dingo_server.SetIndexServiceReadWorkerSet(read_worker_set);

    dingodb::PriorWorkerSetPtr write_worker_set = dingodb::PriorWorkerSet::New(
        "write_wkr", FLAGS_write_worker_num, FLAGS_write_worker_max_pending_num, FLAGS_use_pthread_prior_worker_set);
    if (!write_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Init IndexServiceWrite PriorWorkerSet failed!";
      return -1;
    }
    index_service.SetWriteWorkSet(write_worker_set);
    dingo_server.SetIndexServiceWriteWorkerSet(write_worker_set);

    if (FLAGS_raft_apply_worker_num > 0) {
      dingodb::PriorWorkerSetPtr raft_apply_worker_set =
          dingodb::PriorWorkerSet::New("apply_wkr", FLAGS_raft_apply_worker_num, 0, FLAGS_use_pthread_prior_worker_set);
      if (!raft_apply_worker_set->Init()) {
        DINGO_LOG(ERROR) << "Init RaftApply PriorWorkerSet failed!";
        return -1;
      }
      index_service.SetRaftApplyWorkSet(raft_apply_worker_set);
      dingo_server.SetRaftApplyWorkerSet(raft_apply_worker_set);
      DINGO_LOG(INFO) << "RaftApply worker num: " << FLAGS_raft_apply_worker_num;
    } else {
      DINGO_LOG(INFO) << "RaftApply worker num: 0";
    }

    if (!dingo_server.InitCoordinatorInteraction()) {
      DINGO_LOG(ERROR) << "InitCoordinatorInteraction failed!";
      return -1;
    }
    if (!dingo_server.ValiateCoordinator()) {
      DINGO_LOG(ERROR) << "ValiateCoordinator failed!";
      return -1;
    }
    if (!dingo_server.InitStorage()) {
      DINGO_LOG(ERROR) << "InitStorage failed!";
      return -1;
    }
    // region will do recover in InitStoreMetaManager, and if leader is elected, then it need vector index manager
    // workers to load index, so InitStoreMetaManager must be called before InitStoreMetaManager
    if (!dingo_server.InitVectorIndexManager()) {
      DINGO_LOG(ERROR) << "InitVectorIndexManager failed!";
      return -1;
    }
    index_service.SetVectorIndexManager(dingo_server.GetVectorIndexManager());
    if (!dingo_server.InitStoreMetaManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetaManager failed!";
      return -1;
    }
    if (!dingo_server.InitStoreMetricsManager()) {
      DINGO_LOG(ERROR) << "InitStoreMetricsManager failed!";
      return -1;
    }
    if (!dingo_server.InitStoreController()) {
      DINGO_LOG(ERROR) << "InitStoreController failed!";
      return -1;
    }
    if (!dingo_server.InitRegionCommandManager()) {
      DINGO_LOG(ERROR) << "InitRegionCommandManager failed!";
      return -1;
    }
    if (!dingo_server.InitRegionController()) {
      DINGO_LOG(ERROR) << "InitRegionController failed!";
      return -1;
    }
    if (!dingo_server.InitPreSplitChecker()) {
      DINGO_LOG(ERROR) << "InitPreSplitChecker failed!";
      return -1;
    }

    index_service.SetStorage(dingo_server.GetStorage());
    if (brpc_server.AddService(&index_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add index service!";
      return -1;
    }

    util_service.SetStorage(dingo_server.GetStorage());
    if (brpc_server.AddService(&util_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      DINGO_LOG(ERROR) << "Fail to add util service!";
      return -1;
    }

    if (brpc_server.AddService(&debug_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
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
    if (braft::add_service(&raft_server, dingo_server.RaftEndpoint()) != 0) {
      DINGO_LOG(ERROR) << "Fail to add raft service!";
      return -1;
    }

    if (raft_server.Start(dingo_server.RaftEndpoint(), &options) != 0) {
      DINGO_LOG(ERROR) << "Fail to start raft server!";
      return -1;
    }
    DINGO_LOG(INFO) << "Raft server is running on " << raft_server.listen_address();
  } else {
    DINGO_LOG(ERROR) << "Invalid server role[" + dingodb::GetRoleName() + "]";
    return -1;
  }

  if (!dingo_server.InitHeartbeat()) {
    DINGO_LOG(ERROR) << "InitHeartbeat failed!";
    return -1;
  }

  if (!dingo_server.Recover()) {
    DINGO_LOG(ERROR) << "Recover failed!";
    return -1;
  }

  if (!dingo_server.InitCrontabManager()) {
    DINGO_LOG(ERROR) << "InitCrontabManager failed!";
    return -1;
  }

  // Start server after raft server started.
  options.pid_file = dingo_server.PidFilePath();

  DINGO_LOG(INFO) << "Server is going to start on " << dingo_server.ServerEndpoint()
                  << ", pid_file:" << options.pid_file;

  if (brpc_server.Start(dingo_server.ServerEndpoint(), &options) != 0) {
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

  dingo_server.Destroy();

  return 0;
}
