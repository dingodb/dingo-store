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

#include <backtrace.h>
#include <cxxabi.h>
#include <dlfcn.h>
#include <libunwind.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "br/backup.h"
#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/parameter.h"
#include "br/restore.h"
#include "br/utils.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

const std::string kProgramName = "dingodb_br";

static void InitLog(const std::string& log_dir) {
  if (!dingodb::Helper::IsExistPath(log_dir)) {
    dingodb::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  google::InitGoogleLogging(kProgramName.c_str());
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, kProgramName).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

static butil::Status SetStoreInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_STORE);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get store map, status={}", br::Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "store_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> store_interaction = std::make_shared<br::ServerInteraction>();
  if (!store_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init store_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetStoreInteraction(store_interaction);
  return butil::Status();
}

static butil::Status SetIndexInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_INDEX);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get index map, status={}", br::Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "index_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> index_interaction = std::make_shared<br::ServerInteraction>();
  if (!index_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init index_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetIndexInteraction(index_interaction);
  return butil::Status();
}

static butil::Status SetDocumentInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_DOCUMENT);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get document map, status={}", br::Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "document_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> document_interaction = std::make_shared<br::ServerInteraction>();
  if (!document_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init document_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetDocumentInteraction(document_interaction);
  return butil::Status();
}

struct DingoStackTraceInfo {
  char* filename;
  int lineno;
  char* function;
  uintptr_t pc;
};

/* Passed to backtrace callback function.  */
struct DingoBacktraceData {
  struct DingoStackTraceInfo* all;
  size_t index;
  size_t max;
  int failed;
};

static int BacktraceCallback(void* vdata, uintptr_t pc, const char* filename, int lineno, const char* function) {
  struct DingoBacktraceData* data = (struct DingoBacktraceData*)vdata;
  struct DingoStackTraceInfo* p;

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

static void ErrorCallback(void* vdata, const char* msg, int errnum) {
  struct DingoBacktraceData* data = (struct DingoBacktraceData*)vdata;

  fprintf(stderr, "%s", msg);                                 // NOLINT
  if (errnum > 0) fprintf(stderr, ": %s", strerror(errnum));  // NOLINT
  fprintf(stderr, "\n");                                      // NOLINT
  data->failed = 1;
}

// The signal handler
#ifndef MAX_STACKTRACE_SIZE
#define MAX_STACKTRACE_SIZE 128
#endif
static void SignalHandler(int signo) {
  printf("========== handle signal '%d' ==========\n", signo);

  if (signo == SIGTERM) {
    // TODO: graceful shutdown
    // clean temp directory
    // dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance().GetCheckpointPath());
    // dingodb::Helper::RemoveFileOrDirectory(dingodb::Server::GetInstance().PidFilePath());
    // DINGO_LOG(WARNING) << "GRACEFUL SHUTDOWN, clean up checkpoint dir: "
    //                    << dingodb::Server::GetInstance().GetCheckpointPath()
    //                    << ", clean up pid_file: " << dingodb::Server::GetInstance().PidFilePath();
    _exit(0);
  }

  std::cerr << "Received signal " << signo << '\n';
  std::cerr << "Stack trace:" << '\n';
  DINGO_LOG(ERROR) << "Received signal " << signo;
  DINGO_LOG(ERROR) << "Stack trace:";

  struct backtrace_state* state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
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
    char* nameptr = all[x].function;
    char* demangled = abi::__cxa_demangle(all[x].function, nullptr, nullptr, &status);
    if (status == 0 && demangled) {
      nameptr = demangled;
    }

    Dl_info info = {};

    if (!dladdr((void*)all[x].pc, &info)) {
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
    // // clean temp directory
    // dingodb::Helper::RemoveAllFileOrDirectory(dingodb::Server::GetInstance().GetCheckpointPath());
    // dingodb::Helper::RemoveFileOrDirectory(dingodb::Server::GetInstance().PidFilePath());
    // DINGO_LOG(ERROR) << "GRACEFUL SHUTDOWN, clean up checkpoint dir: "
    //                  << dingodb::Server::GetInstance().GetCheckpointPath()
    //                  << ", clean up pid_file: " << dingodb::Server::GetInstance().PidFilePath();
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
      char* nameptr = symbol;
      // Demangle the symbol name
      int demangle_status;
      char* demangled = abi::__cxa_demangle(symbol, nullptr, nullptr, &demangle_status);
      if (demangled) {
        nameptr = demangled;
      }
      // std::cout << "  " << nameptr << " + " << offset << " (0x" << std::hex << pc << ")" << '\n';

      if (!dladdr((void*)pc, &info)) {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") ";
        std::string const error_msg = string_stream.str();
        DINGO_LOG(ERROR) << error_msg;
        std::cout << error_msg << '\n';
      } else {
        std::stringstream string_stream;
        string_stream << "Frame [" << i++ << "] symbol=[" << nameptr << " + " << offset << "] (0x" << std::hex << pc
                      << ") " << " fname=[" << info.dli_fname << "] saddr=[" << info.dli_saddr << "] fbase=["
                      << info.dli_fbase << "]";
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

int main(int argc, char* argv[]) {
  if (dingodb::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }

  google::ParseCommandLineFlags(&argc, &argv, false);

  SetupSignalHandler();

  if (dingodb::FLAGS_show_version || argc == 1) {
    dingodb::DingoShowVerion();
    printf(
        "Usage: --br_coor_url=[ip:port] --br_type=[backup]  --br_backup_type=[full] --backupts='[YYYY-MM-DD "
        "HH:MM:SS ]' --storage=local://[path_dir]\n");
    printf(
        "Usage: --br_coor_url=[file://./conf/coor_list] --br_type=[backup]  --br_backup_type=[full] "
        "--backupts='[YYYY-MM-DD "
        "HH:MM:SS ]' --storage=local://[path_dir]\n");

    printf(
        "Usage: --br_coor_url=[ip:port] --br_type=[restore]  --br_restore_type=[full]   "
        "--storage=local://[path_dir]\n");
    printf(
        "Usage: --br_coor_url=[file://./conf/coor_list] --br_type=[restore]  --br_restore_type=[full] "
        "--storage=local://[path_dir]\n");
    printf("Example: \n");
    printf(
        "./dingodb_br --br_coor_url=127.0.0.1:22001 --br_type=backup --br_backup_type=full --backupts='2020-01-01 "
        "00:00:00 +08:00' "
        "--storage=local:///opt/backup-2020-01-01\n");

    printf(
        "./dingodb_br --br_coor_url=file://./conf/coor_list --br_type=backup --br_backup_type=full "
        "--backupts='2020-01-01 "
        "00:00:00 +08:00' "
        "--storage=local:///opt/backup-2020-01-01\n");
    printf(
        "./dingodb_br --br_coor_url=127.0.0.1:22001 --br_type=restore --br_restore_type=full "
        "--storage=local:///opt/backup-2020-01-01\n");

    printf(
        "./dingodb_br --br_coor_url=file://./conf/coor_list --br_type=restore --br_restore_type=full "
        "--storage=local:///opt/backup-2020-01-01\n");
    exit(-1);
  }

  InitLog(br::FLAGS_br_log_dir);

  std::cout << "Number of command line arguments : " << argc << std::endl;
  DINGO_LOG(INFO) << "Number of command line arguments : " << argc;

  for (int i = 0; i < argc; i++) {
    std::cout << fmt::format("args[{}]=[{}]", i, argv[i]) << std::endl;
    DINGO_LOG(INFO) << fmt::format("args[{}]=[{}]", i, argv[i]);
  }

  std::cout << "Detail BR log in " << br::FLAGS_br_log_dir << std::endl;

  butil::Status status;
  std::shared_ptr<br::ServerInteraction> coordinator_interaction = std::make_shared<br::ServerInteraction>();
  if (br::FLAGS_br_coor_url.empty()) {
    DINGO_LOG(WARNING) << "coordinator url is empty, try to use file://.conf/coor_list";
    br::FLAGS_br_coor_url = "file://./conf/coor_list";

    std::string path = br::FLAGS_br_coor_url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = br::Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "coor_url not find addr, path=" << path;
      return -1;
    }
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --br_coor_url="
                       << br::FLAGS_br_coor_url;
      return -1;
    }
  } else {
    auto addrs = br::FLAGS_br_coor_url;
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --br_coor_url="
                       << br::FLAGS_br_coor_url;
      return -1;
    }
  }

  br::InteractionManager::GetInstance().SetCoordinatorInteraction(coordinator_interaction);

  status = SetStoreInteraction();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
    return -1;
  }

  status = SetIndexInteraction();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
    return -1;
  }

  status = SetDocumentInteraction();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
    return -1;
  }

  // command parse
  if (br::FLAGS_br_type == "backup") {
    if (br::FLAGS_br_backup_type == "full") {
    } else {
      DINGO_LOG(ERROR) << "backup type not support, please check parameter --br_backup_type="
                       << br::FLAGS_br_backup_type;
      return -1;
    }

    status = br::Utils::ConvertBackupTsToTso(br::FLAGS_backupts, br::FLAGS_backuptso_internal);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      return -1;
    }
  } else if (br::FLAGS_br_type == "restore") {
    if (br::FLAGS_br_restore_type == "full") {
    } else {
      DINGO_LOG(ERROR) << "restore type not support, please check parameter --br_restore_type="
                       << br::FLAGS_br_restore_type;
      return -1;
    }
  } else {
    DINGO_LOG(ERROR) << "br type not support, please check parameter --br_type=" << br::FLAGS_br_type;
    return -1;
  }

  if (br::FLAGS_storage.empty()) {
    DINGO_LOG(ERROR) << "storage is empty, please check parameter --storage=" << br::FLAGS_storage;
    return -1;
  }

  if (std::string(br::FLAGS_storage).find("local://") == 0) {
    std::string path = br::FLAGS_storage;
    path = path.replace(path.find("local://"), 8, "");
    if (!path.empty()) {
      if (path.back() == '/') {
        path.pop_back();
      }
    }

    if (path.empty()) {
      DINGO_LOG(ERROR) << "path is empty, please check parameter --storage=" << br::FLAGS_storage;
      return -1;
    }

    std::filesystem::path temp_path = path;
    if (temp_path.is_relative()) {
      DINGO_LOG(ERROR) << "storage not support relative path. use absolute path. " << br::FLAGS_storage;
      return -1;
    }

    br::FLAGS_storage_internal = path;
  } else {
    DINGO_LOG(ERROR) << "storage not support, please check parameter --storage=" << br::FLAGS_storage;
    return -1;
  }

  // backup
  if (br::FLAGS_br_type == "backup") {
    br::BackupParams params;
    params.coor_url = br::FLAGS_br_coor_url;
    params.br_type = br::FLAGS_br_type;
    params.br_backup_type = br::FLAGS_br_backup_type;
    params.backupts = br::FLAGS_backupts;
    params.backuptso_internal = br::FLAGS_backuptso_internal;
    params.storage = br::FLAGS_storage;
    params.storage_internal = br::FLAGS_storage_internal;

    std::cout << "Full Backup Parameter :" << std::endl;
    DINGO_LOG(INFO) << "Full Backup Parameter :";

    std::cout << "coordinator url    : "
              << br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "coordinator url    : "
                    << br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrsAsString();

    std::cout << "store       url    : "
              << br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "store       url    : "
                    << br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrsAsString();

    std::cout << "index       url    : "
              << br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "index       url    : "
                    << br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrsAsString();

    std::cout << "document    url    : "
              << br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "document    url    : "
                    << br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrsAsString();

    std::cout << "br type            : " << params.br_type << std::endl;
    DINGO_LOG(INFO) << "br type            : " << params.br_type;

    std::cout << "br backup type     : " << params.br_backup_type << std::endl;
    DINGO_LOG(INFO) << "br backup type     : " << params.br_backup_type;

    std::cout << "backupts           : " << params.backupts << std::endl;
    DINGO_LOG(INFO) << "backupts           : " << params.backupts;

    std::cout << "backuptso_internal : " << params.backuptso_internal << std::endl;
    DINGO_LOG(INFO) << "backuptso_internal : " << params.backuptso_internal;

    std::cout << "storage            : " << params.storage << std::endl;
    DINGO_LOG(INFO) << "storage            : " << params.storage;

    std::cout << "storage_internal   : " << params.storage_internal << std::endl;
    DINGO_LOG(INFO) << "storage_internal   : " << params.storage_internal;

    std::shared_ptr<br::Backup> backup = std::make_shared<br::Backup>(params);

    std::cout << std::endl;
    DINGO_LOG(INFO) << "";

    std::cout << "Full Backup" << std::endl;
    DINGO_LOG(INFO) << "Full Backup";

    status = backup->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Backup failed" << std::endl;
      DINGO_LOG(INFO) << "Backup failed";
      return -1;
    }
    status = backup->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Backup failed" << std::endl;
      DINGO_LOG(INFO) << "Backup failed";
      return -1;
    }

    status = backup->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Backup failed" << std::endl;
      DINGO_LOG(INFO) << "Backup failed";
      return -1;
    }

    DINGO_LOG(INFO) << "Backup finish";
  } else if (br::FLAGS_br_type == "restore") {
    br::RestoreParams params;
    params.coor_url = br::FLAGS_br_coor_url;
    params.br_type = br::FLAGS_br_type;
    params.br_restore_type = br::FLAGS_br_restore_type;
    params.storage = br::FLAGS_storage;
    params.storage_internal = br::FLAGS_storage_internal;

    std::cout << "Full Restore Parameter :" << std::endl;
    DINGO_LOG(INFO) << "Full Restore Parameter :";

    std::cout << "coordinator url    : "
              << br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "coordinator url    : "
                    << br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrsAsString();

    std::cout << "store       url    : "
              << br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "store       url    : "
                    << br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrsAsString();

    std::cout << "index       url    : "
              << br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "index       url    : "
                    << br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrsAsString();

    std::cout << "document    url    : "
              << br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrsAsString() << std::endl;
    DINGO_LOG(INFO) << "document    url    : "
                    << br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrsAsString();

    std::cout << "br type            : " << params.br_type << std::endl;
    DINGO_LOG(INFO) << "br type            : " << params.br_type;

    std::cout << "br restore type    : " << params.br_restore_type << std::endl;
    DINGO_LOG(INFO) << "br restore type    : " << params.br_restore_type;

    std::cout << "storage            : " << params.storage << std::endl;
    DINGO_LOG(INFO) << "storage            : " << params.storage;

    std::cout << "storage_internal   : " << params.storage_internal << std::endl;
    DINGO_LOG(INFO) << "storage_internal   : " << params.storage_internal;

    std::shared_ptr<br::Restore> restore = std::make_shared<br::Restore>(
        params, br::FLAGS_create_region_concurrency, br::FLAGS_restore_region_concurrency,
        br::FLAGS_create_region_timeout_s, br::FLAGS_restore_region_timeout_s, br::FLAGS_br_default_replica_num);

    std::cout << std::endl;
    DINGO_LOG(INFO) << "";

    std::cout << "Full Restore" << std::endl;
    DINGO_LOG(INFO) << "Full Restore";

    status = restore->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Restore failed" << std::endl;
      DINGO_LOG(INFO) << "Restore failed";
      return -1;
    }
    status = restore->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Restore failed" << std::endl;
      DINGO_LOG(INFO) << "Restore failed";
      return -1;
    }

    status = restore->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << br::Utils::FormatStatusError(status);
      std::cout << "Restore failed" << std::endl;
      DINGO_LOG(INFO) << "Restore failed";
      return -1;
    }

    DINGO_LOG(INFO) << "Restore finish";
  } else {
    DINGO_LOG(ERROR) << "br type not support, please check parameter --br_type=" << br::FLAGS_br_type;
    return -1;
  }

  return 0;
}
