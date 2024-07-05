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

#include "diskann/diskann_item_runtime.h"

#include <string>

#include "common/logging.h"
#include "fmt/core.h"

DEFINE_bool(use_pthread_diskann_import_worker_set, false, "use pthread diskann import worker set");
DEFINE_bool(use_pthread_diskann_build_worker_set, true, "use pthread diskann build worker set");
DEFINE_bool(use_pthread_diskann_load_worker_set, true, "use pthread diskann load worker set");
DEFINE_bool(use_pthread_diskann_search_worker_set, true, "use pthread diskann search worker set");
DEFINE_bool(use_pthread_diskann_misc_worker_set, true, "use pthread diskann misc worker set");
DEFINE_bool(use_prior_diskann_worker_set, true, "use prior diskann worker set");

DEFINE_int32(diskann_import_worker_num, 32, "the number of import worker used by diskann_service");
DEFINE_int32(diskann_import_worker_max_pending_num, 1024, "diskann_import_worker_max_pending_num");
DEFINE_int32(diskann_build_worker_num, 1, "the number of build worker used by diskann_service");
DEFINE_int32(diskann_build_worker_max_pending_num, 128, " 0 is unlimited");
DEFINE_int32(diskann_load_worker_num, 10, "the number of load worker used by diskann_service");
DEFINE_int32(diskann_load_worker_max_pending_num, 512, "0 is unlimited");
DEFINE_int32(diskann_search_worker_num, 1024, "the number of search worker used by diskann_service");
DEFINE_int32(diskann_search_worker_max_pending_num, 10240, " 0 is unlimited");
DEFINE_int32(diskann_misc_worker_num, 32, "the number of misc worker used by diskann_service");
DEFINE_int32(diskann_misc_worker_max_pending_num, 1024, " 0 is unlimited");

namespace dingodb {

template <typename FLAGS>
static bool ParseItem(std::shared_ptr<Config> config, const std::string& name, int& value, FLAGS& flags) {
  value = config->GetInt("server." + name);
  if (value <= 0) {
    DINGO_LOG(WARNING) << fmt::format("server.{} is not set, use dingodb::FLAGS_{}", name, name);
  } else {
    flags = value;
  }

  if (flags <= 0) {
    DINGO_LOG(ERROR) << fmt::format("server.{} is less than 0", name);
    return false;
  }
  DINGO_LOG(INFO) << fmt::format("server.FLAGS_{} is set to {}", name, flags);
  return true;
}

bool DiskANNItemRuntime::Init(std::shared_ptr<Config> config) {
  // init import worker set
  {
    int diskann_import_worker_num = 0;
    if (!ParseItem(config, "diskann_import_worker_num", diskann_import_worker_num, FLAGS_diskann_import_worker_num)) {
      return false;
    }

    int diskann_import_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_import_worker_max_pending_num", diskann_import_worker_max_pending_num,
                   FLAGS_diskann_import_worker_max_pending_num)) {
      return false;
    }

    import_worker_set = std::make_shared<SimpleWorkerSet>(
        "diskann_import", FLAGS_diskann_import_worker_num, FLAGS_diskann_import_worker_max_pending_num,
        FLAGS_use_pthread_diskann_import_worker_set, FLAGS_use_prior_diskann_worker_set);
    if (!import_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init import worker set";
      return false;
    }

    num_bthreads += (FLAGS_use_pthread_diskann_import_worker_set ? 0 : FLAGS_diskann_import_worker_num);
  }

  // init build worker set
  {
    int diskann_build_worker_num = 0;
    if (!ParseItem(config, "diskann_build_worker_num", diskann_build_worker_num, FLAGS_diskann_build_worker_num)) {
      return false;
    }

    int diskann_build_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_build_worker_max_pending_num", diskann_build_worker_max_pending_num,
                   FLAGS_diskann_build_worker_max_pending_num)) {
      return false;
    }

    build_worker_set = std::make_shared<SimpleWorkerSet>(
        "diskann_build", FLAGS_diskann_build_worker_num, FLAGS_diskann_build_worker_max_pending_num,
        FLAGS_use_pthread_diskann_build_worker_set, FLAGS_use_prior_diskann_worker_set);
    if (!build_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init build worker set";
      return false;
    }
    num_bthreads += (FLAGS_use_pthread_diskann_build_worker_set ? 0 : FLAGS_diskann_build_worker_num);
  }

  // init load worker set
  {
    int diskann_load_worker_num = 0;
    if (!ParseItem(config, "diskann_load_worker_num", diskann_load_worker_num, FLAGS_diskann_load_worker_num)) {
      return false;
    }

    int diskann_load_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_load_worker_max_pending_num", diskann_load_worker_max_pending_num,
                   FLAGS_diskann_load_worker_max_pending_num)) {
      return false;
    }

    load_worker_set = std::make_shared<SimpleWorkerSet>(
        "diskann_load", FLAGS_diskann_load_worker_num, FLAGS_diskann_load_worker_max_pending_num,
        FLAGS_use_pthread_diskann_load_worker_set, FLAGS_use_prior_diskann_worker_set);
    if (!load_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init load worker set";
      return false;
    }

    num_bthreads += (FLAGS_use_pthread_diskann_load_worker_set ? 0 : FLAGS_diskann_load_worker_num);
  }

  // init search worker Set
  {
    int diskann_search_worker_num = 0;
    if (!ParseItem(config, "diskann_search_worker_num", diskann_search_worker_num, FLAGS_diskann_search_worker_num)) {
      return false;
    }

    int diskann_search_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_search_worker_max_pending_num", diskann_search_worker_max_pending_num,
                   FLAGS_diskann_search_worker_max_pending_num)) {
      return false;
    }

    search_worker_set = std::make_shared<SimpleWorkerSet>(
        "diskann_search", FLAGS_diskann_search_worker_num, FLAGS_diskann_search_worker_max_pending_num,
        FLAGS_use_pthread_diskann_search_worker_set, FLAGS_use_prior_diskann_worker_set);
    if (!search_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init search worker set";
      return false;
    }
    num_bthreads += (FLAGS_use_pthread_diskann_search_worker_set ? 0 : FLAGS_diskann_search_worker_num);
  }

  // init misc worker set
  {
    int diskann_misc_worker_num = 0;
    if (!ParseItem(config, "diskann_misc_worker_num", diskann_misc_worker_num, FLAGS_diskann_misc_worker_num)) {
      return false;
    }

    int diskann_misc_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_misc_worker_max_pending_num", diskann_misc_worker_max_pending_num,
                   FLAGS_diskann_misc_worker_max_pending_num)) {
      return false;
    }

    misc_worker_set = std::make_shared<SimpleWorkerSet>(
        "diskann_misc", FLAGS_diskann_misc_worker_num, FLAGS_diskann_misc_worker_max_pending_num,
        FLAGS_use_pthread_diskann_misc_worker_set, FLAGS_use_prior_diskann_worker_set);
    if (!misc_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init misc worker set";
      return false;
    }

    num_bthreads += (FLAGS_use_pthread_diskann_misc_worker_set ? 0 : FLAGS_diskann_misc_worker_num);
  }

  DINGO_LOG(INFO) << "DiskANNItemRuntime initialized";
  return true;
}

SimpleWorkerSetPtr& DiskANNItemRuntime::GetImportWorkerSet() { return import_worker_set; }
SimpleWorkerSetPtr& DiskANNItemRuntime::GetBuildWorkerSet() { return build_worker_set; }
SimpleWorkerSetPtr& DiskANNItemRuntime::GetLoadWorkerSet() { return load_worker_set; }
SimpleWorkerSetPtr& DiskANNItemRuntime::GetSearchWorkerSet() { return search_worker_set; }
SimpleWorkerSetPtr& DiskANNItemRuntime::GetMiscWorkerSet() { return misc_worker_set; }
uint32_t DiskANNItemRuntime::GetNumBthreads() { return num_bthreads; }

}  // namespace dingodb
