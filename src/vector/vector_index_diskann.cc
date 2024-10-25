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

#include "vector/vector_index_diskann.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mvcc/codec.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/diskann.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "server/service_helper.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_bool(enable_vector_index_diskann, true, "enable vector index diskann. default is true");

DEFINE_bool(dingo_log_switch_diskann_detail, false, "dingo log switch diskann detail. default is false");

bvar::LatencyRecorder g_diskann_build_latency("dingo_diskann_build_latency");
bvar::LatencyRecorder g_diskann_search_latency("dingo_diskann_search_latency");
bvar::LatencyRecorder g_diskann_status_latency("dingo_diskann_status_latency");
bvar::LatencyRecorder g_diskann_reset_latency("dingo_diskann_reset_latency");
bvar::LatencyRecorder g_diskann_load_latency("dingo_diskann_load_latency");
bvar::LatencyRecorder g_diskann_drop_latency("dingo_diskann_drop_latency");
bvar::LatencyRecorder g_diskann_dump_latency("dingo_diskann_dump_latency");
bvar::LatencyRecorder g_diskann_count_latency("dingo_diskann_count_latency");

DEFINE_int32(diskann_server_build_worker_num, 64, "the number of build worker used by diskann_service");
DEFINE_int32(diskann_server_build_worker_max_pending_num, 256, " 0 is unlimited");
DEFINE_int32(diskann_server_load_worker_num, 128, "the number of load worker used by diskann_service");
DEFINE_int32(diskann_server_load_worker_max_pending_num, 1024, "0 is unlimited");

DEFINE_int32(diskann_server_import_batch_size, 32 * 1024 * 1024, "diskann server import batch size");

// outside not to modify
DEFINE_bool(diskann_build_sync_internal, false, "diskann build sync internal. default is false");

// outside not to modify
DEFINE_bool(diskann_load_sync_internal, false, "diskann load sync internal. default is false");

// outside not to modify
DEFINE_bool(diskann_reset_force_delete_file_internal, true,
            "diskann reset force delete file internal. default is true");

VectorIndexDiskANN::VectorIndexDiskANN(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                       const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                       ThreadPoolPtr thread_pool)
    : VectorIndex(id, vector_index_parameter, epoch, range, thread_pool),
      is_channel_init_(false),
      is_connected_(false) {
  metric_type_ = vector_index_parameter.diskann_parameter().metric_type();
  dimension_ = vector_index_parameter.diskann_parameter().dimension();
}

VectorIndexDiskANN::~VectorIndexDiskANN() = default;

void VectorIndexDiskANN::Init() {
  if (FLAGS_enable_vector_index_diskann) {
    InitDiskannServerAddr();
    InitWorkSet();
  }
}

void VectorIndexDiskANN::InitDiskannServerAddr() {
  auto const config = ConfigManager::GetInstance().GetRoleConfig();
  std::string host = config->GetStringOrNullIfNotExists("diskann.listen_host");
  if (host.empty()) {
    host = config->GetString("diskann.host");
    DINGO_LOG(INFO) << "diskann.listen_host is not set, use diskann.host: " << host;
  } else {
    DINGO_LOG(INFO) << "diskann.listen_host is set to: " << host;
  }
  const int port = config->GetInt("diskann.port");
  DINGO_LOG(INFO) << "diskann.port is set to: " << port;
  diskann_server_addr = Helper::EndPointToString(Helper::StringToEndPoint(host, port));
  DINGO_LOG(INFO) << "diskann server addr : " << diskann_server_addr;
}

template <typename FLAGS>
static bool ParseItem(std::shared_ptr<Config> config, const std::string& name, int& value, FLAGS& flags) {
  value = config->GetInt("diskann." + name);
  if (value <= 0) {
    DINGO_LOG(WARNING) << fmt::format("diskann.{} is not set, use dingodb::FLAGS_{}", name, name);
  } else {
    flags = value;
  }

  if (flags <= 0) {
    DINGO_LOG(ERROR) << fmt::format("diskann.{} is less than 0", name);
    return false;
  }
  DINGO_LOG(INFO) << fmt::format("diskann.FLAGS_{} is set to {}", name, flags);
  return true;
}

void VectorIndexDiskANN::InitWorkSet() {
  auto const config = ConfigManager::GetInstance().GetRoleConfig();
  // init build worker set
  {
    int diskann_server_build_worker_num = 0;
    if (!ParseItem(config, "diskann_server_build_worker_num", diskann_server_build_worker_num,
                   FLAGS_diskann_server_build_worker_num)) {
      return;
    }

    int diskann_server_build_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_server_build_worker_max_pending_num", diskann_server_build_worker_max_pending_num,
                   FLAGS_diskann_server_build_worker_max_pending_num)) {
      return;
    }

    diskann_server_build_worker_set =
        SimpleWorkerSet::New("diskann_build", FLAGS_diskann_server_build_worker_num,
                             FLAGS_diskann_server_build_worker_max_pending_num, false, false);
    if (!diskann_server_build_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init build worker set";
      return;
    }
  }

  // init load worker set
  {
    int diskann_server_load_worker_num = 0;
    if (!ParseItem(config, "diskann_server_load_worker_num", diskann_server_load_worker_num,
                   FLAGS_diskann_server_load_worker_num)) {
      return;
    }

    int diskann_server_load_worker_max_pending_num = 0;
    if (!ParseItem(config, "diskann_server_load_worker_max_pending_num", diskann_server_load_worker_max_pending_num,
                   FLAGS_diskann_server_load_worker_max_pending_num)) {
      return;
    }

    diskann_server_load_worker_set =
        SimpleWorkerSet::New("diskann_load", FLAGS_diskann_server_load_worker_num,
                             FLAGS_diskann_server_load_worker_max_pending_num, false, false);
    if (!diskann_server_load_worker_set->Init()) {
      DINGO_LOG(ERROR) << "Failed to init load worker set";
      return;
    }
  }
}

butil::Status VectorIndexDiskANN::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                     bool is_upsert) {
  return AddOrUpsert(vector_with_ids, is_upsert);
}

butil::Status VectorIndexDiskANN::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                              bool /*is_upsert*/) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexDiskANN::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexDiskANN::Delete(const std::vector<int64_t>& /*delete_ids*/) { return butil::Status::OK(); }

butil::Status VectorIndexDiskANN::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                         const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool,
                                         const pb::common::VectorSearchParameter& parameter,
                                         std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }

  if (topk <= 0) return butil::Status::OK();

  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  {
    BvarLatencyGuard bvar_guard(&g_diskann_search_latency);

    if (!filters.empty()) {
      return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Search with filter not support in DiskANN!!!");
    }

    butil::Status status;

    // search rpc
    status = SendVectorSearchRequestWrapper(vector_with_ids, topk, parameter, results);
    if (!status.ok() && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // create index rpc
    if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
      status = SendVectorNewRequestWrapper();
      if (!status.ok()) {
        std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                    pb::error::Errno_Name(status.error_code()), status.error_cstr());
        DINGO_LOG(ERROR) << s;
        return status;
      }
    } else {  // ok
      return butil::Status::OK();
    }

    // search again
    status = SendVectorSearchRequestWrapper(vector_with_ids, topk, parameter, results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::RangeSearch(
    const std::vector<pb::common::VectorWithId>& /*vector_with_ids*/, float /*radius*/,
    const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& /*filters*/, bool /*reconstruct*/,
    const pb::common::VectorSearchParameter& /*parameter*/,
    std::vector<pb::index::VectorWithDistanceResult>& /*results*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "RangeSearch not support in DiskANN!!!");
}

void VectorIndexDiskANN::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexDiskANN::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexDiskANN::SupportSave() { return false; }

butil::Status VectorIndexDiskANN::Build(const pb::common::Range& region_range, mvcc::ReaderPtr reader,
                                        const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                        pb::common::VectorStateParameter& vector_state_parameter) {
  butil::Status status;
  pb::error::Error last_error;
  vector_state_parameter.mutable_diskann()->set_state(pb::common::DiskANNCoreState::UNKNOWN);
  pb::common::DiskANNCoreState internal_state;

  BvarLatencyGuard bvar_guard(&g_diskann_build_latency);

  auto lambda_set_state_function = [this, &last_error, &internal_state, &vector_state_parameter]() {
    pb::common::DiskANNState diskann_state;
    TransToDiskANNStateFromDiskANNCoreState(internal_state, last_error.errcode(), diskann_state);
    vector_state_parameter.mutable_diskann()->set_state(internal_state);
    vector_state_parameter.mutable_diskann()->set_diskann_state(diskann_state);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);

  // rpc status
  status = SendVectorStatusRequestWrapper(internal_state, last_error);
  if (status.error_code() != pb::error::Errno::OK && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_str(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  // create index rpc
  if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
    status = SendVectorNewRequestWrapper();
    if (!status.ok()) {
      std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }

    // rpc status again
    internal_state = pb::common::DiskANNCoreState::UNKNOWN;
    last_error.Clear();
    status = SendVectorStatusRequestWrapper(internal_state, last_error);
    if (status.error_code() != pb::error::Errno::OK) {
      std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_str(),
                                  pb::common::DiskANNCoreState_Name(internal_state));
      DINGO_LOG(ERROR) << s;
      return status;
    }
  }

  // check last error
  if (last_error.errcode() != pb::error::OK) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(last_error.errcode()), last_error.errmsg(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    status = butil::Status(last_error.errcode(), s);
    return status;
  }

  if (pb::common::DiskANNCoreState::NODATA == internal_state) {
    status = butil::Status::OK();
    return status;
  }

  if (pb::common::DiskANNCoreState::IMPORTING == internal_state ||
      pb::common::DiskANNCoreState::IMPORTED == internal_state ||
      pb::common::DiskANNCoreState::BUILDING == internal_state ||
      pb::common::DiskANNCoreState::BUILDED == internal_state ||
      pb::common::DiskANNCoreState::UPDATINGPATH == internal_state) {
    status = butil::Status::OK();
    return status;
  }

  if (pb::common::DiskANNCoreState::UPDATEDPATH == internal_state ||
      pb::common::DiskANNCoreState::FAKEBUILDED == internal_state) {
    status = butil::Status::OK();
    return status;
  }

  if (pb::common::DiskANNCoreState::LOADING == internal_state) {
    status = butil::Status::OK();
    return status;
  }

  if (pb::common::DiskANNCoreState::LOADED == internal_state) {
    status = butil::Status::OK();
    return status;
  }

  if (!FLAGS_diskann_build_sync_internal) {
    internal_state = pb::common::DiskANNCoreState::BUILDING;
    auto task = std::make_shared<ServiceTask>([this, region_range, reader, parameter, ts]() {
      pb::common::DiskANNCoreState state;
      auto status = DoBuild(region_range, reader, parameter, ts, state);
      (void)status;
    });

    bool ret = diskann_server_build_worker_set->Execute(task);
    if (!ret) {
      std::string s = "Build worker set is full, please wait and retry";
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::EREQUEST_FULL, s);
      return status;
    }

  } else {
    status = DoBuild(region_range, reader, parameter, ts, internal_state);
    if (!status.ok()) {
      std::string s = fmt::format("Build failed, errcode: {}  errmsg: {}", pb::error::Errno_Name(status.error_code()),
                                  status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }
  }

  status = butil::Status::OK();
  return status;
}

butil::Status VectorIndexDiskANN::Load(const pb::common::VectorLoadParameter& parameter,
                                       pb::common::VectorStateParameter& vector_state_parameter) {
  butil::Status status;
  pb::error::Error last_error;
  vector_state_parameter.mutable_diskann()->set_state(pb::common::DiskANNCoreState::UNKNOWN);
  pb::common::DiskANNCoreState internal_state;
  pb::common::VectorLoadParameter internal_parameter = parameter;

  BvarLatencyGuard bvar_guard(&g_diskann_load_latency);

  auto lambda_set_state_function = [this, &last_error, &internal_state, &vector_state_parameter]() {
    pb::common::DiskANNState diskann_state;
    TransToDiskANNStateFromDiskANNCoreState(internal_state, last_error.errcode(), diskann_state);
    vector_state_parameter.mutable_diskann()->set_state(internal_state);
    vector_state_parameter.mutable_diskann()->set_diskann_state(diskann_state);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);

  // check rpc status
  status = SendVectorStatusRequestWrapper(internal_state, last_error);
  if (status.error_code() != pb::error::Errno::OK && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_str(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  // create index rpc
  if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
    status = SendVectorNewRequestWrapper();
    if (!status.ok()) {
      std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }

    // rpc status again
    internal_state = pb::common::DiskANNCoreState::UNKNOWN;
    last_error.Clear();
    status = SendVectorStatusRequestWrapper(internal_state, last_error);
    if (status.error_code() != pb::error::Errno::OK) {
      std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_str(),
                                  pb::common::DiskANNCoreState_Name(internal_state));
      DINGO_LOG(ERROR) << s;
      return status;
    }
  }

  // check last error
  if (last_error.errcode() != pb::error::OK) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(last_error.errcode()), last_error.errmsg(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return butil::Status(last_error.errcode(), s);
  }

  if (pb::common::DiskANNCoreState::NODATA == internal_state) {
    status = butil::Status(pb::error::EDISKANN_IS_NO_DATA, "DiskANN is no data");
    return status;
  }

  // direct_load_without_build depreciated. only inner use.
  internal_parameter.mutable_diskann()->set_direct_load_without_build(false);

  // check state . if state is FAKEBUILDED,  set direct.
  if (pb::common::DiskANNCoreState::FAKEBUILDED == internal_state) {
    internal_parameter.mutable_diskann()->set_direct_load_without_build(true);
  }

  if (internal_parameter.diskann().direct_load_without_build()) {
    if (pb::common::DiskANNCoreState::LOADED == internal_state) {
      status = butil::Status::OK();
      return status;
    }

    if (pb::common::DiskANNCoreState::LOADING == internal_state) {
      status = butil::Status::OK();
      return status;
    }

    if (pb::common::DiskANNCoreState::UNKNOWN != internal_state &&
        pb::common::DiskANNCoreState::FAKEBUILDED != internal_state) {
      std::string s =
          fmt::format("load direct state wrong state: {}, if you want to load direct, please call reset first",
                      pb::common::DiskANNCoreState_Name(internal_state));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EDISKANN_TRYLOAD_STATE_WRONG, s);
    }
  } else {
    if (pb::common::DiskANNCoreState::LOADED == internal_state) {
      status = butil::Status::OK();
      return status;
    }

    if (pb::common::DiskANNCoreState::LOADING == internal_state) {
      status = butil::Status::OK();
      return status;
    }

    if (pb::common::DiskANNCoreState::UPDATEDPATH != internal_state) {
      std::string s = fmt::format("load state wrong state: {}, maybe try again later or forget import or build",
                                  pb::common::DiskANNCoreState_Name(internal_state));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EDISKANN_LOAD_STATE_WRONG, s);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    internal_state = pb::common::DiskANNCoreState::LOADING;
    auto task = std::make_shared<ServiceTask>([this, internal_parameter]() {
      pb::common::DiskANNCoreState state;
      butil::Status status;
      // load index rpc
      if (internal_parameter.diskann().direct_load_without_build()) {
        status = SendVectorTryLoadRequestWrapper(internal_parameter, state);
      } else {
        status = SendVectorLoadRequestWrapper(internal_parameter, state);
      }
      if (!status.ok()) {
        LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] " << status.error_cstr();
        return;
      }
    });

    bool ret = diskann_server_load_worker_set->Execute(task);
    if (!ret) {
      std::string s = "Load worker set is full, please wait and retry";
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EREQUEST_FULL, s);
    }
  } else {
    if (internal_parameter.diskann().direct_load_without_build()) {
      status = SendVectorTryLoadRequestWrapper(internal_parameter, internal_state);
    } else {
      status = SendVectorLoadRequestWrapper(internal_parameter, internal_state);
    }

    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  status = butil::Status::OK();
  return status;
}

butil::Status VectorIndexDiskANN::Status(pb::common::VectorStateParameter& vector_state_parameter,
                                         pb::error::Error& internal_error) {
  butil::Status status;
  pb::error::Error last_error;
  vector_state_parameter.mutable_diskann()->set_state(pb::common::DiskANNCoreState::UNKNOWN);
  pb::common::DiskANNCoreState internal_state;

  BvarLatencyGuard bvar_guard(&g_diskann_status_latency);

  auto lambda_set_state_function = [this, &last_error, &internal_state, &vector_state_parameter, &internal_error]() {
    pb::common::DiskANNState diskann_state;
    TransToDiskANNStateFromDiskANNCoreState(internal_state, last_error.errcode(), diskann_state);
    vector_state_parameter.mutable_diskann()->set_state(internal_state);
    vector_state_parameter.mutable_diskann()->set_diskann_state(diskann_state);
    internal_error = last_error;
  };

  ON_SCOPE_EXIT(lambda_set_state_function);
  // rpc status
  status = SendVectorStatusRequestWrapper(internal_state, last_error);

  // check error
  if (status.error_code() != pb::error::Errno::OK && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (status.error_code() == pb::error::Errno::OK) {
    return status;
  }

  // create index rpc
  if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
    status = SendVectorNewRequestWrapper();
    if (!status.ok()) {
      std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }
  }

  // rpc status again
  status = SendVectorStatusRequestWrapper(internal_state, last_error);
  // check error
  if (status.error_code() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  return status;
}

butil::Status VectorIndexDiskANN::Reset(bool delete_data_file,
                                        pb::common::VectorStateParameter& vector_state_parameter) {
  butil::Status status;
  pb::error::Error last_error;
  vector_state_parameter.mutable_diskann()->set_state(pb::common::DiskANNCoreState::UNKNOWN);
  pb::common::DiskANNCoreState internal_state;

  BvarLatencyGuard bvar_guard(&g_diskann_reset_latency);

  auto lambda_set_state_function = [this, &last_error, &internal_state, &vector_state_parameter]() {
    pb::common::DiskANNState diskann_state;
    TransToDiskANNStateFromDiskANNCoreState(internal_state, last_error.errcode(), diskann_state);
    vector_state_parameter.mutable_diskann()->set_state(internal_state);
    vector_state_parameter.mutable_diskann()->set_diskann_state(diskann_state);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);

  // delete_data_file = false is deprecated, always delete data file
  if (FLAGS_diskann_reset_force_delete_file_internal) {
    delete_data_file = true;
  }

  // rpc reset
  status = SendVectorResetRequestWrapper(delete_data_file, internal_state);
  if (status.error_code() != pb::error::Errno::OK && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
    std::string s = fmt::format("VectorReset response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  // create index rpc
  if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
    status = SendVectorNewRequestWrapper();
    if (!status.ok()) {
      std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }
  } else {
    status = butil::Status::OK();
    return status;
  }

  // rpc reset again
  status = SendVectorResetRequestWrapper(delete_data_file, internal_state);
  if (status.error_code() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorReset response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr(),
                                pb::common::DiskANNCoreState_Name(internal_state));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  status = butil::Status::OK();
  return status;
}

butil::Status VectorIndexDiskANN::Drop() {
  butil::Status status;

  BvarLatencyGuard bvar_guard(&g_diskann_drop_latency);
  // rpc destroy
  status = SendVectorDestroyRequestWrapper();
  if (status.error_code() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDestroy response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    DINGO_LOG(ERROR) << "drop index : " << Id() << " failed";
    return status;
  }

  DINGO_LOG(INFO) << "drop index : " << Id() << " success";

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::Dump(bool dump_all, std::vector<std::string>& dump_datas) {
  BvarLatencyGuard bvar_guard(&g_diskann_dump_latency);
  if (dump_all) {
    return DoDumpAll(dump_datas);
  } else {
    return DoDump(dump_datas);
  }
}

butil::Status VectorIndexDiskANN::Save(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "not support in DiskANN!!!");
}

butil::Status VectorIndexDiskANN::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "not support in DiskANN!!!");
}

int32_t VectorIndexDiskANN::GetDimension() { return static_cast<int32_t>(this->dimension_); }

pb::common::MetricType VectorIndexDiskANN::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexDiskANN::GetCount(int64_t& count) {
  int64_t internal_count = 0;
  butil::Status status;

  BvarLatencyGuard bvar_guard(&g_diskann_count_latency);
  status = SendVectorCountRequestWrapper(internal_count);
  if (!status.ok()) {
    std::string s = fmt::format("VectorCount request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  count = internal_count;

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::GetMemorySize(int64_t& memory_size) {
  memory_size = 0;
  return butil::Status::OK();
}

bool VectorIndexDiskANN::IsExceedsMaxElements() { return false; }

bool VectorIndexDiskANN::NeedToSave(int64_t /*last_save_log_behind*/) { return false; }

butil::Status VectorIndexDiskANN::DoBuild(const pb::common::Range& region_range, mvcc::ReaderPtr reader,
                                          const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                          pb::common::DiskANNCoreState& state) {
  butil::Status status;
  pb::error::Error last_error;
  state = pb::common::DiskANNCoreState::UNKNOWN;
  pb::common::DiskANNCoreState internal_state;
  pb::common::Range encode_range = mvcc::Codec::EncodeRange(region_range);
  IteratorOptions options;
  options.upper_bound = encode_range.end_key();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_diskann_detail) << fmt::format("Enter DoBuild");

  int64_t region_count = 0;
  status = reader->KvCount(Constant::kVectorDataCF, ts, region_range.start_key(), region_range.end_key(), region_count);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_diskann_detail) << fmt::format("After KvCount");

  // count too small, set no data to diskann.
  if (region_count < Constant::kDiskannMinCount) {
    std::string s = fmt::format("region : {} vector current count {} is less than min count {}. set no data to diskann",
                                Id(), region_count, Constant::kDiskannMinCount);
    DINGO_LOG(WARNING) << s;
    status = SendVectorSetNoDataRequestWrapper();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    state = pb::common::DiskANNCoreState::NODATA;
    return butil::Status::OK();
  }

  auto iter = reader->NewIterator(Constant::kVectorDataCF, ts, options);

  if (!iter) {
    std::string s =
        fmt::format("NewIterator failed start : {} end : {} ts : {}", Helper::StringToHex(region_range.start_key()),
                    Helper::StringToHex(region_range.end_key()), ts);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, "NewIterator failed");
  }

  iter->Seek(encode_range.start_key());
  if (!iter->Valid()) {
    std::string s =
        fmt::format("NewIterator invalid maybe empty. start : {} end : {} ts : {}",
                    Helper::StringToHex(region_range.start_key()), Helper::StringToHex(region_range.end_key()), ts);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_COUNT_TOO_FEW, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_diskann_detail) << fmt::format("After seek");

  pb::diskann::VectorPushDataRequest vector_push_data_request;

  int64_t count = 0;
  int64_t last_count = 0;
  int64_t tso =
#if defined(TEST_VECTOR_INDEX_DISKANN_MOCK)
      llabs(static_cast<int64_t>(butil::fast_rand()));
#else
      Server::GetInstance().GetTsProvider()->GetTs();
#endif

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_diskann_detail) << fmt::format("Enter while iter");
  // scan data from raw engine
  while (iter->Valid()) {
    std::string key(iter->Key());
    int64_t vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
    CHECK(vector_id > 0) << fmt::format("vector_id({}) is invalid", vector_id);

    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));

    pb::common::Vector vector;
    CHECK(vector.ParseFromString(value)) << "Parse vector proto error";
    // std::cout << "key : " << Helper::StringToHex(key) << ", vector_id : " << vector_id << std::endl;
    vector_push_data_request.add_vectors()->Swap(&vector);
    vector_push_data_request.add_vector_ids(vector_id);
    count++;

    // A rough estimate of the number of bytes transferred. Never use ByteSizeLong.
    auto size = vector_push_data_request.vector_ids_size() *
                (sizeof(int32_t) + vector_push_data_request.vectors(0).dimension() * sizeof(float));
    if (size >= FLAGS_diskann_server_import_batch_size) {
      // Deprecate protocol ByteSizeLong api. When the amount of data is large, the calculation is too slow. It will
      // cause the next import data to time out. The timeout is 30 seconds.
      // if (vector_push_data_request.ByteSizeLong() >= FLAGS_diskann_server_import_batch_size) {
      vector_push_data_request.set_vector_index_id(Id());
      vector_push_data_request.set_has_more(true);  // true has data
      vector_push_data_request.set_error(::dingodb::pb::error::Errno::OK);
      vector_push_data_request.set_force_to_load_data_if_exist(false);
      vector_push_data_request.set_already_send_vector_count(last_count);
      vector_push_data_request.set_ts(ts);
      vector_push_data_request.set_tso(tso);
      status = SendVectorPushDataRequestWrapper(vector_push_data_request);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
      vector_push_data_request.Clear();
      last_count = count;
    }

    iter->Next();
  }

  vector_push_data_request.set_vector_index_id(Id());
  vector_push_data_request.set_has_more(false);  // false
  vector_push_data_request.set_error(::dingodb::pb::error::Errno::OK);
  vector_push_data_request.set_force_to_load_data_if_exist(false);
  vector_push_data_request.set_already_send_vector_count(last_count);
  vector_push_data_request.set_ts(ts);
  vector_push_data_request.set_tso(tso);
  status = SendVectorPushDataRequestWrapper(vector_push_data_request);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // build
  status = VectorIndexDiskANN::SendVectorBuildRequestWrapper(parameter, internal_state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  state = internal_state;

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::DoDump(std::vector<std::string>& dump_datas) {
  butil::Status status;

  // rpc dump
  status = SendVectorDumpRequestWrapper(dump_datas);
  if (status.error_code() != pb::error::Errno::OK && status.error_code() != pb::error::Errno::EINDEX_NOT_FOUND) {
    std::string s = fmt::format("VectorDump response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  // create index rpc
  if (status.error_code() == pb::error::Errno::EINDEX_NOT_FOUND) {
    status = SendVectorNewRequestWrapper();
    if (!status.ok()) {
      std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                  pb::error::Errno_Name(status.error_code()), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }
  } else {
    return butil::Status::OK();
  }

  // rpc dump again
  status = SendVectorDumpRequestWrapper(dump_datas);
  if (status.error_code() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDump response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::DoDumpAll(std::vector<std::string>& dump_datas) {
  butil::Status status;

  // rpc dump all
  status = SendVectorDumpAllRequestWrapper(dump_datas);
  if (status.error_code() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDumpAll response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }
  return butil::Status::OK();
}

bool VectorIndexDiskANN::InitChannel(const std::string& addr) {
  {
    RWLockReadGuard guard(&rw_lock_);
    if (is_channel_init_ && is_connected_) {
      return true;
    }
  }
  {
    RWLockWriteGuard guard(&rw_lock_);
    if (is_channel_init_ && is_connected_) {
      return true;
    }

    std::vector<butil::EndPoint> endpoints;
    endpoints = Helper::StringToEndpoints({addr});
    if (endpoints.empty()) {
      DINGO_LOG(ERROR) << "Parse addr failed";
      return false;
    }

    auto& endpoint = endpoints[0];

    DINGO_LOG(INFO) << fmt::format("Init channel {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    channel_ = std::make_unique<brpc::Channel>();
    options_ = std::make_unique<brpc::ChannelOptions>();
    options_->timeout_ms = 0x7fffffff;
    options_->connect_timeout_ms = 5000;  // 5s
    options_->max_retry = 3;
    if (channel_->Init(endpoint, options_.get()) != 0) {
      DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
      return false;
    }

    is_channel_init_ = true;
    is_connected_ = true;
    return true;
  }
}

butil::Status VectorIndexDiskANN::SendRequest(const std::string& service_name, const std::string& api_name,
                                              const google::protobuf::Message& request,
                                              google::protobuf::Message& response) {
  bool is_error_occur = false;
  butil::Status status;
  {
    RWLockReadGuard guard(&rw_lock_);
    if (!is_channel_init_ || !is_connected_) {
      return butil::Status(pb::error::EDISKANN_NET_DISCONNECTED, "diskann  network is not connected : %s",
                           diskann_server_addr.c_str());
    }

    const google::protobuf::MethodDescriptor* method = nullptr;

    if (service_name == "DiskAnnService") {
      method = dingodb::pb::diskann::DiskAnnService::descriptor()->FindMethodByName(api_name);
    } else {
      DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
    }

    if (method == nullptr) {
      DINGO_LOG(FATAL) << "Unknown api name: " << api_name;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(0x7fffffff);
    cntl.set_log_id(butil::fast_rand());

    channel_->CallMethod(method, &cntl, &request, &response, nullptr);

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_diskann_detail)
        << fmt::format("send request api {}  response: {} request: {}", api_name,
                       response.ShortDebugString().substr(0, 1024), request.ShortDebugString().substr(0, 1024));

    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                      cntl.ErrorText());
      if (cntl.ErrorCode() == 112) {
        // ECONNRESET
        // continue;
      }
      // cntl.latency_us();
      // channel_.reset();
      // options_.reset();
      // is_channel_init_ = false;
      // is_connected_ = false;
      is_error_occur = true;
      status = butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
  }

  if (is_error_occur) {
    RWLockWriteGuard guard(&rw_lock_);
    if (is_channel_init_) {
      channel_.reset();
      options_.reset();
      is_channel_init_ = false;
      is_connected_ = false;
    }
  }

  return status;
}

butil::Status VectorIndexDiskANN::SendVectorNewRequest(const google::protobuf::Message& request,
                                                       google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // rpc status
  status = SendRequest("DiskAnnService", "VectorNew", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorNewRequestWrapper() {
  butil::Status status;
  pb::diskann::VectorNewRequest vector_new_request;
  pb::diskann::VectorNewResponse vector_new_response;

  vector_new_request.set_vector_index_id(Id());
  vector_new_request.mutable_vector_index_parameter()->CopyFrom(VectorIndexParameter());

  status = SendVectorNewRequest(vector_new_request, vector_new_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_new_response.error().errcode() != pb::error::Errno::OK &&
      vector_new_response.error().errcode() != pb::error::EINDEX_EXISTS) {
    std::string s = fmt::format("VectorNew request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_new_response.error().errcode(), s);
  }
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorStatusRequest(const google::protobuf::Message& request,
                                                          google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // rpc status
  status = SendRequest("DiskAnnService", "VectorStatus", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorStatus request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorStatusRequestWrapper(pb::common::DiskANNCoreState& state,
                                                                 pb::error::Error& last_error) {
  butil::Status status;
  pb::diskann::VectorStatusRequest vector_status_request;
  vector_status_request.set_vector_index_id(Id());
  pb::diskann::VectorStatusResponse vector_status_response;

  status = SendVectorStatusRequest(vector_status_request, vector_status_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorStatus request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_status_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorStatus response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_status_response.error().errcode()),
                                vector_status_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_status_response.state()));  // state
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_status_response.error().errcode(), s);
  }

  status = butil::Status(vector_status_response.error().errcode(), vector_status_response.error().errmsg());
  last_error = vector_status_response.last_error();
  state = vector_status_response.state();

  return status;
}

butil::Status VectorIndexDiskANN::SendVectorCountRequest(const google::protobuf::Message& request,
                                                         google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // count rpc
  status = SendRequest("DiskAnnService", "VectorCount", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorCount request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorCountRequestWrapper(int64_t& count) {
  butil::Status status;

  // count rpc
  pb::diskann::VectorCountRequest vector_count_request;
  pb::diskann::VectorCountResponse vector_count_response;

  vector_count_request.set_vector_index_id(Id());
  status = SendVectorCountRequest(vector_count_request, vector_count_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorCount request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_count_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorCount response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_count_response.error().errcode()),
                                vector_count_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_count_response.state()));  // state
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_count_response.error().errcode(), s);
  }

  count = vector_count_response.count();

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorSetNoDataRequest(const google::protobuf::Message& request,
                                                             google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // count rpc
  status = SendRequest("DiskAnnService", "VectorSetNoData", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorSetNoData request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorSetNoDataRequestWrapper() {
  butil::Status status;

  // count rpc
  pb::diskann::VectorSetNoDataRequest vector_set_no_data_request;
  pb::diskann::VectorSetNoDataResponse vector_set_no_data_response;

  vector_set_no_data_request.set_vector_index_id(Id());
  status = SendVectorSetNoDataRequest(vector_set_no_data_request, vector_set_no_data_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorSetNoData request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_set_no_data_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorSetNoData response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_set_no_data_response.error().errcode()),
                                vector_set_no_data_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_set_no_data_response.state()));  // state
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_set_no_data_response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorBuildRequest(const google::protobuf::Message& request,
                                                         google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorBuild", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorBuild request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorBuildRequestWrapper(const pb::common::VectorBuildParameter& parameter,
                                                                pb::common::DiskANNCoreState& state) {
  butil::Status status;
  pb::diskann::VectorBuildRequest vector_build_request;
  pb::diskann::VectorBuildResponse vector_build_response;
  state = pb::common::DiskANNCoreState::UNKNOWN;

  vector_build_request.set_vector_index_id(Id());
  if (parameter.has_diskann()) {
    vector_build_request.set_force_to_build_if_exist(parameter.diskann().force_to_build_if_exist());
  }

  status = SendVectorBuildRequest(vector_build_request, vector_build_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorBuild request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  if (vector_build_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorBuild response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_build_response.error().errcode()),
                                vector_build_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_build_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_build_response.error().errcode(), s);
  }

  state = vector_build_response.state();

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorLoadRequest(const google::protobuf::Message& request,
                                                        google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorLoad", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorLoad request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorLoadRequestWrapper(const pb::common::VectorLoadParameter& parameter,
                                                               pb::common::DiskANNCoreState& state) {
  butil::Status status;
  pb::diskann::VectorLoadRequest vector_load_request;
  pb::diskann::VectorLoadResponse vector_load_response;
  state = pb::common::DiskANNCoreState::UNKNOWN;

  vector_load_request.set_vector_index_id(Id());
  if (parameter.has_diskann()) {
    vector_load_request.mutable_load_param()->CopyFrom(parameter.diskann());
  }
  status = SendVectorLoadRequest(vector_load_request, vector_load_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorLoad request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_load_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorLoad response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_load_response.error().errcode()),
                                vector_load_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_load_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_load_response.error().errcode(), s);
  }

  state = vector_load_response.state();
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorTryLoadRequest(const google::protobuf::Message& request,
                                                           google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorTryLoad", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorTryLoad request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}
butil::Status VectorIndexDiskANN::SendVectorTryLoadRequestWrapper(const pb::common::VectorLoadParameter& parameter,
                                                                  pb::common::DiskANNCoreState& state) {
  butil::Status status;
  pb::diskann::VectorTryLoadRequest vector_try_load_request;
  pb::diskann::VectorTryLoadResponse vector_try_load_response;
  state = pb::common::DiskANNCoreState::UNKNOWN;

  vector_try_load_request.set_vector_index_id(Id());
  if (parameter.has_diskann()) {
    vector_try_load_request.mutable_load_param()->CopyFrom(parameter.diskann());
  }
  status = SendVectorTryLoadRequest(vector_try_load_request, vector_try_load_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorTryLoad request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_try_load_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorTryLoad response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_try_load_response.error().errcode()),
                                vector_try_load_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_try_load_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_try_load_response.error().errcode(), s);
  }

  state = vector_try_load_response.state();
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorPushDataRequest(const google::protobuf::Message& request,
                                                            google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorPushData", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorPushData request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}
butil::Status VectorIndexDiskANN::SendVectorPushDataRequestWrapper(
    const pb::diskann::VectorPushDataRequest& vector_push_data_request) {
  butil::Status status;
  pb::diskann::VectorPushDataResponse vector_push_data_response;
  status = SendVectorPushDataRequest(vector_push_data_request, vector_push_data_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorPushData request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_push_data_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorPushData response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_push_data_response.error().errcode()),
                                vector_push_data_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_push_data_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_push_data_response.error().errcode(), s);
  }
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorSearchRequest(const google::protobuf::Message& request,
                                                          google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorSearch", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorSearch request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorSearchRequestWrapper(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
    const pb::common::VectorSearchParameter& parameter, std::vector<pb::index::VectorWithDistanceResult>& results) {
  butil::Status status;
  pb::diskann::VectorSearchRequest vector_search_request;
  pb::diskann::VectorSearchResponse vector_search_response;

  vector_search_request.set_vector_index_id(Id());
  vector_search_request.set_top_n(topk);
  for (const auto& vector_with_id : vector_with_ids) {
    vector_search_request.add_vectors()->CopyFrom(vector_with_id.vector());
  }
  if (parameter.has_diskann()) {
    vector_search_request.mutable_search_param()->CopyFrom(parameter.diskann());
  }

  status = SendVectorSearchRequest(vector_search_request, vector_search_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorSearch request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  // check error
  if (vector_search_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorSearch response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_search_response.error().errcode()),
                                vector_search_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_search_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_search_response.error().errcode(), s);
  }

  if (vector_search_response.error().errcode() == pb::error::Errno::OK) {
    results.clear();
    results.reserve(vector_search_response.batch_results_size());
    for (int i = 0; i < vector_search_response.batch_results_size(); i++) {
      results.emplace_back().Swap(vector_search_response.mutable_batch_results(i));
    }
  }
  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorResetRequest(const google::protobuf::Message& request,
                                                         google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorReset", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorReset request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorResetRequestWrapper(bool delete_data_file,
                                                                pb::common::DiskANNCoreState& state) {
  butil::Status status;
  pb::diskann::VectorResetRequest vector_reset_request;
  pb::diskann::VectorResetResponse vector_reset_response;
  state = pb::common::DiskANNCoreState::UNKNOWN;

  vector_reset_request.set_vector_index_id(Id());
  vector_reset_request.set_delete_data_file(delete_data_file);

  status = SendVectorResetRequest(vector_reset_request, vector_reset_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorReset request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_reset_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorReset response error, errcode: {}  errmsg: {} state: {}",
                                pb::error::Errno_Name(vector_reset_response.error().errcode()),
                                vector_reset_response.error().errmsg(),
                                pb::common::DiskANNCoreState_Name(vector_reset_response.state()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_reset_response.error().errcode(), s);
  }

  state = vector_reset_response.state();

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDestroyRequest(const google::protobuf::Message& request,
                                                           google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorDestroy", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDestroy request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDestroyRequestWrapper() {
  butil::Status status;
  pb::diskann::VectorDestroyRequest vector_destroy_request;
  vector_destroy_request.set_vector_index_id(Id());
  pb::diskann::VectorDestroyResponse vector_destroy_response;

  status = SendVectorDestroyRequest(vector_destroy_request, vector_destroy_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDestroy request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_destroy_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDestroy response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(vector_destroy_response.error().errcode()),
                                vector_destroy_response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_destroy_response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDumpRequest(const google::protobuf::Message& request,
                                                        google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorDump", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDump request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDumpRequestWrapper(std::vector<std::string>& dump_datas) {
  butil::Status status;
  pb::diskann::VectorDumpRequest vector_dump_request;
  pb::diskann::VectorDumpResponse vector_dump_response;

  vector_dump_request.set_vector_index_id(Id());

  status = SendVectorDumpRequest(vector_dump_request, vector_dump_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDump request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_dump_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDump response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(vector_dump_response.error().errcode()),
                                vector_dump_response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_dump_response.error().errcode(), s);
  }

  dump_datas.emplace_back(vector_dump_response.dump_data());

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDumpAllRequest(const google::protobuf::Message& request,
                                                           google::protobuf::Message& response) {
  butil::Status status;
  if (!InitChannel(diskann_server_addr)) {
    std::string s = fmt::format("Init channel failed, addr : {}", diskann_server_addr);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  status = SendRequest("DiskAnnService", "VectorDumpAll", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDumpAll request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;

    return butil::Status(status.error_code(), s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::SendVectorDumpAllRequestWrapper(std::vector<std::string>& dump_datas) {
  butil::Status status;
  pb::diskann::VectorDumpAllRequest vector_dump_all_request;
  pb::diskann::VectorDumpAllResponse vector_dump_all_response;

  status = SendVectorDumpAllRequest(vector_dump_all_request, vector_dump_all_response);
  if (!status.ok()) {
    std::string s = fmt::format("VectorDumpAll request failed, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(status.error_code()), status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(status.error_code(), s);
  }

  if (vector_dump_all_response.error().errcode() != pb::error::Errno::OK) {
    std::string s = fmt::format("VectorDumpAll response error, errcode: {}  errmsg: {}",
                                pb::error::Errno_Name(vector_dump_all_response.error().errcode()),
                                vector_dump_all_response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(vector_dump_all_response.error().errcode(), s);
  }

  for (int i = 0; i < vector_dump_all_response.dump_datas_size(); i++) {
    dump_datas.emplace_back(vector_dump_all_response.dump_datas(i));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexDiskANN::TransToDiskANNStateFromDiskANNCoreState(const pb::common::DiskANNCoreState& state,
                                                                          pb::error::Errno error,
                                                                          pb::common::DiskANNState& diskann_state) {
  switch (state) {
    case pb::common::RESETING:
      [[fallthrough]];
    case pb::common::RESET:
      [[fallthrough]];
    case pb::common::UNKNOWN: {
      diskann_state = pb::common::DiskANNState::DISKANN_INITIALIZED;
      break;
    }
    case pb::common::IMPORTING:
      [[fallthrough]];
    case pb::common::IMPORTED:
      [[fallthrough]];
    case pb::common::BUILDING:
      [[fallthrough]];
    case pb::common::BUILDED:
      [[fallthrough]];
    case pb::common::UPDATINGPATH: {
      diskann_state = pb::common::DiskANNState::DISKANN_BUILDING;
      if (error != pb::error::Errno::OK) {
        diskann_state = pb::common::DiskANNState::DISKANN_BUILD_FAILED;
      }
      break;
    }
    case pb::common::FAKEBUILDED:
      [[fallthrough]];
    case pb::common::UPDATEDPATH: {
      diskann_state = pb::common::DiskANNState::DISKANN_BUILDED;
      if (error != pb::error::Errno::OK) {
        diskann_state = pb::common::DiskANNState::DISKANN_BUILD_FAILED;
      }
      break;
    }
    case pb::common::LOADING: {
      diskann_state = pb::common::DiskANNState::DISKANN_LOADING;
      if (error != pb::error::Errno::OK) {
        diskann_state = pb::common::DiskANNState::DISKANN_LOAD_FAILED;
      }
      break;
    }
    case pb::common::LOADED: {
      diskann_state = pb::common::DiskANNState::DISKANN_LOADED;
      if (error != pb::error::Errno::OK) {
        diskann_state = pb::common::DiskANNState::DISKANN_LOAD_FAILED;
      }
      break;
    }
    case pb::common::DESTROYING:
      [[fallthrough]];
    case pb::common::DESTROYED:
      [[fallthrough]];
    case pb::common::SEARCHING:
      [[fallthrough]];
    case pb::common::SEARCHED:
      [[fallthrough]];
    case pb::common::IDLE:
      [[fallthrough]];
    case pb::common::FAILED:
      [[fallthrough]];
    case pb::common::UNINITIALIZED:
      [[fallthrough]];
    case pb::common::INITIALIZED: {
      std::string s =
          fmt::format("index receive wrong diskann server state : {}  {}  error : {}", static_cast<int>(state),
                      pb::common::DiskANNCoreState_Name(state), pb::error::Errno_Name(error));
      DINGO_LOG(FATAL) << s;
    }
    case pb::common::NODATA: {
      diskann_state = pb::common::DiskANNState::DISKANN_NODATA;
      break;
    }
    default: {
      break;
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
