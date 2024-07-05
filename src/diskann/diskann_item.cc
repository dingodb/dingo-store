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

#include "diskann/diskann_item.h"

#include <omp.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "diskann/diskann_core.h"
#include "diskann/diskann_utils.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

bvar::LatencyRecorder g_diskann_server_import_latency("dingo_diskann_server_import_latency");
bvar::LatencyRecorder g_diskann_server_build_latency("dingo_diskann_server_build_latency");
bvar::LatencyRecorder g_diskann_server_search_latency("dingo_diskann_server_search_latency");
bvar::LatencyRecorder g_diskann_server_status_latency("dingo_diskann_server_status_latency");
bvar::LatencyRecorder g_diskann_server_close_latency("dingo_diskann_server_close_latency");
bvar::LatencyRecorder g_diskann_server_load_latency("dingo_diskann_server_load_latency");
bvar::LatencyRecorder g_diskann_server_destroy_latency("dingo_diskann_server_destroy_latency");
bvar::LatencyRecorder g_diskann_server_tryload_latency("dingo_diskann_server_tryload_latency");
bvar::LatencyRecorder g_diskann_server_count_latency("dingo_diskann_server_count_latency");

#ifndef ENABLE_DISKANN_ITEM_PTHREAD
#define ENABLE_DISKANN_ITEM_PTHREAD
#endif

#undef ENABLE_DISKANN_ITEM_PTHREAD

DiskANNItem::DiskANNItem(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                         const pb::common::VectorIndexParameter& vector_index_parameter, u_int32_t num_threads,
                         float search_dram_budget_gb, float build_dram_budget_gb)
    : vector_index_id_(vector_index_id),
      vector_index_parameter_(vector_index_parameter),
      num_threads_(num_threads),
      search_dram_budget_gb_(search_dram_budget_gb),
      build_dram_budget_gb_(build_dram_budget_gb),
      is_import_(false),
      state_(DiskANNCoreState::kUnknown),
      already_recv_vector_count_(0),
      ts_(std::numeric_limits<int64_t>::min()),
      tso_(std::numeric_limits<int64_t>::min()),
      last_import_time_ms_(0) {
  remote_side_ = std::string(butil::endpoint2str(ctx->Cntl()->remote_side()).c_str());
  local_side_ = std::string(butil::endpoint2str(ctx->Cntl()->local_side()).c_str());
}

DiskANNItem::~DiskANNItem() {
  if (writer_.is_open()) writer_.close();
  if (diskann_core_) diskann_core_.reset();
#if defined(ENABLE_DISKANN_ID_MAPPING)
  if (id_writer_.is_open()) id_writer_.close();
#endif
}

butil::Status DiskANNItem::Import(std::shared_ptr<Context> ctx, const std::vector<pb::common::Vector>& vectors,
                                  const std::vector<int64_t>& vector_ids, bool has_more,
                                  bool /*force_to_load_data_if_exist*/, int64_t already_send_vector_count, int64_t ts,
                                  int64_t tso, int64_t& already_recv_vector_count) {
  DiskANNCoreState old_state;
  butil::Status status;
  RWLockWriteGuard guard(&rw_lock_);
  SetSide(ctx);
  bool is_error_occurred = false;
  old_state = state_.load();

  auto lambda_set_state_function = [this, &is_error_occurred, &status, ctx]() {
    if (is_error_occurred) {
      last_error_ = status;
      error_local_side_ = local_side_;
      error_remote_side_ = remote_side_;
    }

    ctx->SetStatus(last_error_);
    ctx->SetDiskANNCoreStateX(state_);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);
  if (!last_error_.ok()) {
    is_error_occurred = true;
    DINGO_LOG(ERROR) << "already error occurred, ignore import.  return." << last_error_.error_cstr();
    status = last_error_;
    return status;
  }

  if (is_import_) {
    std::string s = fmt::format("diskann is imported, ignore import.  {}", FormatParameter());
    DINGO_LOG(INFO) << s;
    status = butil::Status(pb::error::Errno::OK, s);
    return status;
  }

  if (state_.load() != DiskANNCoreState::kImporting && state_.load() != DiskANNCoreState::kUnknown) {
    std::string s = fmt::format("diskann item state wrong. {}", FormatParameter());
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG, s);
    return status;
  }

  if (writer_.is_open()) {
    if (ts != ts_) {
      std::string s = fmt::format("diskann import ts is : {}  not equal to last ts : {}", ts, ts_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IMPORT_TS_NOT_MATCH, s);
    }

    if (tso != tso_) {
      std::string s = fmt::format("diskann import tso is : {}  not equal to last tso : {}", tso, tso_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IMPORT_TSO_NOT_MATCH, s);
    }
  }

  try {
    BvarLatencyGuard bvar_guard(&g_diskann_server_import_latency);
    status = DoImport(vectors, vector_ids, has_more, already_send_vector_count, ts, tso, already_recv_vector_count,
                      old_state);
    if (!status.ok()) {
      is_error_occurred = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } catch (const std::exception& e) {
    is_error_occurred = true;
    std::string s = fmt::format("diskann import failed. {} {}", e.what(), FormatParameter());
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::Errno::EDISKANN_IMPORT_FAILED, s);
    return status;
  }

  is_error_occurred = false;
  return butil::Status::OK();
}

butil::Status DiskANNItem::Build(std::shared_ptr<Context> ctx, bool force_to_build, bool is_sync) {
  DiskANNCoreState old_state;
  butil::Status status;
  {
    RWLockWriteGuard guard(&rw_lock_);
    SetSide(ctx);
    bool is_error_occurred = false;
    old_state = state_.load();

    auto lambda_set_state_function = [this, &is_error_occurred, ctx, &status]() {
      if (is_error_occurred) {
        last_error_ = status;
        error_local_side_ = local_side_;
        error_remote_side_ = remote_side_;
      }
      ctx->SetStatus(last_error_);
      ctx->SetDiskANNCoreStateX(state_);
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (!last_error_.ok()) {
      is_error_occurred = true;
      DINGO_LOG(ERROR) << "already error occurred, ignore build.  return." << last_error_.error_cstr();
      status = last_error_;
      return status;
    }

    if (DiskANNCoreState::kUpdatedPath == state_) {
      std::string s = fmt::format("diskann is already builded, skip. {}", FormatParameter());
      DINGO_LOG(INFO) << s;
      status = butil::Status(pb::error::Errno::OK, s);
      return status;
    }

    if (!is_import_) {
      std::string s = fmt::format("diskann is not imported, import first. {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_NOT_IMPORT, s);
      return status;
    }

    if (DiskANNCoreState::kBuilding == state_.load()) {
      std::string s = fmt::format("diskann is building, wait... {}", FormatParameter());
      DINGO_LOG(INFO) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, s);
      return status;
    }

    if (DiskANNCoreState::kImported == state_.load()) {
      state_ = DiskANNCoreState::kBuilding;
      old_state = state_;
    } else {
      std::string s = fmt::format("diskann wrong state : {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_BUILD_STATE_WRONG, s);
      return status;
    }

    if (!diskann_core_) {
      index_path_prefix_ = fmt::format("{}/{}/{}/{}", base_dir, tmp_name, std::to_string(vector_index_id_), build_name);
      DiskANNUtils::CreateDir(base_dir);
      DiskANNUtils::CreateDir(base_dir + "/" + tmp_name);
      DiskANNUtils::CreateDir(base_dir + "/" + tmp_name + "/" + std::to_string(vector_index_id_));
      DiskANNUtils::CreateDir(base_dir + "/" + tmp_name + "/" + std::to_string(vector_index_id_) + "/" + build_name);
      diskann_core_ =
          std::make_shared<DiskANNCore>(vector_index_id_, vector_index_parameter_, num_threads_, search_dram_budget_gb_,
                                        build_dram_budget_gb_, data_path_, index_path_prefix_);
    }

    BvarLatencyGuard bvar_guard(&g_diskann_server_build_latency);
    if (!is_sync) {
      butil::Status s = DoAsyncBuild(ctx, force_to_build, old_state);
      if (!s.ok()) {
        is_error_occurred = true;
        DINGO_LOG(ERROR) << s.error_cstr();
        status = s;
        return s;
      }

      status = butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, "diskann is building, wait... ");
      return status;
    }
    is_error_occurred = false;
  }

  return DoSyncBuild(ctx, force_to_build, old_state);
}

butil::Status DiskANNItem::Load(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                bool is_sync) {
  DiskANNCoreState old_state;
  butil::Status status;
  {
    RWLockWriteGuard guard(&rw_lock_);
    SetSide(ctx);
    bool is_error_occurred = false;
    old_state = state_.load();

    auto lambda_set_state_function = [this, &is_error_occurred, ctx, &status]() {
      if (is_error_occurred) {
        last_error_ = status;
        error_local_side_ = local_side_;
        error_remote_side_ = remote_side_;
      }
      ctx->SetStatus(last_error_);
      ctx->SetDiskANNCoreStateX(state_);
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (!last_error_.ok()) {
      is_error_occurred = true;
      DINGO_LOG(ERROR) << "already error occurred, ignore load.  return." << last_error_.error_cstr();
      status = last_error_;
      return status;
    }

    if (DiskANNCoreState::kLoaded == state_.load()) {
      std::string s = fmt::format("diskann already loaded, skip load.  {}", FormatParameter());
      DINGO_LOG(INFO) << s;
      return butil::Status(pb::error::Errno::OK, s);
    }

    if (DiskANNCoreState::kLoading == state_.load()) {
      std::string s = fmt::format("diskann is loading,  wait...  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
      return status;
    }

    if (DiskANNCoreState::kUpdatedPath == state_.load()) {
      state_.store(DiskANNCoreState::kLoading);
      old_state = state_;
    } else {
      std::string s = fmt::format("diskann wrong state : {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_LOAD_STATE_WRONG, s);
      return status;
    }

    BvarLatencyGuard bvar_guard(&g_diskann_server_load_latency);
    if (!is_sync) {
      butil::Status s = DoAsyncLoad(ctx, load_param, old_state);
      if (!s.ok()) {
        is_error_occurred = true;
        DINGO_LOG(ERROR) << s.error_cstr();
        status = s;
        return s;
      }
      status = butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, "diskann is loading, wait... ");
      return status;
    }
  }

  return DoSyncLoad(ctx, load_param, old_state);
}

butil::Status DiskANNItem::Search(std::shared_ptr<Context> ctx, uint32_t top_n,
                                  const pb::common::SearchDiskAnnParam& search_param,
                                  const std::vector<pb::common::Vector>& vectors,
                                  std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& ts) {
  butil::Status status;
  RWLockReadGuard guard(&rw_lock_);
  SetSide(ctx);

  if (!last_error_.ok()) {
    DINGO_LOG(ERROR) << "already error occurred, ignore search.  return." << last_error_.error_cstr();
    status = last_error_;
    return status;
  }

  if (DiskANNCoreState::kLoaded != state_.load()) {
    if (DiskANNCoreState::kBuilded == state_.load() || DiskANNCoreState::kUpdatingPath == state_.load() ||
        DiskANNCoreState::kUpdatedPath == state_.load()) {
      std::string s = fmt::format("diskann not load. load first. {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_NOT_LOAD, s);
    } else if (DiskANNCoreState::kBuilding == state_.load()) {
      std::string s = fmt::format("diskann is building, wait... {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, s);
    } else if (DiskANNCoreState::kLoading == state_.load()) {
      std::string s = fmt::format("diskann is loading, wait... {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
    } else if (DiskANNCoreState::kImporting == state_.load()) {
      std::string s = fmt::format("diskann is importing, wait... {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_IMPORTING, s);
    } else if (DiskANNCoreState::kImported == state_.load()) {
      std::string s = fmt::format("diskann is imported, build first. {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_NOT_BUILD, s);
    } else {
      bool exist = false;
      exist = IsBuildedFilesExist(vector_index_id_, vector_index_parameter_.diskann_parameter().metric_type());
      if (exist) {
        std::string s = fmt::format("diskann not load. load first. {} ", FormatParameter());
        DINGO_LOG(ERROR) << s;
        status = butil::Status(pb::error::Errno::EDISKANN_NOT_LOAD, s);
      } else {
        std::string s = fmt::format("diskann not build. build first. {} ", FormatParameter());
        DINGO_LOG(ERROR) << s;
        status = butil::Status(pb::error::Errno::EDISKANN_NOT_BUILD, s);
      }
    }

    return status;
  }

  DiskANNCoreState state;

#if defined(ENABLE_DISKANN_ID_MAPPING)
  BvarLatencyGuard bvar_guard(&g_diskann_server_search_latency);
  status = diskann_core_->Search(top_n, search_param, vectors, IsBuildedFilesExist, results, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      auto id = vector_with_distance.mutable_vector_with_id()->id();
      vector_with_distance.mutable_vector_with_id()->set_id(diskann_to_vector_ids_[id]);
    }
  }
  ts = ts_;

  return butil::Status::OK();
#else
  ts = ts_;
  BvarLatencyGuard bvar_guard(&g_diskann_server_search_latency);
  status = diskann_core_->Search(top_n, search_param, vectors, IsBuildedFilesExist, results, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return status;
#endif
}

butil::Status DiskANNItem::TryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                   bool is_sync) {
  DiskANNCoreState old_state;
  butil::Status status;
  {
    RWLockWriteGuard guard(&rw_lock_);
    SetSide(ctx);
    bool is_error_occurred = false;
    old_state = state_.load();

    auto lambda_set_state_function = [this, &is_error_occurred, &status, ctx]() {
      if (is_error_occurred) {
        last_error_ = status;
        error_local_side_ = local_side_;
        error_remote_side_ = remote_side_;
      }
      ctx->SetStatus(last_error_);
      ctx->SetDiskANNCoreStateX(state_);
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (!last_error_.ok()) {
      DINGO_LOG(ERROR) << "already error occurred, ignore load.  return." << last_error_.error_cstr();
      status = last_error_;
      return status;
    }

    if (state_.load() == DiskANNCoreState::kLoaded) {
      std::string s = fmt::format("diskann already loaded, skip load. {}", FormatParameter());
      status = butil::Status(pb::error::Errno::OK, s);
      return status;
    }

    if (state_.load() == DiskANNCoreState::kLoading) {
      std::string s = fmt::format("diskann item is loading, wait... {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
      return status;
    }

    if (state_.load() != DiskANNCoreState::kUnknown) {
      std::string s = fmt::format("diskann item state wrong : {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_TRYLOAD_STATE_WRONG, s);
      return status;
    }

    bool exist = IsBuildedFilesExist(vector_index_id_, vector_index_parameter_.diskann_parameter().metric_type());
    if (!exist) {
      std::string s = fmt::format("diskann not build. build first. {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_NOT_BUILD, s);
      return status;
    }

    state_.store(DiskANNCoreState::kLoading);
    old_state = state_;

    if (!diskann_core_) {
      std::string data_path;
      data_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, std::to_string(vector_index_id_), input_name);
      DiskANNUtils::CreateDir(base_dir);
      DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
      DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));

      std::string index_path_prefix =
          fmt::format("{}/{}/{}/{}", base_dir, normal_name, std::to_string(vector_index_id_), build_name);
      DiskANNUtils::CreateDir(base_dir);
      DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
      DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));
      DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_) + "/" + build_name);
      diskann_core_ =
          std::make_shared<DiskANNCore>(vector_index_id_, vector_index_parameter_, num_threads_, search_dram_budget_gb_,
                                        build_dram_budget_gb_, data_path, index_path_prefix);
      data_path_ = data_path;
      index_path_prefix_ = index_path_prefix;
    }

#if defined(ENABLE_DISKANN_ID_MAPPING)
    std::string id_path;
    id_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, std::to_string(vector_index_id_), id_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));
    id_path_ = id_path;
#endif

    BvarLatencyGuard bvar_guard(&g_diskann_server_tryload_latency);
    if (!is_sync) {
      butil::Status s = DoAsyncTryLoad(ctx, load_param, old_state);
      if (!s.ok()) {
        is_error_occurred = true;
        DINGO_LOG(ERROR) << s.error_cstr();
        return s;
      }

      status = butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, "diskann is try loading, wait... ");
      return status;
    }
  }

  return DoSyncTryLoad(ctx, load_param, old_state);
}

butil::Status DiskANNItem::Close(std::shared_ptr<Context> ctx) {
  BvarLatencyGuard bvar_guard(&g_diskann_server_close_latency);
  RWLockWriteGuard guard(&rw_lock_);
  SetSide(ctx);
  return DoClose(ctx, false);
}

butil::Status DiskANNItem::Destroy(std::shared_ptr<Context> ctx) {
  BvarLatencyGuard bvar_guard(&g_diskann_server_destroy_latency);
  RWLockWriteGuard guard(&rw_lock_);
  SetSide(ctx);
  return DoClose(ctx, true);
}

DiskANNCoreState DiskANNItem::Status(std::shared_ptr<Context> ctx) {
  DiskANNCoreState state;
  butil::Status status;
  BvarLatencyGuard bvar_guard(&g_diskann_server_status_latency);
  RWLockReadGuard guard(&rw_lock_);
  SetSide(ctx);
  state = state_.load();

  status = last_error_;
  if (DiskANNCoreState::kImporting == state) {
    if (status.ok()) {
      auto current_time_ms = Helper::TimestampMs();
      if ((current_time_ms - last_import_time_ms_) > (import_timeout_s * 1000)) {
        std::string s = fmt::format(
            "diskann import timeout, last import time is : {} ms, current time is : {} ms  timeout is : {} s",
            last_import_time_ms_, current_time_ms, import_timeout_s);
        DINGO_LOG(ERROR) << s;
        status = butil::Status(pb::error::Errno::EDISKANN_IMPORT_TIMEOUT, s);
      }
    }
  }
  ctx->SetStatus(status);
  ctx->SetDiskANNCoreStateX(state);

  return state;
}
std::string DiskANNItem::Dump(std::shared_ptr<Context> ctx) {
  DiskANNCoreState state;
  RWLockReadGuard guard(&rw_lock_);
  SetSide(ctx);
  state = state_.load();

  ctx->SetStatus(last_error_);
  ctx->SetDiskANNCoreStateX(state);
  if (diskann_core_) {
    return diskann_core_->Dump() + "\n" + MiniFormatParameter();
  } else {
    return FormatParameter();
  }
}

butil::Status DiskANNItem::Count(std::shared_ptr<Context> ctx, int64_t& count) {
  DiskANNCoreState state;
  BvarLatencyGuard bvar_guard(&g_diskann_server_count_latency);
  RWLockReadGuard guard(&rw_lock_);
  SetSide(ctx);
  if (diskann_core_) {
    diskann_core_->Count(count, state);
  } else {
    count = 0;
    state = state_.load();
  }

  ctx->SetStatus(last_error_);
  ctx->SetDiskANNCoreStateX(state);

  return butil::Status::OK();
}

bool DiskANNItem::IsBuildedFilesExist(int64_t vector_index_id, pb::common::MetricType metric_type) {
  std::string data_bin_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id, input_name);
#if defined(ENABLE_DISKANN_ID_MAPPING)
  std::string id_bin_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id, id_name);
#endif
  std::string build_dir = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id, build_name);

  butil::Status status;
  status = DiskANNUtils::FileExistsAndRegular(data_bin_path);
  if (!status.ok()) {
    return false;
  }

#if defined(ENABLE_DISKANN_ID_MAPPING)
  status = DiskANNUtils::FileExistsAndRegular(id_bin_path);
  if (!status.ok()) {
    return false;
  }
#endif

  bool build_with_mem_index;
  status = DiskANNUtils::DiskANNIndexPathPrefixExists(build_dir, build_with_mem_index, metric_type);
  return status.ok();
}

butil::Status DiskANNItem::DoImport(const std::vector<pb::common::Vector>& vectors,
                                    const std::vector<int64_t>& vector_ids, bool has_more,
                                    int64_t already_send_vector_count, int64_t ts, int64_t tso,
                                    int64_t& already_recv_vector_count, DiskANNCoreState& old_state) {
  auto dimension = vector_index_parameter_.diskann_parameter().dimension();

  if (already_send_vector_count != already_recv_vector_count_) {
    std::string s = fmt::format("already_send_vector_count:{} != already_recv_vector_count_:{} {}",
                                already_send_vector_count, already_recv_vector_count_, FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_FILE_TRANSFER_QUANTITY_MISMATCH, s);
  }

  if (!writer_.is_open()) {
    last_import_time_ms_ = Helper::TimestampMs();
  }

  int64_t current_time_ms = Helper::TimestampMs();
  if ((current_time_ms - last_import_time_ms_) > (import_timeout_s * 1000)) {
    std::string s =
        fmt::format("diskann import timeout, last import time is : {} ms, current time is : {} ms  timeout is : {} s",
                    last_import_time_ms_, current_time_ms, import_timeout_s);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_TIMEOUT, s);
  }

  last_import_time_ms_ = current_time_ms;

  if (!writer_.is_open()) {
    state_.store(DiskANNCoreState::kImporting);
    old_state = state_;
    std::string data_path = fmt::format("{}/{}/{}/{}", base_dir, tmp_name, vector_index_id_, input_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + tmp_name);
    DiskANNUtils::CreateDir(base_dir + "/" + tmp_name + "/" + std::to_string(vector_index_id_));
    DiskANNUtils::RemoveFile(data_path);
    diskann::open_file_to_write(writer_, data_path);
    data_path_ = data_path;
    uint32_t count = 0;
    uint32_t dim = dimension;
    writer_.write((char*)&count, sizeof(uint32_t));
    writer_.write((char*)&dim, sizeof(uint32_t));
  }

#if defined(ENABLE_DISKANN_ID_MAPPING)
  if (!id_writer_.is_open()) {
    std::string id_path = fmt::format("{}/{}/{}/{}", base_dir, tmp_name, vector_index_id_, id_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + tmp_name);
    DiskANNUtils::CreateDir(base_dir + "/" + tmp_name + "/" + std::to_string(vector_index_id_));
    DiskANNUtils::RemoveFile(id_path);
    diskann::open_file_to_write(id_writer_, id_path);
    id_path_ = id_path;
    uint32_t count = 0;
    uint32_t dim = dimension;
    id_writer_.write((char*)&count, sizeof(uint32_t));
    id_writer_.write((char*)&dim, sizeof(uint32_t));
    id_writer_.write((char*)&ts, sizeof(int64_t));
    ts_ = ts;
    tso_ = tso;
  }
#endif

  for (const auto& vector : vectors) {
    const auto& vector_value = vector.float_values();
    uint32_t total = dimension * sizeof(float);
    uint32_t remain = total;
    uint32_t already = 0;
    while (remain > 0) {
      uint32_t write_size = std::min(remain, (uint32_t)4 * 1024);
      writer_.write(reinterpret_cast<const char*>(vector_value.data()) + already, write_size);
      already += write_size;
      remain -= write_size;
    }
  }

#if defined(ENABLE_DISKANN_ID_MAPPING)
  if (vectors.size() != vector_ids.size()) {
    std::string s = fmt::format("diskann import vector count is : {}  not equal to id count : {}", vectors.size(),
                                vector_ids.size());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_COUNT_NOT_MATCH, s);
  }

  for (const auto& id : vector_ids) {
    vector_to_diskann_ids_.insert(std::make_pair(id, diskann_to_vector_ids_.size()));
    diskann_to_vector_ids_.push_back(id);
    id_writer_.write(reinterpret_cast<const char*>(&id), sizeof(id));
  }

  if (vector_to_diskann_ids_.size() != diskann_to_vector_ids_.size()) {
    std::string s = fmt::format("diskann import vector id count is : {}  not equal to diskann id count : {}",
                                vector_to_diskann_ids_.size(), diskann_to_vector_ids_.size());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED, s);
  }
#endif

  already_recv_vector_count_ += vectors.size();
  already_recv_vector_count = already_recv_vector_count_;

  if (already_recv_vector_count > Constant::kDiskannMaxCount) {
    std::string s = fmt::format("diskann import total vector count is : {}  more than : {}, not support build. {}",
                                already_recv_vector_count, Constant::kDiskannMaxCount, FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_COUNT_TOO_MANY, s);
  }

  if (!has_more) {
    writer_.seekp(0, std::ios::beg);
    if (already_recv_vector_count < Constant::kDiskannMinCount) {
      std::string s = fmt::format("diskann import total vector count is : {}  less than : {}, not support build. {}",
                                  already_recv_vector_count, Constant::kDiskannMinCount, FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IMPORT_COUNT_TOO_FEW, s);
    }

    uint32_t count = already_recv_vector_count;
    writer_.write((char*)&count, sizeof(uint32_t));
    writer_.close();
    std::string new_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id_, input_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));
    butil::Status status = DiskANNUtils::Rename(data_path_, new_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

#if defined(ENABLE_DISKANN_ID_MAPPING)
    id_writer_.seekp(0, std::ios::beg);
    id_writer_.write((char*)&count, sizeof(uint32_t));
    id_writer_.close();
    std::string new_id_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id_, id_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
    DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));
    status = DiskANNUtils::Rename(id_path_, new_id_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    id_path_ = new_id_path;
#endif

    data_path_ = new_path;
    state_.store(DiskANNCoreState::kImported);
    is_import_ = true;
  }

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoSyncBuild(std::shared_ptr<Context> ctx, bool force_to_build, DiskANNCoreState old_state) {
  return DoBuildInternal(ctx, force_to_build, old_state);
}

butil::Status DiskANNItem::DoAsyncBuild(std::shared_ptr<Context> ctx, bool force_to_build, DiskANNCoreState old_state) {
  auto lambda_call = [this, force_to_build, old_state, ctx]() {
    this->DoBuildInternal(ctx, force_to_build, old_state);
  };

#if defined(ENABLE_DISKANN_ITEM_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else
  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_DISKANN_ITEM_PTHREAD)

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoBuildInternal(std::shared_ptr<Context> ctx, bool force_to_build,
                                           DiskANNCoreState /*old_state*/) {
  DiskANNCoreState state;
  butil::Status status;
  std::string new_index_path_prefix;
  bool is_error_occurred = true;
  auto lambda_set_state_function = [this, &is_error_occurred, &status, ctx, &state, &new_index_path_prefix]() {
    RWLockWriteGuard guard(&rw_lock_);
    state_.store(state);
    if (is_error_occurred) {
      last_error_ = status;
      error_local_side_ = local_side_;
      error_remote_side_ = remote_side_;
    } else {
      index_path_prefix_ = new_index_path_prefix;
    }
    ctx->SetStatus(last_error_);
    ctx->SetDiskANNCoreStateX(state_);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);

  status = diskann_core_->Build(force_to_build, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  new_index_path_prefix =
      fmt::format("{}/{}/{}/{}", base_dir, normal_name, std::to_string(vector_index_id_), build_name);
  DiskANNUtils::CreateDir(base_dir);
  DiskANNUtils::CreateDir(base_dir + "/" + normal_name);
  DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_));
  DiskANNUtils::CreateDir(base_dir + "/" + normal_name + "/" + std::to_string(vector_index_id_) + "/" + build_name);
  status = DiskANNUtils::Rename(index_path_prefix_, new_index_path_prefix);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  status = diskann_core_->UpdateIndexPathPrefix(new_index_path_prefix, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  is_error_occurred = false;

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoSyncLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                      DiskANNCoreState old_state) {
  return DoLoadInternal(ctx, load_param, old_state);
}

butil::Status DiskANNItem::DoAsyncLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                       DiskANNCoreState old_state) {
  auto lambda_call = [this, &load_param, old_state, ctx]() { this->DoLoadInternal(ctx, load_param, old_state); };

#if defined(ENABLE_DISKANN_ITEM_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_DISKANN_ITEM_PTHREAD)

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoLoadInternal(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                          DiskANNCoreState /*old_state*/) {
  DiskANNCoreState state;
  butil::Status status;
  bool is_error_occurred = true;

  auto lambda_set_state_function = [this, &is_error_occurred, &status, ctx, &state]() {
    RWLockWriteGuard guard(&rw_lock_);
    state_.store(state);
    if (is_error_occurred) {
      last_error_ = status;
      error_local_side_ = local_side_;
      error_remote_side_ = remote_side_;
    }
    ctx->SetStatus(last_error_);
    ctx->SetDiskANNCoreStateX(state_);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);
  status = diskann_core_->Load(load_param, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  is_error_occurred = false;

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoSyncTryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                         DiskANNCoreState old_state) {
  return DoTryLoadInternal(ctx, load_param, old_state);
}

butil::Status DiskANNItem::DoAsyncTryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                          DiskANNCoreState old_state) {
  auto lambda_call = [this, &load_param, old_state, ctx]() { this->DoTryLoadInternal(ctx, load_param, old_state); };

#if defined(ENABLE_DISKANN_ITEM_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_DISKANN_ITEM_PTHREAD)

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoTryLoadInternal(std::shared_ptr<Context> ctx,
                                             const pb::common::LoadDiskAnnParam& load_param,
                                             DiskANNCoreState /*old_state*/) {
  DiskANNCoreState state;
  butil::Status status;
  int64_t ts = 0;
  bool is_error_occurred = true;

  auto lambda_set_state_function = [this, &is_error_occurred, &state, &status, ctx, &ts]() {
    RWLockWriteGuard guard(&rw_lock_);
    state_.store(state);
    if (is_error_occurred) {
      last_error_ = status;
      error_local_side_ = local_side_;
      error_remote_side_ = remote_side_;
    } else {
      ts_ = ts;
    }
    ctx->SetStatus(last_error_);
    ctx->SetDiskANNCoreStateX(state_);
  };

  ON_SCOPE_EXIT(lambda_set_state_function);
  status = diskann_core_->TryLoad(load_param, state);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
#if defined(ENABLE_DISKANN_ID_MAPPING)

  try {
    std::ifstream reader(id_path_.c_str(), std::ios::binary);
    uint32_t count = 0;
    uint32_t dim = 0;
    ts = 0;
    reader.read((char*)&count, sizeof(uint32_t));
    reader.read((char*)&dim, sizeof(uint32_t));
    reader.read((char*)&ts, sizeof(int64_t));

    if (dim != vector_index_parameter_.diskann_parameter().dimension()) {
      std::string s = fmt::format("diskann id.bin dimension is : {}  not equal to dimension : {} {}", dim,
                                  vector_index_parameter_.diskann_parameter().dimension(), FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_ID_BIN_DIMENSION_NOT_MATCH, s);
      return status;
    }

    std::vector<int64_t> buffer;
    int64_t remain = 0;
    size_t buffer_size = 1024 * 1024;
    buffer.resize(buffer_size);
    uint32_t real_count = 0;
    vector_to_diskann_ids_.clear();
    diskann_to_vector_ids_.clear();
    while (true) {
      reader.read(reinterpret_cast<char*>(buffer.data()) + remain, sizeof(int64_t) * buffer_size - remain);
      int64_t num = reader.gcount();
      for (int i = 0; i < (num + remain) / sizeof(int64_t); i++) {
        vector_to_diskann_ids_.insert(std::make_pair(buffer[i], diskann_to_vector_ids_.size()));
        diskann_to_vector_ids_.push_back(buffer[i]);
        real_count++;
      }
      memmove(reinterpret_cast<char*>(buffer.data()),
              reinterpret_cast<char*>(buffer.data()) + ((num + remain) - (num + remain) % sizeof(int64_t)),
              (num + remain) % sizeof(int64_t));
      remain = (num + remain) % sizeof(int64_t);

      if (0 == num) {
        break;
      }
    }

    if (count != real_count) {
      std::string s = fmt::format("diskann id.bin record count is : {}  not equal to id.bin real count : {} {}", count,
                                  real_count, FormatParameter());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(pb::error::Errno::EDISKANN_ID_BIN_COUNT_NOT_MATCH, s);
      return status;
    }
  } catch (const std::exception& e) {
    std::string s = fmt::format("diskann id.bin read error : {} {}", e.what(), FormatParameter());
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::Errno::EDISKANN_ID_BIN_READ_ERROR, s);
    return status;
  }

#endif
  is_error_occurred = false;

  return butil::Status::OK();
}

butil::Status DiskANNItem::DoClose(std::shared_ptr<Context> ctx, bool is_destroy) {
  bool is_delete_files = false;
  bool is_force = false;
  DiskANNCoreState state;
  if (diskann_core_) {
    auto status = diskann_core_->Reset(is_delete_files, state, is_force);
    if (!status.ok()) {
      std::string s = fmt::format("diskann DoClose fail : {}", status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }
  }
  data_path_ = "";
  index_path_prefix_ = "";
  is_import_ = false;
  state_ = DiskANNCoreState::kUnknown;
  if (writer_.is_open()) writer_.close();
  already_recv_vector_count_ = 0;
  if (diskann_core_) diskann_core_.reset();
#if defined(ENABLE_DISKANN_ID_MAPPING)
  diskann_to_vector_ids_.clear();
  vector_to_diskann_ids_.clear();
  if (id_writer_.is_open()) id_writer_.close();
  id_path_.clear();
#endif
  ts_ = std::numeric_limits<int64_t>::min();
  tso_ = std::numeric_limits<int64_t>::min();
  last_import_time_ms_ = 0;
  remote_side_.clear();
  local_side_.clear();
  error_remote_side_.clear();
  error_local_side_.clear();
  last_error_ = butil::Status::OK();

  ctx->SetStatus(last_error_);
  ctx->SetDiskANNCoreStateX(DiskANNCoreState::kReset);

  if (is_destroy) {
    std::string destroyed_data_path =
        fmt::format("{}/{}/{}/{}", base_dir, destroyed_name, vector_index_id_, input_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + destroyed_name);
    DiskANNUtils::CreateDir(base_dir + "/" + destroyed_name + "/" + std::to_string(vector_index_id_));

    std::string destroyed_index_path_prefix =
        fmt::format("{}/{}/{}/{}", base_dir, destroyed_name, vector_index_id_, build_name);

    DiskANNUtils::CreateDir(base_dir + "/" + destroyed_name + "/" + std::to_string(vector_index_id_) + "/" +
                            build_name);

    std::string normal_data_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id_, input_name);
    std::string normal_index_path_prefix =
        fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id_, build_name);

#if defined(ENABLE_DISKANN_ID_MAPPING)
    std::string destroyed_id_path = fmt::format("{}/{}/{}/{}", base_dir, destroyed_name, vector_index_id_, id_name);
    DiskANNUtils::CreateDir(base_dir);
    DiskANNUtils::CreateDir(base_dir + "/" + destroyed_name);
    DiskANNUtils::CreateDir(base_dir + "/" + destroyed_name + "/" + std::to_string(vector_index_id_));

    std::string normal_id_path = fmt::format("{}/{}/{}/{}", base_dir, normal_name, vector_index_id_, id_name);
#endif

    DiskANNUtils::Rename(normal_data_path, destroyed_data_path);
    DiskANNUtils::Rename(normal_index_path_prefix, destroyed_index_path_prefix);

#if defined(ENABLE_DISKANN_ID_MAPPING)
    DiskANNUtils::Rename(normal_id_path, destroyed_id_path);
#endif
  }

  return butil::Status::OK();
}

std::string DiskANNItem::FormatParameter() {
#if defined(ENABLE_DISKANN_ID_MAPPING)
  std::string s = fmt::format(
      " vector_index_id:{} num_threads:{} search_dram_budget_gb:{} "
      "build_dram_budget_gb:{} data_path:\"{}\" index_path_prefix:\"{}\" is_import:{} state:\"{}\" writer:{} "
      "already_recv_vector_count:{} diskann_core:\"{}\" diskann_to_vector_ids.size():{} "
      "vector_to_diskann_ids.size():{} id_writer:{} id_path:\"{}\" ts:{} tso:{} last_error : {} {} remote_side:{} "
      "local_side:{} error_remote_side:{} error_local_side:{} "
      "vector_index_parameter: {} ",
      vector_index_id_, num_threads_, search_dram_budget_gb_, build_dram_budget_gb_, data_path_, index_path_prefix_,
      (is_import_ ? "true" : "false"), DiskANNUtils::DiskANNCoreStateToString(state_),
      (writer_.is_open() ? "open" : "close"), already_recv_vector_count_, (diskann_core_ ? "exist" : "null"),
      diskann_to_vector_ids_.size(), vector_to_diskann_ids_.size(), (id_writer_.is_open() ? "open" : "close"), id_path_,
      ts_, tso_, last_error_.error_code(), last_error_.error_cstr(), remote_side_, local_side_, error_remote_side_,
      error_local_side_, vector_index_parameter_.ShortDebugString());
#else
  std::string s = fmt::format(
      " vector_index_id:{} num_threads:{} search_dram_budget_gb:{} "
      "build_dram_budget_gb:{} data_path:\"{}\" index_path_prefix:\"{}\" is_import:{} state:\"{}\" writer:{} "
      "already_recv_vector_count:{} diskann_core:\"{}\" ts:{} tso:{} last_error_ : {} {} remote_side:{} "
      "local_side:{} error_remote_side:{} error_local_side:{} "
      "vector_index_parameter: {} ",
      vector_index_id_, num_threads_, search_dram_budget_gb_, build_dram_budget_gb_, data_path_, index_path_prefix_,
      (is_import_ ? "true" : "false"), DiskANNUtils::DiskANNCoreStateToString(state_),
      (writer_.is_open() ? "open" : "close"), already_recv_vector_count_, (diskann_core_ ? "exist" : "null"), ts, tso,
      last_error.error_code(), last_error_.error_cstr(), remote_side_, local_side_, error_remote_side_,
      error_local_side_, vector_index_parameter_.ShortDebugString());
#endif

  return s;
}

std::string DiskANNItem::MiniFormatParameter() {
#if defined(ENABLE_DISKANN_ID_MAPPING)
  std::string s = fmt::format(
      " is_import:{} state:\"{}\" writer:{} "
      "already_recv_vector_count:{} diskann_core:\"{}\" diskann_to_vector_ids.size():{} "
      "vector_to_diskann_ids.size():{} id_writer:{} id_path:\"{}\" ts:{} tso:{} last_error : {} {} remote_side:{} "
      "local_side:{} error_remote_side:{} error_local_side:{} ",
      (is_import_ ? "true" : "false"), DiskANNUtils::DiskANNCoreStateToString(state_),
      (writer_.is_open() ? "open" : "close"), already_recv_vector_count_, (diskann_core_ ? "exist" : "null"),
      diskann_to_vector_ids_.size(), vector_to_diskann_ids_.size(), (id_writer_.is_open() ? "open" : "close"), id_path_,
      ts_, tso_, last_error_.error_code(), last_error_.error_cstr(), remote_side_, local_side_, error_remote_side_,
      error_local_side_);
#else
  std::string s = fmt::format(
      " is_import:{} state:\"{}\" writer:{} "
      "already_recv_vector_count:{} diskann_core:\"{}\" ts:{} tso:{} last_error_ : {} {} remote_side:{} "
      "local_side:{} error_remote_side:{} error_local_side:{} ",
      (is_import_ ? "true" : "false"), DiskANNUtils::DiskANNCoreStateToString(state_),
      (writer_.is_open() ? "open" : "close"), already_recv_vector_count_, (diskann_core_ ? "exist" : "null"), ts, tso,
      last_error.error_code(), last_error_.error_cstr(), remote_side_, local_side_, error_remote_side_,
      error_local_side_);
#endif

  return s;
}

void DiskANNItem::SetSide(std::shared_ptr<Context> ctx) {
  auto remote_side = std::string(butil::endpoint2str(ctx->Cntl()->remote_side()).c_str());
  auto local_side = std::string(butil::endpoint2str(ctx->Cntl()->local_side()).c_str());
  if (remote_side != remote_side_ || local_side != local_side_) {
    remote_side_ = remote_side;
    local_side_ = local_side;
    DINGO_LOG(INFO) << "vector_index_id: " << vector_index_id_ << "remote_side: " << remote_side_
                    << " local_side: " << local_side_;
  }
}

}  // namespace dingodb
