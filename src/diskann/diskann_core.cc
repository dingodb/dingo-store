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

#include "diskann/diskann_core.h"

#include <omp.h>
#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "disk_utils.h"
#include "diskann/diskann_utils.h"
#include "distance.h"
#include "fmt/core.h"
#include "linux_aligned_file_reader.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

DiskANNCore::DiskANNCore(int64_t vector_index_id, const pb::common::VectorIndexParameter& vector_index_parameter,
                         u_int32_t num_threads, float search_dram_budget_gb, float build_dram_budget_gb,
                         const std::string& data_path, const std::string& index_path_prefix)
    : vector_index_id_(vector_index_id),
      vector_index_parameter_(vector_index_parameter),
      num_threads_(num_threads),
      search_dram_budget_gb_(search_dram_budget_gb),
      build_dram_budget_gb_(build_dram_budget_gb),
      data_path_(data_path),
      index_path_prefix_(index_path_prefix),
      is_built_(false),
      count_(0),
      dimension_(0),
      build_with_mem_index_(false),
      is_update_(false),
      is_load_(false),
      metric_type_(pb::common::MetricType::METRIC_TYPE_NONE),
      metric_(diskann::Metric::L2),
      num_nodes_to_cache_(0),
      warmup_(true),
      state_(DiskANNCoreState::kUninitialized) {
  state_ = DiskANNCoreState::kInitialized;
}

DiskANNCore::~DiskANNCore() {
  DiskANNCoreState state;
  Reset(false, state, true);
}

butil::Status DiskANNCore::Build(bool force_to_build, DiskANNCoreState& state) {
  DiskANNCoreState old_state;
  {
    RWLockWriteGuard guard(&rw_lock_);
    bool is_error_occurred = true;
    old_state = state_.load();

    auto lambda_set_state_function = [&state, this, &is_error_occurred, &old_state]() {
      if (is_error_occurred) {
        state_.store(old_state);
      }
      state = state_.load();
    };

    ON_SCOPE_EXIT(lambda_set_state_function);

    if (!force_to_build && is_built_) {
      is_error_occurred = false;
      DINGO_LOG(INFO) << "already built. skip build";
      return butil::Status::OK();
    }

    if (!is_built_ && DiskANNCoreState::kBuilding == state_) {
      std::string s = fmt::format("diskann is building, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, s);
    }

    if (is_built_ && DiskANNCoreState::kLoading == state_) {
      std::string s = fmt::format("diskann is loading, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
    }

    if (force_to_build) {
      if (DiskANNCoreState::kInitialized != state_.load() && DiskANNCoreState::kBuilded != state_.load()) {
        std::string s = fmt::format("diskann build failed. state not init or builded : {}", FormatParameter());
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EDISKANN_NOT_INIT, s);
      }
    } else {
      if (DiskANNCoreState::kInitialized != state_.load()) {
        std::string s = fmt::format("diskann build failed. state not init : {}", FormatParameter());
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EDISKANN_NOT_INIT, s);
      }
    }

    count_ = 0;
    dimension_ = 0;
    metric_type_ = pb::common::MetricType::METRIC_TYPE_NONE;
    metric_ = diskann::Metric::L2;
    build_with_mem_index_ = false;
    is_built_ = false;
    is_load_ = false;
    state_.store(DiskANNCoreState::kBuilding);
    old_state = state_;
    state = state_.load();
    is_error_occurred = false;
  }

  return DoBuild(state, old_state);
}

butil::Status DiskANNCore::UpdateIndexPathPrefix(const std::string& index_path_prefix, DiskANNCoreState& state) {
  butil::Status status;
  DiskANNCoreState old_state;

  RWLockWriteGuard guard(&rw_lock_);
  bool is_error_occurred = true;
  old_state = state_.load();

  auto lambda_set_state_function = [&state, this, &is_error_occurred, &old_state]() {
    if (is_error_occurred) {
      state_.store(old_state);
    }
    state = state_.load();
  };

  ON_SCOPE_EXIT(lambda_set_state_function);

  if (DiskANNCoreState::kUpdatedPath == state_.load()) {
    is_error_occurred = false;
    std::string s = fmt::format("diskann path already updated. skip update. {} ", FormatParameter());
    DINGO_LOG(INFO) << s;
    return butil::Status(pb::error::Errno::OK, s);
  }

  if (DiskANNCoreState::kBuilding == state_.load()) {
    std::string s = fmt::format("diskann is building, try wait.  {}", FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, s);
  } else if (DiskANNCoreState::kBuilded == state_.load()) {
    // normal do nothing
  } else {
    std::string s = fmt::format("diskann path updated failed. state wrong", FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (!is_built_) {
    std::string s = fmt::format("diskann load failed. build first : {}", FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_NOT_BUILD, s);
  }

  state_.store(DiskANNCoreState::kUpdatingPath);
  old_state = state_;

  // check index_path_prefix is exist and is dir
  status = DiskANNUtils::DirExists(index_path_prefix);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  bool build_with_mem_index = false;
  status = DiskANNUtils::DiskANNIndexPathPrefixExists(index_path_prefix, build_with_mem_index, metric_type_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  index_path_prefix_ = index_path_prefix;
  if (index_path_prefix_.back() != '/') {
    index_path_prefix_ += "/";
  }

  is_update_ = true;
  state_.store(DiskANNCoreState::kUpdatedPath);
  is_error_occurred = false;

  return butil::Status::OK();
}

butil::Status DiskANNCore::Load(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState& state) {
  DiskANNCoreState old_state;
  {
    RWLockWriteGuard guard(&rw_lock_);
    bool is_error_occurred = true;
    old_state = state_.load();

    auto lambda_set_state_function = [&state, this, &is_error_occurred, &old_state]() {
      if (is_error_occurred) {
        state_.store(old_state);
      }
      state = state_.load();
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (is_load_) {
      is_error_occurred = false;
      DINGO_LOG(INFO) << "already load. skip load";
      return butil::Status::OK();
    }

    if (DiskANNCoreState::kLoading == state_.load()) {
      std::string s = fmt::format("diskann is loading, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
    }

    if (!is_update_) {
      std::string s = fmt::format("diskann load failed. update first : {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_NOT_UPDATE, s);
    }

    state_.store(DiskANNCoreState::kLoading);
    old_state = state_;
    is_error_occurred = false;
  }

  return DoLoad(load_param, old_state, state, false);
}

butil::Status DiskANNCore::Search(uint32_t top_n, const pb::common::SearchDiskAnnParam& search_param,
                                  const std::vector<pb::common::Vector>& vectors, ExistFunction exist_func,
                                  std::vector<pb::index::VectorWithDistanceResult>& results, DiskANNCoreState& state) {
  RWLockReadGuard guard(&rw_lock_);
  auto lambda_set_state_function = [&state, this]() { state = state_.load(); };
  ON_SCOPE_EXIT(lambda_set_state_function);
  if (!is_load_) {
    bool exist = false;
    if (exist_func) {
      exist = exist_func(vector_index_id_, vector_index_parameter_.diskann_parameter().metric_type());
    }
    if (exist) {
      std::string s = fmt::format("diskann not build. build first. {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_NOT_BUILD, s);
    } else {
      std::string s = fmt::format("diskann not load. load first. {} ", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_NOT_LOAD, s);
    }
  }

  if (0 == top_n) {
    return butil::Status::OK();
  }

  std::vector<std::vector<float>> vector_floats{vectors.size()};

  for (size_t i = 0; i < vectors.size(); ++i) {
    vector_floats[i].resize(dimension_);
    const auto& vector_value = vectors[i].float_values();
    memcpy(vector_floats[i].data(), vector_value.data(), dimension_ * sizeof(float));
  }

  uint64_t k_search = top_n;
  uint64_t l_search = k_search;

  std::vector<uint64_t> res_ids;
  std::vector<float> res_dists;

  const uint64_t beam_width = (0 == search_param.beamwidth()) ? 1 : search_param.beamwidth();
  const bool use_reorder_data = false;
  diskann::QueryStats query_stats;

  std::vector<std::vector<uint64_t>> result_labels;
  std::vector<std::vector<float>> result_distances;

  for (const auto& vector_float : vector_floats) {
    res_ids.resize(k_search, std::numeric_limits<uint64_t>::max());
    res_dists.resize(k_search, std::numeric_limits<float>::max());
    try {
      flash_index_->cached_beam_search(vector_float.data(), k_search, l_search, res_ids.data(), res_dists.data(),
                                       beam_width, use_reorder_data, &query_stats);
    } catch (const std::exception& e) {
      std::string s = fmt::format("cached_beam_search exception : {} {}", e.what(), FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
    result_labels.emplace_back(res_ids);
    result_distances.emplace_back(res_dists);
  }

  FillSearchResult(top_n, result_distances, result_labels, results);

  return butil::Status::OK();
}

butil::Status DiskANNCore::Reset(bool is_delete_files, DiskANNCoreState& state, bool is_force) {
  RWLockWriteGuard guard(&rw_lock_);
  auto lambda_set_state_function = [&state, this]() { state = state_.load(); };
  ON_SCOPE_EXIT(lambda_set_state_function);

  if (!is_force) {
    if (DiskANNCoreState::kBuilding == state_.load()) {
      std::string s = fmt::format("diskann is building, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_BUILDING, s);
    }

    if (DiskANNCoreState::kLoading == state_.load()) {
      std::string s = fmt::format("diskann is loading, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
    }
  }

  state_.store(DiskANNCoreState::kDestroying);
  flash_index_.reset();
  reader_.reset();
  vector_index_id_ = 0;
  vector_index_parameter_.Clear();
  num_threads_ = 0;
  search_dram_budget_gb_ = 0.0f;
  build_dram_budget_gb_ = 0.0f;
  is_built_ = false;
  count_ = 0;
  dimension_ = 0;
  build_with_mem_index_ = false;
  is_update_ = false;
  is_load_ = false;
  metric_type_ = pb::common::MetricType::METRIC_TYPE_NONE;
  metric_ = diskann::Metric::L2;
  num_nodes_to_cache_ = 0;
  warmup_ = true;

  if (is_delete_files) {
    DiskANNUtils::RemoveFile(data_path_);
    DiskANNUtils::RemoveDir(index_path_prefix_);
  }
  data_path_ = "";
  index_path_prefix_ = "";

  state_.store(DiskANNCoreState::kDestroyed);
  state_.store(DiskANNCoreState::kUninitialized);
  return butil::Status::OK();
}

butil::Status DiskANNCore::Init(int64_t vector_index_id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                u_int32_t num_threads, float search_dram_budget_gb, float build_dram_budget_gb,
                                const std::string& data_path, const std::string& index_path_prefix) {
  RWLockWriteGuard guard(&rw_lock_);

  vector_index_id_ = vector_index_id;
  vector_index_parameter_ = vector_index_parameter;
  num_threads_ = num_threads;
  search_dram_budget_gb_ = search_dram_budget_gb;
  build_dram_budget_gb_ = build_dram_budget_gb;
  data_path_ = data_path;
  index_path_prefix_ = index_path_prefix;
  is_built_ = false;
  count_ = 0;
  dimension_ = 0;
  build_with_mem_index_ = false;
  is_update_ = false;
  is_load_ = false;
  metric_type_ = pb::common::MetricType::METRIC_TYPE_NONE;
  metric_ = diskann::Metric::L2;
  reader_.reset();
  flash_index_.reset();
  num_nodes_to_cache_ = 0;
  warmup_ = true;
  state_.store(DiskANNCoreState::kInitialized);

  return butil::Status::OK();
}

butil::Status DiskANNCore::TryLoad(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState& state) {
  DiskANNCoreState old_state;
  {
    RWLockWriteGuard guard(&rw_lock_);
    bool is_error_occurred = true;
    old_state = state_.load();

    auto lambda_set_state_function = [&state, this, &is_error_occurred, &old_state]() {
      if (is_error_occurred) {
        state_.store(old_state);
      }
      state = state_.load();
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (is_load_) {
      is_error_occurred = false;
      DINGO_LOG(INFO) << "already load. skip load";
      return butil::Status::OK();
    }

    if (DiskANNCoreState::kLoading == state_.load()) {
      std::string s = fmt::format("try diskann is loading, try wait.  {}", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IS_LOADING, s);
    }

    if (DiskANNCoreState::kInitialized != state_.load()) {
      std::string s = fmt::format("try diskann load failed. state wrong", FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    state_.store(DiskANNCoreState::kLoading);
    old_state = state_;
    is_error_occurred = false;
  }

  return DoLoad(load_param, old_state, state, true);
}

DiskANNCoreState DiskANNCore::Status() {
  RWLockReadGuard guard(&rw_lock_);
  return state_.load();
}

bool DiskANNCore::IsBuilt() {
  RWLockReadGuard guard(&rw_lock_);
  return is_built_;
}
bool DiskANNCore::IsUpdate() {
  RWLockReadGuard guard(&rw_lock_);
  return is_update_;
}
bool DiskANNCore::IsLoad() {
  RWLockReadGuard guard(&rw_lock_);
  return is_load_;
}

std::string DiskANNCore::Dump() {
  RWLockReadGuard guard(&rw_lock_);
  return FormatParameter();
}

butil::Status DiskANNCore::Count(int64_t& count, DiskANNCoreState& state) {
  RWLockReadGuard guard(&rw_lock_);
  count = count_;
  state = state_.load();
  return butil::Status::OK();
}

butil::Status DiskANNCore::DoBuild(DiskANNCoreState& state, DiskANNCoreState old_state) {
  diskann::Metric metric;
  auto diskann_parameter = vector_index_parameter_.diskann_parameter();
  pb::common::MetricType metric_type = diskann_parameter.metric_type();
  size_t count = 0;
  size_t dim = 0;
  bool build_with_mem_index = false;

  // building
  {
    bool is_error_occur = true;
    auto lambda_set_state_function = [&is_error_occur, &state, this, old_state]() {
      if (is_error_occur) {
        RWLockWriteGuard guard(&rw_lock_);
        state_.store(old_state);
        state = state_.load();
      }
    };

    ON_SCOPE_EXIT(lambda_set_state_function);

    auto value_type = diskann_parameter.value_type();
    if (value_type == pb::common::ValueType::INT8_T) {
      std::string s = "diskann value_type not support int8";
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
    } else if (value_type == pb::common::ValueType::UINT8) {
      std::string s = "diskann value_type not support uint8";
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
    }

    // check data_path is exist and regular file.
    butil::Status status = DiskANNUtils::FileExistsAndRegular(data_path_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // check index_path_prefix is exist and is dir
    status = DiskANNUtils::DirExists(index_path_prefix_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // clear index_path_prefix
    status = DiskANNUtils::ClearDir(index_path_prefix_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (index_path_prefix_.back() != '/') {
      index_path_prefix_ += "/";
    }

    try {
      diskann::get_bin_metadata(data_path_, count, dim);
    } catch (const std::exception& e) {
      std::string s = fmt::format("get_bin_metadata exception : {} {}", e.what(), FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    if (count < Constant::kDiskannMinCount) {
      std::string s = fmt::format("diskann import total vector count is : {}  less than : {}, not support build. {}",
                                  count, Constant::kDiskannMinCount, FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EDISKANN_IMPORT_COUNT_TOO_FEW, s);
    }

    if (static_cast<uint32_t>(dim) != diskann_parameter.dimension()) {
      std::string s = fmt::format("dimension not match : {} {}", static_cast<int>(dim), diskann_parameter.dimension());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    uint32_t max_degree = diskann_parameter.max_degree();
    uint32_t search_list_size = diskann_parameter.search_list_size();
    float search_dram_budget_gb = search_dram_budget_gb_;
    float build_dram_budget_gb = build_dram_budget_gb_;

    uint32_t disk_pq = diskann_parameter.pq_disk_bytes();
    disk_pq = 0;

    bool append_reorder_data = diskann_parameter.append_reorder_data();
    append_reorder_data = false;

    uint32_t build_pq = diskann_parameter.build_pq_bytes();
    build_pq = 0;

    uint32_t qd = diskann_parameter.qd();
    qd = 0;

    std::string params = std::string(std::to_string(max_degree)) + " " + std::string(std::to_string(search_list_size)) +
                         " " + std::string(std::to_string(search_dram_budget_gb)) + " " +
                         std::string(std::to_string(build_dram_budget_gb)) + " " +
                         std::string(std::to_string(num_threads_)) + " " + std::string(std::to_string(disk_pq)) + " " +
                         std::string(std::to_string(append_reorder_data)) + " " +
                         std::string(std::to_string(build_pq)) + " " + std::string(std::to_string(qd));

    if (metric_type == pb::common::MetricType::METRIC_TYPE_L2)
      metric = diskann::Metric::L2;
    else if (metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT)
      metric = diskann::Metric::INNER_PRODUCT;
    else if (metric_type == pb::common::MetricType::METRIC_TYPE_COSINE)
      metric = diskann::Metric::COSINE;
    else {
      std::string s = fmt::format("diskann metric_type not support : {} {}", static_cast<int>(metric_type),
                                  pb::common::MetricType_Name(metric_type));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    bool use_opq = diskann_parameter.use_opq();
    use_opq = false;

    std::string codebook_prefix = diskann_parameter.codebook_prefix();
    codebook_prefix = "";

    std::string label_file;
    bool use_filters = (label_file != "");

    std::string universal_label;

    uint32_t filter_threshold = 0;

    uint32_t lf = 0;

    try {
      int ret = diskann::build_disk_index<float>(data_path_.c_str(), index_path_prefix_.c_str(), params.c_str(), metric,
                                                 use_opq, codebook_prefix, use_filters, label_file, universal_label,
                                                 filter_threshold, lf);
      if (ret != 0) {
        std::string s = fmt::format("diskann build failed {}", FormatParameter());
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }

    } catch (const std::exception& e) {
      std::string s = fmt::format("diskann build exception : {} {}", e.what(), FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    status = DiskANNUtils::DiskANNIndexPathPrefixExists(index_path_prefix_, build_with_mem_index, metric_type);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    is_error_occur = false;
  }  // end build

  // finish build
  {
    RWLockWriteGuard guard(&rw_lock_);
    count_ = static_cast<uint32_t>(count);
    dimension_ = static_cast<uint32_t>(dim);

    metric_type_ = metric_type;
    metric_ = metric;
    build_with_mem_index_ = build_with_mem_index;
    is_built_ = true;
    state_.store(DiskANNCoreState::kBuilded);
    state = this->state_.load();
  }

  DINGO_LOG(INFO) << "build_disk_index success :  " << FormatParameter();

  return butil::Status::OK();
}

butil::Status DiskANNCore::DoLoad(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState old_state,
                                  DiskANNCoreState& state, bool is_try_load) {
  std::shared_ptr<AlignedFileReader> reader;
  std::unique_ptr<diskann::PQFlashIndex<float>> flash_index;
  uint32_t num_nodes_to_cache = 0;
  bool warmup = true;
  std::string index_path_prefix;

  // for prepare try load
  diskann::Metric metric;
  auto diskann_parameter = vector_index_parameter_.diskann_parameter();
  pb::common::MetricType metric_type = diskann_parameter.metric_type();
  size_t count = 0;
  size_t dim = 0;
  bool build_with_mem_index = false;

  {
    bool is_error_occur = true;
    auto lambda_set_state_function = [&is_error_occur, &state, this, old_state]() {
      if (is_error_occur) {
        RWLockWriteGuard guard(&rw_lock_);
        state_.store(old_state);
        state = state_.load();
      }
    };

    ON_SCOPE_EXIT(lambda_set_state_function);
    if (is_try_load) {
      butil::Status status = DoPrepareTryLoad(diskann_parameter, metric, metric_type, count, dim, build_with_mem_index);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    } else {
      count = count_;
      metric_type = metric_type_;
      dim = dimension_;
    }

    // garbage diskann interface. I modify diskann interface.
    reader = std::make_shared<LinuxAlignedFileReader>();
    flash_index = std::make_unique<diskann::PQFlashIndex<float>>(reader, metric_);

    try {
      index_path_prefix = index_path_prefix_;
      if (!index_path_prefix.empty()) {
        if (index_path_prefix.back() != '/') {
          index_path_prefix += "/";
        }
      }

      // diskann/src/linux_aligned_file_reader.cpp #define MAX_EVENTS 1024
      std::atomic<int64_t> this_aio_wait_count = ++aio_wait_count;
      butil::Status status = DiskANNUtils::CheckAioRelatedInformation(num_threads_, 1024, this_aio_wait_count);
      if (!status.ok()) {
        aio_wait_count--;
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      int res = flash_index->load(num_threads_, index_path_prefix.c_str());
      if (res != 0) {
        aio_wait_count--;
        std::string s = fmt::format("load diskann failed ret : {} {}", res, FormatParameter());
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
    } catch (const std::exception& e) {
      aio_wait_count--;
      std::string s = fmt::format("load diskann exception : {} {}", e.what(), FormatParameter());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    aio_wait_count--;

    if (count != flash_index->get_num_points()) {
      std::string s = fmt::format("count not match : file :{} load :{}", count, flash_index->get_num_points());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    if (metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
      if (dim != (flash_index->get_data_dim() - 1)) {
        std::string s = fmt::format("dimension_ not match : dim :{} load :{}", dim, flash_index->get_data_dim());
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
    } else {
      if (dim != flash_index->get_data_dim()) {
        std::string s = fmt::format("dimension_ not match : dim :{} load :{}", dim, flash_index->get_data_dim());
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
    }

    num_nodes_to_cache = load_param.num_nodes_to_cache();
    warmup = load_param.warmup();

    std::vector<uint32_t> node_list;
    DINGO_LOG(INFO) << "Caching " << num_nodes_to_cache << " nodes around medoid(s)" << std::endl;
    flash_index->cache_bfs_levels(num_nodes_to_cache, node_list);

    flash_index->load_cache_list(node_list);
    node_list.clear();
    node_list.shrink_to_fit();

    omp_set_num_threads(num_threads_);

    uint64_t warmup_l = 20;
    uint64_t warmup_num = 0, warmup_dim = 0, warmup_aligned_dim = 0;
    float* warmup_data = nullptr;

    if (warmup) {
      ON_SCOPE_EXIT([&warmup_data]() {
        if (warmup_data != nullptr) {
          diskann::aligned_free(warmup_data);
          warmup_data = nullptr;
        }
      });
      std::string warmup_query_file = index_path_prefix + "_sample_data.bin";
      if (file_exists(warmup_query_file)) {
        diskann::load_aligned_bin<float>(warmup_query_file, warmup_data, warmup_num, warmup_dim, warmup_aligned_dim);
      } else {
        warmup_num = (std::min)((uint32_t)150000, (uint32_t)15000 * num_threads_);
        warmup_dim = dim;
        warmup_aligned_dim = ROUND_UP(warmup_dim, 8);
        diskann::alloc_aligned(((void**)&warmup_data), warmup_num * warmup_aligned_dim * sizeof(float),
                               8 * sizeof(float));
        std::memset(warmup_data, 0, warmup_num * warmup_aligned_dim * sizeof(float));
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(-128, 127);
        for (uint32_t i = 0; i < warmup_num; i++) {
          for (uint32_t d = 0; d < warmup_dim; d++) {
            warmup_data[i * warmup_aligned_dim + d] = (float)dis(gen);
          }
        }
      }
      DINGO_LOG(INFO) << "Warming up index... " << std::flush;
      std::vector<uint64_t> warmup_result_ids_64(warmup_num, 0);
      std::vector<float> warmup_result_dists(warmup_num, 0);

#pragma omp parallel for schedule(dynamic, 1)
      for (int64_t i = 0; i < (int64_t)warmup_num; i++) {
        flash_index->cached_beam_search(warmup_data + (i * warmup_aligned_dim), 1, warmup_l,
                                        warmup_result_ids_64.data() + (i * 1), warmup_result_dists.data() + (i * 1), 4);
      }

      DINGO_LOG(INFO) << "Warming up ..done" << std::endl;
    }

    is_error_occur = false;
  }

  RWLockWriteGuard guard(&rw_lock_);
  if (is_try_load) {
    count_ = static_cast<uint32_t>(count);
    dimension_ = static_cast<uint32_t>(dim);
    metric_type_ = metric_type;
    metric_ = metric;
    build_with_mem_index_ = build_with_mem_index;
    is_update_ = true;
    is_built_ = true;
  }
  reader_ = reader;
  flash_index_ = std::move(flash_index);
  num_nodes_to_cache_ = num_nodes_to_cache;
  warmup_ = warmup;
  state_.store(DiskANNCoreState::kLoaded);
  state = this->state_.load();
  index_path_prefix_ = index_path_prefix;
  is_load_ = true;

  DINGO_LOG(INFO) << "load success :  " << FormatParameter();

  return butil::Status::OK();
}

butil::Status DiskANNCore::DoPrepareTryLoad(const pb::common::CreateDiskAnnParam& diskann_parameter,
                                            diskann::Metric& metric, pb::common::MetricType& metric_type, size_t& count,
                                            size_t& dim, bool& build_with_mem_index) {
  auto value_type = diskann_parameter.value_type();
  if (value_type == pb::common::ValueType::INT8_T) {
    std::string s = "diskann value_type not support int8";
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
  } else if (value_type == pb::common::ValueType::UINT8) {
    std::string s = "diskann value_type not support uint8";
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
  }

  // check data_path is exist and regular file.
  butil::Status status = DiskANNUtils::FileExistsAndRegular(data_path_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  try {
    diskann::get_bin_metadata(data_path_, count, dim);
  } catch (const std::exception& e) {
    std::string s = fmt::format("get_bin_metadata exception : {} {}", e.what(), FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (count < Constant::kDiskannMinCount) {
    std::string s = fmt::format("diskann import total vector count is : {}  less than : {}, not support build. {}",
                                count, Constant::kDiskannMinCount, FormatParameter());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EDISKANN_IMPORT_COUNT_TOO_FEW, s);
  }

  if (static_cast<uint32_t>(dim) != diskann_parameter.dimension()) {
    std::string s = fmt::format("dimension not match : {} {}", static_cast<int>(dim), diskann_parameter.dimension());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (metric_type == pb::common::MetricType::METRIC_TYPE_L2)
    metric = diskann::Metric::L2;
  else if (metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT)
    metric = diskann::Metric::INNER_PRODUCT;
  else if (metric_type == pb::common::MetricType::METRIC_TYPE_COSINE)
    metric = diskann::Metric::COSINE;
  else {
    std::string s = fmt::format("diskann metric_type not support : {} {}", static_cast<int>(metric_type),
                                pb::common::MetricType_Name(metric_type));
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  status = DiskANNUtils::DiskANNIndexPathPrefixExists(index_path_prefix_, build_with_mem_index, metric_type);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status DiskANNCore::FillSearchResult(uint32_t topk, const std::vector<std::vector<float>>& distances,
                                            const std::vector<std::vector<uint64_t>>& labels,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) {
  for (size_t row = 0; row < labels.size(); ++row) {
    pb::index::VectorWithDistanceResult& result = results.emplace_back();

    for (size_t i = 0; i < topk; i++) {
      size_t pos = labels[row][i];
      if (pos > std::numeric_limits<uint32_t>::max()) {
        continue;
      }

      auto* vector_with_distance = result.add_vector_with_distances();

      auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
      vector_with_id->set_id(pos);
      vector_with_id->mutable_vector()->set_dimension(dimension_);
      vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      if (metric_type_ == pb::common::MetricType::METRIC_TYPE_COSINE ||
          metric_type_ == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
        vector_with_distance->set_distance(1.0F - distances[row][i]);
      } else {
        vector_with_distance->set_distance(distances[row][i]);
      }

      vector_with_distance->set_metric_type(metric_type_);
    }

    // sort by distance
    std::sort(result.mutable_vector_with_distances()->begin(), result.mutable_vector_with_distances()->end(),
              [](const ::dingodb::pb::common::VectorWithDistance& a,
                 const ::dingodb::pb::common::VectorWithDistance& b) { return a.distance() < b.distance(); });
  }

  return butil::Status::OK();
}

std::string DiskANNCore::FormatParameter() {
  const auto& diskann_parameter = vector_index_parameter_.diskann_parameter();
  std::string s = fmt::format(
      " vector_index_id:{} data_path:\"{}\" index_path_prefix:\"{}\" search_dram_budget_gb:{}G build_dram_budget_gb:"
      "{}G num_threads:{}  is_build:{} count:{}  dimension:{}  build_with_mem_index:{} is_update:{} "
      "is_load:{} metric_type:{} metric:{}  reader:\"{}\" flash_index:\"{}\" num_nodes_to_cache:{}  warmup"
      ":{}  state:\"{}\" "
      "diskann_parameter"
      ":\"{}\"",
      vector_index_id_, data_path_, index_path_prefix_, search_dram_budget_gb_, build_dram_budget_gb_, num_threads_,
      (is_built_ ? "true" : "false"), count_, dimension_, (build_with_mem_index_ ? "true" : "false"),
      (is_update_ ? "true" : "false"), (is_load_ ? "true" : "false"), pb::common::MetricType_Name(metric_type_),
      static_cast<int>(metric_), (nullptr != reader_ ? "not nullptr" : "nullptr"),
      (nullptr != flash_index_ ? "not nullptr" : "nullptr"), num_nodes_to_cache_, (warmup_ ? "true" : "false"),
      DiskANNUtils::DiskANNCoreStateToString(state_), diskann_parameter.DebugString());

  return s;
}

}  // namespace dingodb
