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

#ifndef DINGODB_DISKANN_DISKANN_CORE_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_CORE_H_

#include <sys/types.h>
#include <xmmintrin.h>

#include <cstdint>
#include <string>

#include "butil/status.h"
#include "common/synchronization.h"
#include "diskann/diskann_utils.h"
#include "pq_flash_index.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

using ExistFunction = bool(int64_t, pb::common::MetricType);

class DiskANNCore {
 public:
  explicit DiskANNCore(int64_t vector_index_id, const pb::common::VectorIndexParameter& vector_index_parameter,
                       u_int32_t num_threads, float search_dram_budget_gb, float build_dram_budget_gb,
                       const std::string& data_path, const std::string& index_path_prefix);

  ~DiskANNCore();

  DiskANNCore(const DiskANNCore& rhs) = delete;
  DiskANNCore& operator=(const DiskANNCore& rhs) = delete;
  DiskANNCore(DiskANNCore&& rhs) = delete;
  DiskANNCore& operator=(DiskANNCore&& rhs) = delete;

  butil::Status Build(bool force_to_build, DiskANNCoreState& state);
  butil::Status UpdateIndexPathPrefix(const std::string& index_path_prefix, DiskANNCoreState& state);
  butil::Status Load(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState& state);
  butil::Status Search(uint32_t top_n, const pb::common::SearchDiskAnnParam& search_param,
                       const std::vector<pb::common::Vector>& vectors, ExistFunction exist_func,
                       std::vector<pb::index::VectorWithDistanceResult>& results, DiskANNCoreState& state);
  butil::Status Reset(bool is_delete_files, DiskANNCoreState& state, bool is_force = false);
  butil::Status Init(int64_t vector_index_id, const pb::common::VectorIndexParameter& vector_index_parameter,
                     u_int32_t num_threads, float search_dram_budget_gb, float build_dram_budget_gb,
                     const std::string& data_path, const std::string& index_path_prefix);
  butil::Status TryLoad(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState& state);
  DiskANNCoreState Status();
  bool IsBuilt();
  bool IsUpdate();
  bool IsLoad();
  std::string Dump();
  butil::Status Count(int64_t& count, DiskANNCoreState& state);

 protected:
 private:
  butil::Status DoBuild(DiskANNCoreState& state, DiskANNCoreState old_state);
  butil::Status DoLoad(const pb::common::LoadDiskAnnParam& load_param, DiskANNCoreState old_state,
                       DiskANNCoreState& state, bool is_try_load);
  butil::Status DoPrepareTryLoad(const pb::common::CreateDiskAnnParam& diskann_parameter, diskann::Metric& metric,
                                 const pb::common::MetricType& metric_type, size_t& count, size_t& dim,
                                 bool& build_with_mem_index);
  butil::Status FillSearchResult(uint32_t topk, const std::vector<std::vector<float>>& distances,
                                 const std::vector<std::vector<uint64_t>>& labels,
                                 std::vector<pb::index::VectorWithDistanceResult>& results);
  std::string FormatParameter();

  int64_t vector_index_id_;
  pb::common::VectorIndexParameter vector_index_parameter_;
  uint32_t num_threads_;
  float search_dram_budget_gb_;
  float build_dram_budget_gb_;
  std::string data_path_;
  std::string index_path_prefix_;
  bool is_built_;
  uint32_t count_;
  uint32_t dimension_;
  bool build_with_mem_index_;
  bool is_update_;
  bool is_load_;
  pb::common::MetricType metric_type_;
  diskann::Metric metric_;
  std::shared_ptr<AlignedFileReader> reader_;
  std::unique_ptr<diskann::PQFlashIndex<float>> flash_index_;
  uint32_t num_nodes_to_cache_;
  bool warmup_;
  std::atomic<DiskANNCoreState> state_;
  RWLock rw_lock_;
  static inline std::atomic<int64_t> aio_wait_count = 0;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_CORE_H_  // NOLINT
