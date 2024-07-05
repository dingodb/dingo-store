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

#ifndef DINGODB_DISKANN_DISKANN_ITEM_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_ITEM_H_

#include <sys/types.h>
#include <xmmintrin.h>

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "common/synchronization.h"
#include "diskann/diskann_core.h"
#include "diskann/diskann_utils.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

#ifndef ENABLE_DISKANN_ID_MAPPING
#define ENABLE_DISKANN_ID_MAPPING
#endif

// #undef ENABLE_DISKANN_ID_MAPPING

class DiskANNItem {
 public:
  explicit DiskANNItem(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                       const pb::common::VectorIndexParameter& vector_index_parameter, u_int32_t num_threads,
                       float search_dram_budget_gb, float build_dram_budget_gb);

  ~DiskANNItem();

  DiskANNItem(const DiskANNItem& rhs) = delete;
  DiskANNItem& operator=(const DiskANNItem& rhs) = delete;
  DiskANNItem(DiskANNItem&& rhs) = delete;
  DiskANNItem& operator=(DiskANNItem&& rhs) = delete;

  butil::Status Import(std::shared_ptr<Context> ctx, const std::vector<pb::common::Vector>& vectors,
                       const std::vector<int64_t>& vector_ids, bool has_more, bool force_to_load_data_if_exist,
                       int64_t already_send_vector_count, int64_t ts, int64_t tso, int64_t& already_recv_vector_count);
  butil::Status Build(std::shared_ptr<Context> ctx, bool force_to_build, bool is_sync);
  butil::Status Load(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param, bool is_sync);
  butil::Status Search(std::shared_ptr<Context> ctx, uint32_t top_n, const pb::common::SearchDiskAnnParam& search_param,
                       const std::vector<pb::common::Vector>& vectors,
                       std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& ts);
  butil::Status TryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param, bool is_sync);
  butil::Status Close(std::shared_ptr<Context> ctx);
  butil::Status Destroy(std::shared_ptr<Context> ctx);
  DiskANNCoreState Status(std::shared_ptr<Context> ctx);
  std::string Dump(std::shared_ptr<Context> ctx);
  butil::Status Count(std::shared_ptr<Context> ctx, int64_t& count);

  static void SetImportTimeout(int64_t timeout_s) { DiskANNItem::import_timeout_s = timeout_s; }
  static void SetBaseDir(const std::string& base) { DiskANNItem::base_dir = base; }
  static bool IsBuildedFilesExist(int64_t vector_index_id, pb::common::MetricType metric_type);

 protected:
 private:
  butil::Status DoImport(const std::vector<pb::common::Vector>& vectors, const std::vector<int64_t>& vector_ids,
                         bool has_more, int64_t already_send_vector_count, int64_t ts, int64_t tso,
                         int64_t& already_recv_vector_count, DiskANNCoreState& old_state);
  butil::Status DoSyncBuild(std::shared_ptr<Context> ctx, bool force_to_build, DiskANNCoreState old_state);
  butil::Status DoAsyncBuild(std::shared_ptr<Context> ctx, bool force_to_build, DiskANNCoreState old_state);
  butil::Status DoBuildInternal(std::shared_ptr<Context> ctx, bool force_to_build, DiskANNCoreState old_state);
  butil::Status DoSyncLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                           DiskANNCoreState old_state);
  butil::Status DoAsyncLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                            DiskANNCoreState old_state);
  butil::Status DoLoadInternal(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                               DiskANNCoreState old_state);
  butil::Status DoSyncTryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                              DiskANNCoreState old_state);
  butil::Status DoAsyncTryLoad(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                               DiskANNCoreState old_state);
  butil::Status DoTryLoadInternal(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                                  DiskANNCoreState old_state);
  butil::Status DoClose(std::shared_ptr<Context> ctx, bool is_destroy);
  std::string FormatParameter();
  std::string MiniFormatParameter();
  void SetSide(std::shared_ptr<Context> ctx);
  int64_t vector_index_id_;
  pb::common::VectorIndexParameter vector_index_parameter_;
  uint32_t num_threads_;
  float search_dram_budget_gb_;
  float build_dram_budget_gb_;
  std::string data_path_;
  std::string index_path_prefix_;
  bool is_import_;
  std::atomic<DiskANNCoreState> state_;
  std::ofstream writer_;
  int64_t already_recv_vector_count_;
  std::shared_ptr<DiskANNCore> diskann_core_;
#if defined(ENABLE_DISKANN_ID_MAPPING)
  std::vector<int64_t> diskann_to_vector_ids_;
  std::unordered_map<int64_t, uint32_t> vector_to_diskann_ids_;
  std::ofstream id_writer_;
  std::string id_path_;
#endif
  int64_t ts_;
  int64_t tso_;
  butil::Status last_error_;
  std::string remote_side_;
  std::string local_side_;
  std::string error_remote_side_;
  std::string error_local_side_;
  int64_t last_import_time_ms_;  // millisecond timestamp
  RWLock rw_lock_;

  static inline std::string base_dir = "/opt/data/diskann";
  static inline std::string tmp_name = "tmp";
  static inline std::string normal_name = "normal";
  static inline std::string destroyed_name = "destroyed";
  static inline std::string input_name = "data.bin";
  static inline std::string build_name = "index";

#if defined(ENABLE_DISKANN_ID_MAPPING)
  static inline std::string id_name = "id.bin";
#endif
  static inline int64_t import_timeout_s = 30;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_ITEM_H_  // NOLINT
