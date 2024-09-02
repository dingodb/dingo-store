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

#ifndef DINGODB_DISKANN_DISKANN_SERVICE_HANDLE_H_
#define DINGODB_DISKANN_DISKANN_SERVICE_HANDLE_H_

#include <memory>

#include "common/context.h"
#include "diskann/diskann_item_manager.h"

namespace dingodb {

class DiskAnnServiceHandle {
 public:
  explicit DiskAnnServiceHandle() = default;
  ~DiskAnnServiceHandle() = default;
  static butil::Status VectorNew(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                 const pb::common::VectorIndexParameter& vector_index_parameter);

  static butil::Status VectorPushData(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                      const std::vector<pb::common::Vector>& vectors,
                                      const std::vector<int64_t>& vector_ids, bool has_more, pb::error::Errno error,
                                      bool force_to_load_data_if_exist, int64_t ts, int64_t tso,
                                      int64_t already_send_vector_count);

  static butil::Status VectorBuild(std::shared_ptr<Context> ctx, int64_t vector_index_id, bool force_to_build_if_exist);

  static butil::Status VectorLoad(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                  const pb::common::LoadDiskAnnParam& load_param);

  static butil::Status VectorTryLoad(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                     const pb::common::LoadDiskAnnParam& load_param);

  static butil::Status VectorSearch(std::shared_ptr<Context> ctx, int64_t vector_index_id, uint32_t top_n,
                                    const pb::common::SearchDiskAnnParam& search_param,
                                    const std::vector<pb::common::Vector>& vectors);

  static butil::Status VectorReset(std::shared_ptr<Context> ctx, int64_t vector_index_id, bool delete_data_file);

  static butil::Status VectorClose(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorDestroy(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorStatus(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorCount(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorSetNoData(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorDump(std::shared_ptr<Context> ctx, int64_t vector_index_id);

  static butil::Status VectorDumpAll(std::shared_ptr<Context> ctx);

 private:
  inline static DiskANNItemManager& item_manager = DiskANNItemManager::GetInstance();
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_SERVICE_HANDLE_H_