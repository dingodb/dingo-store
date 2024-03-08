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

#ifndef DINGODB_SDK_VECTOR_SEARCH_TATSK_H_
#define DINGODB_SDK_VECTOR_SEARCH_TATSK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "fmt/core.h"
#include "sdk/client_stub.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorSearchPartTask;
class VectorSearchTask : public VectorTask {
 public:
  VectorSearchTask(const ClientStub& stub, int64_t index_id, const SearchParam& search_param,
                   const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result)
      : VectorTask(stub),
        index_id_(index_id),
        search_param_(search_param),
        target_vectors_(target_vectors),
        out_result_(out_result) {}

  ~VectorSearchTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorSearchTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorSearchPartTask* sub_task);

  void ConstructResultUnlocked();

  const int64_t index_id_;
  const SearchParam& search_param_;
  const std::vector<VectorWithId>& target_vectors_;

  // target_vectors_ idx to search result
  std::unordered_map<int64_t, std::vector<VectorWithDistance>> tmp_out_result_;

  std::vector<SearchResult>& out_result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorSearchPartTask : public VectorTask {
 public:
  VectorSearchPartTask(const ClientStub& stub, int64_t index_id, int64_t part_id, const SearchParam& search_param,
                       const std::vector<VectorWithId>& target_vectors)
      : VectorTask(stub),
        index_id_(index_id),
        part_id_(part_id),
        search_param_(search_param),
        target_vectors_(target_vectors) {}

  ~VectorSearchPartTask() override = default;

  std::unordered_map<int64_t, std::vector<VectorWithDistance>>& GetSearchResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return search_result_;
  }

 private:
  friend class VectorSearchTask;

  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorSearchPartTask-{}-{}", index_id_, part_id_); }

  void FillVectorSearchRpcRequest(pb::index::VectorSearchRequest* request, const std::shared_ptr<Region>& region);

  void VectorSearchRpcCallback(const Status& status, VectorSearchRpc* rpc);

  const int64_t index_id_;
  const int64_t part_id_;
  const SearchParam& search_param_;
  const std::vector<VectorWithId>& target_vectors_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorSearchRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;
  // target_vectors_ idx to search result
  std::unordered_map<int64_t, std::vector<VectorWithDistance>> search_result_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_SEARCH_TATSK_H_