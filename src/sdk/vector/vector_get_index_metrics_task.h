
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

#ifndef DINGODB_SDK_VECTOR_GET_INDEX_METRICS_TASK_H_
#define DINGODB_SDK_VECTOR_GET_INDEX_METRICS_TASK_H_

#include <cstdint>
#include <unordered_map>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/coordinator.pb.h"
#include "sdk/client_stub.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_task.h"
namespace dingodb {
namespace sdk {

static void MergeIndexMetricsResult(const IndexMetricsResult& from, IndexMetricsResult& to) {
  CHECK_EQ(from.index_type, to.index_type);
  to.count += from.count;
  to.deleted_count += from.deleted_count;
  to.max_vector_id = std::max(to.max_vector_id, from.max_vector_id);
  if (from.min_vector_id != 0) {
    to.min_vector_id = std::min(to.min_vector_id, from.min_vector_id);
  }
  to.memory_bytes += from.memory_bytes;
}

static IndexMetricsResult CreateIndexMetricsResult() {
  IndexMetricsResult to_return;
  to_return.min_vector_id = INT64_MAX;
  to_return.max_vector_id = INT64_MIN;
  return to_return;
}

class VectorGetIndexMetricsPartTask;
class VectorGetIndexMetricsTask : public VectorTask {
 public:
  VectorGetIndexMetricsTask(const ClientStub& stub, int64_t index_id, IndexMetricsResult& out_result)
      : VectorTask(stub), index_id_(index_id), out_result_(out_result), tmp_result_(CreateIndexMetricsResult()) {}

  ~VectorGetIndexMetricsTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorGetIndexMetricsTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorGetIndexMetricsPartTask* sub_task);

  const int64_t index_id_;
  IndexMetricsResult& out_result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;
  IndexMetricsResult tmp_result_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorGetIndexMetricsPartTask : public VectorTask {
 public:
  VectorGetIndexMetricsPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorGetIndexMetricsPartTask() override = default;

  IndexMetricsResult GetResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    IndexMetricsResult to_return = CreateIndexMetricsResult();
    to_return.index_type = vector_index_->GetVectorIndexType();
    for (auto& [region_id, metrics] : region_id_to_metrics_) {
      MergeIndexMetricsResult(metrics, to_return);
    }

    return to_return;
  }

 private:
  friend class VectorGetIndexMetricsTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorGetIndexMetricsPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorGetRegionMetricsRpcCallback(const Status& status, VectorGetRegionMetricsRpc* rpc);

  const std::shared_ptr<VectorIndex> vector_index_;
  const int64_t part_id_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorGetRegionMetricsRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;
  std::unordered_map<int64_t, IndexMetricsResult> region_id_to_metrics_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_GET_INDEX_METRICS_TASK_H_
