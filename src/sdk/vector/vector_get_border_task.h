
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

#ifndef DINGODB_SDK_VECTOR_GET_BORDER_TASK_H_
#define DINGODB_SDK_VECTOR_GET_BORDER_TASK_H_

#include <cstdint>

#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorGetBorderPartTask;
class VectorGetBorderTask : public VectorTask {
 public:
  VectorGetBorderTask(const ClientStub& stub, int64_t index_id, bool is_max, int64_t& out_vector_id)
      : VectorTask(stub), index_id_(index_id), is_max_(is_max), out_vector_id_(out_vector_id) {
    target_vector_id_ = is_max_ ? -1 : INT64_MAX;
  }

  ~VectorGetBorderTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorGetBorderTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorGetBorderPartTask* sub_task);

  const int64_t index_id_;
  const bool is_max_;
  int64_t& out_vector_id_;
  std::shared_ptr<VectorIndex> vector_index_;
  int64_t target_vector_id_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorGetBorderPartTask : public VectorTask {
 public:
  VectorGetBorderPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id,
                          bool is_max)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id), is_max_(is_max) {
    result_vector_id_ = is_max_ ? -1 : INT64_MAX;
  }

  ~VectorGetBorderPartTask() override = default;

  int64_t GetResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return result_vector_id_;
  }

 private:
  friend class VectorGetBorderTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorGetBorderPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorGetBorderIdRpcCallback(const Status& status, VectorGetBorderIdRpc* rpc);

  const std::shared_ptr<VectorIndex> vector_index_;
  const int64_t part_id_;
  const bool is_max_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorGetBorderIdRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;
  int64_t result_vector_id_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_GET_BORDER_TASK_H_