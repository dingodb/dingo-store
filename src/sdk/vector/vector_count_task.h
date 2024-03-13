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

#ifndef DINGODB_SDK_VECTOR_COUNT_TASK_H_
#define DINGODB_SDK_VECTOR_COUNT_TASK_H_

#include <atomic>
#include <cstdint>

#include "sdk/client_stub.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorCountPartTask;

class VectorCountTask : public VectorTask {
 public:
  VectorCountTask(const ClientStub& stub, int64_t index_id, int64_t start_vector_id, int64_t end_vector_id,
                  int64_t& out_count)
      : VectorTask(stub),
        index_id_(index_id),
        start_vector_id_(start_vector_id),
        end_vector_id_(end_vector_id),
        out_count_(out_count) {}

  ~VectorCountTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorCountTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorCountPartTask* sub_task);

  void ConstructResultUnlocked();

  const int64_t index_id_;
  const int64_t start_vector_id_;
  const int64_t end_vector_id_;
  int64_t& out_count_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::atomic<int64_t> tmp_count_{0};

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorCountPartTask : public VectorTask {
 public:
  VectorCountPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id,
                      int64_t start_vector_id, int64_t end_vector_id)
      : VectorTask(stub),
        vector_index_(vector_index),
        part_id_(part_id),
        start_vector_id_(start_vector_id),
        end_vector_id_(end_vector_id) {}

  ~VectorCountPartTask() override = default;

  int64_t GetResult() { return ret_count_.load(); }

 private:
  friend class VectorCountTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorCountPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorCountRpcCallback(Status status, VectorCountRpc* rpc);

  const std::shared_ptr<VectorIndex> vector_index_;
  const int64_t part_id_;
  const int64_t start_vector_id_;
  const int64_t end_vector_id_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorCountRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int64_t> ret_count_{0};
  std::atomic<int> sub_tasks_count_{0};
};
}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_COUNT_TASK_H_