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

#ifndef DINGODB_SDK_VECTOR_ADD_TASK_H_
#define DINGODB_SDK_VECTOR_ADD_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorAddTask : public VectorTask {
 public:
  VectorAddTask(const ClientStub& stub, int64_t index_id, std::vector<VectorWithId>& vectors,
                bool replace_deleted = false, bool is_update = false)
      : VectorTask(stub),
        index_id_(index_id),
        vectors_(vectors),
        replace_deleted_(replace_deleted),
        is_update_(is_update) {}

  ~VectorAddTask() override = default;

  Status TEST_Init() { return Init(); } 

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorAddTask-{}", index_id_); }

  void VectorAddRpcCallback(const Status& status, VectorAddRpc* rpc);

  const int64_t index_id_;
  std::vector<VectorWithId>& vectors_;
  const bool replace_deleted_;
  const bool is_update_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorAddRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::unordered_map<int64_t, int64_t> vector_id_to_idx_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};
}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_ADD_TASK_H_