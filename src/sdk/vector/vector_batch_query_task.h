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

#ifndef DINGODB_SDK_VECTOR_BATCH_QUERY_TASK_H_
#define DINGODB_SDK_VECTOR_BATCH_QUERY_TASK_H_

#include <cstdint>
#include <vector>
#include <set>

#include "sdk/status.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorBatchQueryTask : public VectorTask {
 public:
  VectorBatchQueryTask(const ClientStub& stub, int64_t index_id, const QueryParam& query_param, QueryResult& out_result)
      : VectorTask(stub), index_id_(index_id), query_param_(query_param), out_result_(out_result) {}

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorBatchQueryTask-{}", index_id_); }

  void VectorBatchQueryRpcCallback(const Status& status, VectorBatchQueryRpc* rpc);

  const int64_t index_id_;
  const QueryParam query_param_;
  QueryResult& out_result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorBatchQueryRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> vector_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_BATCH_QUERY_TASK_H_