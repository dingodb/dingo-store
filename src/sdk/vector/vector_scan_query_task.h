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

#ifndef DINGODB_SDK_VECTOR_SCAN_QUERY_TATSK_H_
#define DINGODB_SDK_VECTOR_SCAN_QUERY_TATSK_H_

#include <cstdint>

#include "proto/index.pb.h"
#include "sdk/client_stub.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/vector.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {

namespace sdk {

class VectorScanQueryPartTask;

class VectorScanQueryTask : public VectorTask {
 public:
  VectorScanQueryTask(const ClientStub& stub, int64_t index_id, const ScanQueryParam& query_param,
                      ScanQueryResult& out_result)
      : VectorTask(stub), index_id_(index_id), scan_query_param_(query_param), out_result_(out_result) {}

  ~VectorScanQueryTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorScanQueryTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorScanQueryPartTask* sub_task);

  void ConstructResultUnlocked();

  const int64_t index_id_;
  const ScanQueryParam& scan_query_param_;
  ScanQueryResult& out_result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::vector<VectorWithId> result_vectors_;
  std::set<int64_t> vector_ids_; // for unique check
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorScanQueryPartTask : public VectorTask {
 public:
  VectorScanQueryPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id,
                          const ScanQueryParam& query_param)
      : VectorTask(stub), vector_index_(vector_index), part_id_(part_id), scan_query_param_(query_param) {}

  ~VectorScanQueryPartTask() override = default;

  std::vector<VectorWithId> GetResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return result_vectors_;
  }

 private:
  friend class VectorScanQueryTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorScanQueryPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void FillVectorScanQueryRpcRequest(pb::index::VectorScanQueryRequest* request, const std::shared_ptr<Region>& region);

  void VectorScanQueryRpcCallback(Status status, VectorScanQueryRpc* rpc);

  const std::shared_ptr<VectorIndex> vector_index_;
  const int64_t part_id_;
  const ScanQueryParam& scan_query_param_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorScanQueryRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::vector<VectorWithId> result_vectors_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_SCAN_QUERY_TATSK_H_
