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

#ifndef DINGODB_SDK_RAW_KV_DELETE_RANGE_TASK_H_
#define DINGODB_SDK_RAW_KV_DELETE_RANGE_TASK_H_

#include <atomic>
#include <cstdint>

#include "sdk/client_stub.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

class RawKvDeleteRangeTask : public RawKvTask {
 public:
  RawKvDeleteRangeTask(const ClientStub& stub, const std::string& start_key, const std::string& end_key,
                       bool continuous, int64_t& out_delete_count);

  ~RawKvDeleteRangeTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  void PostProcess() override;
  void DeleteNextRange();
  void KvDeleteRangeRpcCallback(Status status, KvDeleteRangeRpc* rpc, StoreRpcController* controller);

  std::string Name() const override { return "RawKvDeleteRangeTask"; }
  std::string ErrorMsg() const override { return fmt::format("start_key: {}, end_key:{}", start_key_, end_key_); }

  const std::string& start_key_;
  const std::string& end_key_;
  const bool continuous_;
  int64_t& out_delete_count_;

  Status status_;
  std::atomic<int64_t> tmp_out_delete_count_;
  std::string next_start_key_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RAW_KV_DELETE_RANGE_TASK_H_