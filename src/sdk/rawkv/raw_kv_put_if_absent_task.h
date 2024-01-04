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

#ifndef DINGODB_SDK_RAW_KV_PUT_IF_ABSENT_TASK_H_
#define DINGODB_SDK_RAW_KV_PUT_IF_ABSENT_TASK_H_

#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

class RawKvPutIfAbsentTask : public RawKvTask {
 public:
  RawKvPutIfAbsentTask(const ClientStub& stub, const std::string& key, const std::string& value, bool& out_state);

  ~RawKvPutIfAbsentTask() override = default;

 private:
  void DoAsync() override;

  void KvPutIfAbsentRpcCallback(const Status& status);

  std::string Name() const override { return "RawKvPutIfAbsentTask"; }
  std::string ErrorMsg() const override { return fmt::format("key: {}", key_); }

  const std::string& key_;
  const std::string& value_;
  bool& out_state_;
  KvPutIfAbsentRpc rpc_;
  StoreRpcController store_rpc_controller_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_RAW_KV_PUT_IF_ABSENT_TASK_H_