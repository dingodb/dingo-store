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

#include "sdk/rawkv/raw_kv_get_task.h"

#include "sdk/common/common.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"

namespace dingodb {
namespace sdk {

RawKvGetTask::RawKvGetTask(const ClientStub& stub, const std::string& key, std::string& out_value)
    : RawKvTask(stub), key_(key), out_value_(out_value), store_rpc_controller_(stub, rpc_) {}

void RawKvGetTask::DoAsync() {
  std::shared_ptr<MetaCache> meta_cache = stub.GetMetaCache();
  std::shared_ptr<Region> region;
  Status s = meta_cache->LookupRegionByKey(key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  rpc_.MutableRequest()->Clear();
  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  rpc_.MutableRequest()->set_key(key_);

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { KvGetRpcCallback(std::forward<decltype(s)>(s)); });
}

void RawKvGetTask::KvGetRpcCallback(Status status) {
  if (status.ok()) {
    result_ = rpc_.Response()->value();
  }

  DoAsyncDone(status);
}

void RawKvGetTask::PostProcess() {
  if (!result_.empty()) {
    out_value_ = std::move(result_);
  }
}

}  // namespace sdk
}  // namespace dingodb