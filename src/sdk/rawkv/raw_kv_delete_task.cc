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

#include "sdk/rawkv/raw_kv_delete_task.h"

#include "sdk/common/common.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RawKvDeleteTask::RawKvDeleteTask(const ClientStub& stub, const std::string& key)
    : RawKvTask(stub), key_(key), store_rpc_controller_(stub, rpc_) {}

void RawKvDeleteTask::DoAsync() {
  std::shared_ptr<MetaCache> meta_cache = stub.GetMetaCache();
  std::shared_ptr<Region> region;
  Status s = meta_cache->LookupRegionByKey(key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  rpc_.MutableRequest()->Clear();
  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  auto* fill = rpc_.MutableRequest()->add_keys();
  *fill = key_;

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { KvDeleteRpcCallback(std::forward<decltype(s)>(s)); });
}

void RawKvDeleteTask::KvDeleteRpcCallback(const Status& status) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc_.Method() << " send to region: " << rpc_.Request()->context().region_id()
                       << " fail: " << status.ToString();
  }

  DoAsyncDone(status);
}

}  // namespace sdk

}  // namespace dingodb