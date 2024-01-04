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

#include "sdk/rawkv/raw_kv_compare_and_set_task.h"

#include "sdk/common/common.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RawKvCompareAndSetTask::RawKvCompareAndSetTask(const ClientStub& stub, const std::string& key, const std::string& value,
                                               const std::string& expected_value, bool& out_state)
    : RawKvTask(stub),
      key_(key),
      value_(value),
      expected_value_(expected_value),
      out_state_(out_state),
      store_rpc_controller_(stub, rpc_) {}

void RawKvCompareAndSetTask::DoAsync() {
  std::shared_ptr<MetaCache> meta_cache = stub.GetMetaCache();
  std::shared_ptr<Region> region;
  Status s = meta_cache->LookupRegionByKey(key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  rpc_.MutableRequest()->Clear();
  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  auto* kv = rpc_.MutableRequest()->mutable_kv();
  kv->set_key(key_);
  kv->set_value(value_);
  rpc_.MutableRequest()->set_expect_value(expected_value_);

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { KvCompareAndSetRpcCallback(std::forward<decltype(s)>(s)); });
}

void RawKvCompareAndSetTask::KvCompareAndSetRpcCallback(const Status& status) {
  if (status.ok()) {
    out_state_ = rpc_.Response()->key_state();
  }

  DoAsyncDone(status);
}

}  // namespace sdk

}  // namespace dingodb