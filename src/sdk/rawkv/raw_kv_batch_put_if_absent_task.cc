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

#include "sdk/rawkv/raw_kv_batch_put_if_absent_task.h"

#include "sdk/common/common.h"
#include "sdk/rawkv/raw_kv_task.h"

namespace dingodb {
namespace sdk {

RawKvBatchPutIfAbsentTask::RawKvBatchPutIfAbsentTask(const ClientStub& stub, const std::vector<KVPair>& kvs,
                                                     std::vector<KeyOpState>& out_states)
    : RawKvTask(stub), kvs_(kvs), out_states_(out_states) {}

Status RawKvBatchPutIfAbsentTask::Init() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  next_keys_.clear();
  for (const auto& kv : kvs_) {
    CHECK(next_keys_.insert(kv.key).second) << "duplicate key: " << kv.key;
  }
  return Status::OK();
}

void RawKvBatchPutIfAbsentTask::DoAsync() {
  std::set<std::string_view> next_batch;
  Status tmp;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    next_batch = next_keys_;
    tmp = status_;
  }

  if (next_batch.empty()) {
    DoAsyncDone(tmp);
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string_view>> region_keys;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& key : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(key, tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_keys[tmp->RegionId()].push_back(key);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_keys) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchPutIfAbsentRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    rpc->MutableRequest()->set_is_atomic(false);
    for (const auto& key : entry.second) {
      auto kv = std::find_if(kvs_.begin(), kvs_.end(), [&](const KVPair& kv) { return kv.key == key; });
      CHECK(kv != kvs_.end()) << "can't find key:" << key;
      auto* fill = rpc->MutableRequest()->add_kvs();
      fill->set_key(kv->key);
      fill->set_value(kv->value);
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  CHECK_EQ(rpcs_.size(), region_keys.size());
  CHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_keys.size());

  for (auto i = 0; i < region_keys.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { KvBatchPutIfAbsentRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void RawKvBatchPutIfAbsentTask::KvBatchPutIfAbsentRpcCallback(const Status& status, KvBatchPutIfAbsentRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    CHECK_EQ(rpc->Request()->kvs_size(), rpc->Response()->key_states_size());

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    for (auto i = 0; i < rpc->Request()->kvs_size(); i++) {
      std::string key = rpc->Request()->kvs(i).key();
      next_keys_.erase(key);
      tmp_out_states_.push_back({std::move(key), rpc->Response()->key_states(i)});
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

void RawKvBatchPutIfAbsentTask::PostProcess() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  out_states_.swap(tmp_out_states_);
}

}  // namespace sdk

}  // namespace dingodb