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

#include "sdk/rawkv/raw_kv_batch_compare_and_set_task.h"

#include <string_view>

#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/rawkv/raw_kv_task.h"

namespace dingodb {
namespace sdk {

RawKvBatchCompareAndSetTask::RawKvBatchCompareAndSetTask(const ClientStub& stub, const std::vector<KVPair>& kvs,
                                                         const std::vector<std::string>& expected_values,
                                                         std::vector<KeyOpState>& out_states)
    : RawKvTask(stub), kvs_(kvs), expected_values_(expected_values), out_states_(out_states) {}

Status RawKvBatchCompareAndSetTask::Init() {
  CHECK_EQ(kvs_.size(), expected_values_.size()) << "kvs size must equal expected_values size";
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  next_keys_.clear();
  for (auto i = 0; i < kvs_.size(); i++) {
    const auto& kv = kvs_[i];
    const auto& expected_value = expected_values_[i];
    CHECK(next_keys_.insert(kv.key).second) << "next_keys_ exist duplicate key: " << kv.key;
    CHECK(compare_and_set_contexts_.insert({kv.key, {kv, expected_value}}).second)
        << "compare_and_set_contexts_ exist duplicate key: " << kv.key;
  }

  return Status::OK();
}

void RawKvBatchCompareAndSetTask::DoAsync() {
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
    std::shared_ptr<Region> region;
    Status s = meta_cache->LookupRegionByKey(key, region);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(region->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(region->RegionId(), region));
    }

    region_keys[region->RegionId()].push_back(key);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_keys) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchCompareAndSetRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    rpc->MutableRequest()->set_is_atomic(false);
    for (const auto& key : entry.second) {
      const auto& key_context = compare_and_set_contexts_.find(key);
      CHECK(key_context != compare_and_set_contexts_.end()) << "can't find key:" << key;

      auto* kv = rpc->MutableRequest()->add_kvs();
      const KVPair& kv_pair = key_context->second.kv_pair;
      kv->set_key(kv_pair.key);
      kv->set_value(kv_pair.value);
      *(rpc->MutableRequest()->add_expect_values()) = key_context->second.expected_value;
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
        [this, rpc = rpcs_[i].get()](auto&& s) { KvBatchCompareAndSetRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void RawKvBatchCompareAndSetTask::KvBatchCompareAndSetRpcCallback(const Status& status, KvBatchCompareAndSetRpc* rpc) {
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

void RawKvBatchCompareAndSetTask::PostProcess() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  out_states_.swap(tmp_out_states_);
}

}  // namespace sdk

}  // namespace dingodb
