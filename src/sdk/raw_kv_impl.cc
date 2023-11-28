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

#include "sdk/raw_kv_impl.h"

#include <cstdint>
#include <iterator>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/common.h"
#include "sdk/meta_cache.h"
#include "sdk/region_scanner.h"
#include "sdk/status.h"
#include "sdk/store_rpc.h"
#include "sdk/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RawKV::RawKVImpl::RawKVImpl(const ClientStub& stub) : stub_(stub) {}

Status RawKV::RawKVImpl::Get(const std::string& key, std::string& value) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  if (!got.IsOK()) {
    return got;
  }

  KvGetRpc rpc;
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  rpc.MutableRequest()->set_key(key);

  StoreRpcController controller(stub_, rpc, region);
  Status call = controller.Call();
  if (call.IsOK()) {
    value = rpc.Response()->value();
  }
  return call;
}

void RawKV::RawKVImpl::ProcessSubBatchGet(SubBatchState* sub) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<KvBatchGetRpc*>(sub->rpc));

  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  Status call = controller.Call();
  if (call.IsOK()) {
    for (const auto& kv : rpc->Response()->kvs()) {
      sub->result_kvs.push_back({kv.key(), kv.value()});
    }
  }
  sub->status = call;
}

Status RawKV::RawKVImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string>> region_keys;

  for (const auto& key : keys) {
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_keys[tmp->RegionId()].push_back(key);
  }

  std::vector<SubBatchState> sub_batch_state;
  std::vector<std::unique_ptr<KvBatchGetRpc>> rpcs;

  for (const auto& entry : region_keys) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchGetRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    for (const auto& key : entry.second) {
      auto* fill = rpc->MutableRequest()->add_keys();
      *fill = key;
    }

    sub_batch_state.emplace_back(rpc.get(), region);
    rpcs.push_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), region_keys.size());
  CHECK_EQ(rpcs.size(), sub_batch_state.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_batch_state.size(); i++) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchGet, this, &sub_batch_state[i]);
  }

  ProcessSubBatchGet(sub_batch_state.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;

  std::vector<KVPair> tmp_kvs;
  for (auto& state : sub_batch_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    } else {
      tmp_kvs.insert(tmp_kvs.end(), std::make_move_iterator(state.result_kvs.begin()),
                     std::make_move_iterator(state.result_kvs.end()));
    }
  }

  kvs = std::move(tmp_kvs);

  return result;
}

Status RawKV::RawKVImpl::Put(const std::string& key, const std::string& value) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  if (!got.IsOK()) {
    return got;
  }

  KvPutRpc rpc;
  auto* kv = rpc.MutableRequest()->mutable_kv();
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  kv->set_key(key);
  kv->set_value(value);

  StoreRpcController controller(stub_, rpc, region);
  return controller.Call();
}

void RawKV::RawKVImpl::ProcessSubBatchPut(SubBatchState* sub) {
  (void)CHECK_NOTNULL(dynamic_cast<KvBatchPutRpc*>(sub->rpc));
  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  sub->status = controller.Call();
}

Status RawKV::RawKVImpl::BatchPut(const std::vector<KVPair>& kvs) {
  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<KVPair>> region_kvs;

  for (const auto& kv : kvs) {
    auto key = kv.key;
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_kvs[tmp->RegionId()].push_back(kv);
  }

  std::vector<SubBatchState> sub_batch_put_state;
  std::vector<std::unique_ptr<KvBatchPutRpc>> rpcs;
  for (const auto& entry : region_kvs) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchPutRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    for (const auto& kv : entry.second) {
      auto* fill = rpc->MutableRequest()->add_kvs();
      fill->set_key(kv.key);
      fill->set_value(kv.value);
    }

    sub_batch_put_state.emplace_back(rpc.get(), region);
    rpcs.emplace_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), region_kvs.size());
  CHECK_EQ(rpcs.size(), sub_batch_put_state.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_batch_put_state.size(); i++) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchPut, this, &sub_batch_put_state[i]);
  }

  ProcessSubBatchPut(sub_batch_put_state.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  for (auto& state : sub_batch_put_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    }
  }

  return result;
}

Status RawKV::RawKVImpl::PutIfAbsent(const std::string& key, const std::string& value, bool& state) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status result = meta_cache->LookupRegionByKey(key, region);
  if (result.IsOK()) {
    KvPutIfAbsentRpc rpc;
    FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());

    auto* kv = rpc.MutableRequest()->mutable_kv();
    kv->set_key(key);
    kv->set_value(value);

    StoreRpcController controller(stub_, rpc, region);
    result = controller.Call();
    if (result.IsOK()) {
      state = rpc.Response()->key_state();
    }
  }

  return result;
}

void RawKV::RawKVImpl::ProcessSubBatchPutIfAbsent(SubBatchState* sub) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<KvBatchPutIfAbsentRpc*>(sub->rpc));
  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  Status call = controller.Call();

  if (call.IsOK()) {
    CHECK_EQ(rpc->Request()->kvs_size(), rpc->Response()->key_states_size());
    for (auto i = 0; i < rpc->Request()->kvs_size(); i++) {
      sub->key_op_states.push_back({rpc->Request()->kvs(i).key(), rpc->Response()->key_states(i)});
    }
  }

  sub->status = call;
}

Status RawKV::RawKVImpl::BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& states) {
  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<KVPair>> region_kvs;

  for (const auto& kv : kvs) {
    auto key = kv.key;
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_kvs[tmp->RegionId()].push_back(kv);
  }

  std::vector<SubBatchState> sub_batch_state;
  std::vector<std::unique_ptr<KvBatchPutIfAbsentRpc>> rpcs;
  for (const auto& entry : region_kvs) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchPutIfAbsentRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    for (const auto& kv : entry.second) {
      auto* fill = rpc->MutableRequest()->add_kvs();
      fill->set_key(kv.key);
      fill->set_value(kv.value);
    }
    rpc->MutableRequest()->set_is_atomic(false);

    sub_batch_state.emplace_back(rpc.get(), region);
    rpcs.emplace_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), region_kvs.size());
  CHECK_EQ(rpcs.size(), sub_batch_state.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_batch_state.size(); i++) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchPutIfAbsent, this, &sub_batch_state[i]);
  }

  ProcessSubBatchPutIfAbsent(sub_batch_state.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  std::vector<KeyOpState> tmp_states;
  for (auto& state : sub_batch_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    } else {
      tmp_states.insert(tmp_states.end(), std::make_move_iterator(state.key_op_states.begin()),
                        std::make_move_iterator(state.key_op_states.end()));
    }
  }

  states = std::move(tmp_states);

  return result;
}

Status RawKV::RawKVImpl::Delete(const std::string& key) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status ret = meta_cache->LookupRegionByKey(key, region);
  if (ret.IsOK()) {
    KvBatchDeleteRpc rpc;
    FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    auto* fill = rpc.MutableRequest()->add_keys();
    *fill = key;

    StoreRpcController controller(stub_, rpc, region);
    ret = controller.Call();
    if (!ret.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << rpc.Method() << " send to region: " << region->RegionId()
                         << " fail: " << ret.ToString();
    }
  }

  return ret;
}

void RawKV::RawKVImpl::ProcessSubBatchDelete(SubBatchState* sub) {
  (void)CHECK_NOTNULL(dynamic_cast<KvBatchDeleteRpc*>(sub->rpc));
  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  sub->status = controller.Call();
}

Status RawKV::RawKVImpl::BatchDelete(const std::vector<std::string>& keys) {
  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string>> region_keys;

  for (const auto& key : keys) {
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_keys[tmp->RegionId()].emplace_back(key);
  }

  std::vector<SubBatchState> sub_batch_state;
  std::vector<std::unique_ptr<KvBatchDeleteRpc>> rpcs;
  for (const auto& entry : region_keys) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchDeleteRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());

    for (const auto& key : entry.second) {
      *(rpc->MutableRequest()->add_keys()) = key;
    }

    sub_batch_state.emplace_back(rpc.get(), region);
    rpcs.emplace_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), region_keys.size());
  CHECK_EQ(rpcs.size(), sub_batch_state.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_batch_state.size(); i++) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchDelete, this, &sub_batch_state[i]);
  }

  ProcessSubBatchDelete(sub_batch_state.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  for (auto& state : sub_batch_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    }
  }

  return result;
}

void RawKV::RawKVImpl::ProcessSubBatchDeleteRange(SubBatchState* sub) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<KvDeleteRangeRpc*>(sub->rpc));
  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  sub->status = controller.Call();
  sub->delete_count = rpc->Response()->delete_count();
}

Status RawKV::RawKVImpl::DeleteRange(const std::string& start_key, const std::string& end_key, bool continuous,
                                     int64_t& delete_count) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  auto meta_cache = stub_.GetMetaCache();

  std::vector<std::shared_ptr<Region>> regions;
  Status ret = meta_cache->ScanRegionsBetweenRange(start_key, end_key, 0, regions);
  if (!ret.IsOK()) {
    if (ret.IsNotFound()) {
      DINGO_LOG(WARNING) << fmt::format("region not found between [{},{}), no need retry, status:{}", start_key,
                                        end_key, ret.ToString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("lookup region fail between [{},{}), need retry, status:{}", start_key, end_key,
                                        ret.ToString());
    }
    return ret;
  }

  CHECK(!regions.empty()) << "regions must not empty";

  if (continuous) {
    for (int i = 0; i < regions.size() - 1; i++) {
      auto cur = regions[i];
      auto next = regions[i + 1];
      if (cur->Range().end_key() != next->Range().start_key()) {
        std::string msg = fmt::format("regions bewteen [{}, {}) not continuous", start_key, end_key);
        DINGO_LOG(WARNING) << msg
                           << fmt::format(", cur region:{} ({}-{}), next region:{} ({}-{})", cur->RegionId(),
                                          cur->Range().start_key(), cur->Range().end_key(), next->RegionId(),
                                          next->Range().start_key(), next->Range().end_key());
        return Status::Aborted(msg);
      }
    }
  }

  struct DeleteRangeContext {
    std::string start;
    std::string end;
  };

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<DeleteRangeContext>> to_delete;

  for (const auto& region : regions) {
    const auto& range = region->Range();
    auto start = (range.start_key() <= start_key ? start_key : range.start_key());
    auto end = (range.end_key() <= end_key) ? range.end_key() : end_key;

    auto iter = region_id_to_region.find(region->RegionId());
    DCHECK(iter == region_id_to_region.end());
    region_id_to_region.emplace(std::make_pair(region->RegionId(), region));

    to_delete[region->RegionId()].push_back({start, end});
  }

  DCHECK_EQ(region_id_to_region.size(), to_delete.size());

  std::vector<SubBatchState> sub_batch_state;
  std::vector<std::unique_ptr<KvDeleteRangeRpc>> rpcs;
  for (const auto& entry : to_delete) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvDeleteRangeRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    for (const DeleteRangeContext& delete_range : entry.second) {
      auto* range_with_option = rpc->MutableRequest()->mutable_range();

      auto* range = range_with_option->mutable_range();
      range->set_start_key(delete_range.start);
      range->set_end_key(delete_range.end);

      range_with_option->set_with_start(true);
      range_with_option->set_with_end(false);
    }

    sub_batch_state.emplace_back(rpc.get(), region);
    rpcs.emplace_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), to_delete.size());
  CHECK_EQ(rpcs.size(), sub_batch_state.size());

  std::vector<std::thread> thread_pool;
  thread_pool.reserve(sub_batch_state.size());
  for (auto& batch_state : sub_batch_state) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchDeleteRange, this, &batch_state);
  }

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  int64_t tmp_delete_count = 0;

  for (auto& state : sub_batch_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    } else {
      tmp_delete_count += state.delete_count;
    }
  }

  delete_count = tmp_delete_count;

  return result;
}

Status RawKV::RawKVImpl::CompareAndSet(const std::string& key, const std::string& value,
                                       const std::string& expected_value, bool& state) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status ret = meta_cache->LookupRegionByKey(key, region);
  if (ret.IsOK()) {
    KvCompareAndSetRpc rpc;
    FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    auto* kv = rpc.MutableRequest()->mutable_kv();
    kv->set_key(key);
    kv->set_value(value);
    rpc.MutableRequest()->set_expect_value(expected_value);

    StoreRpcController controller(stub_, rpc, region);
    ret = controller.Call();
    if (ret.IsOK()) {
      state = rpc.Response()->key_state();
    }
  }

  return ret;
}

void RawKV::RawKVImpl::ProcessSubBatchCompareAndSet(SubBatchState* sub) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<KvBatchCompareAndSetRpc*>(sub->rpc));
  StoreRpcController controller(stub_, *sub->rpc, sub->region);
  Status call = controller.Call();
  if (call.IsOK()) {
    CHECK_EQ(rpc->Request()->kvs_size(), rpc->Response()->key_states_size());
    for (auto i = 0; i < rpc->Request()->kvs_size(); i++) {
      sub->key_op_states.push_back({rpc->Request()->kvs(i).key(), rpc->Response()->key_states(i)});
    }
  }
  sub->status = call;
}

Status RawKV::RawKVImpl::BatchCompareAndSet(const std::vector<KVPair>& kvs,
                                            const std::vector<std::string>& expected_values,
                                            std::vector<KeyOpState>& states) {
  if (kvs.size() != expected_values.size()) {
    return Status::InvalidArgument(
        fmt::format("kvs size:{} must equal expected_values size:{}", kvs.size(), expected_values.size()));
  }

  struct CompareAndSetContext {
    KVPair kv_pair;
    std::string expected_value;
  };

  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<CompareAndSetContext>> region_kvs;

  for (auto i = 0; i < kvs.size(); i++) {
    auto kv = kvs[i];
    auto key = kv.key;
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    const auto& expected_value = expected_values[i];
    region_kvs[tmp->RegionId()].push_back({kv, expected_value});
  }

  CHECK_EQ(region_id_to_region.size(), region_kvs.size());

  std::vector<SubBatchState> sub_batch_state;
  std::vector<std::unique_ptr<KvBatchCompareAndSetRpc>> rpcs;
  for (const auto& entry : region_kvs) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<KvBatchCompareAndSetRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    for (const CompareAndSetContext& context : entry.second) {
      auto* kv = rpc->MutableRequest()->add_kvs();
      kv->set_key(context.kv_pair.key);
      kv->set_value(context.kv_pair.value);
      *(rpc->MutableRequest()->add_expect_values()) = context.expected_value;
    }

    sub_batch_state.emplace_back(rpc.get(), region);
    rpcs.emplace_back(std::move(rpc));
  }

  CHECK_EQ(rpcs.size(), region_kvs.size());
  CHECK_EQ(rpcs.size(), sub_batch_state.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_batch_state.size(); i++) {
    thread_pool.emplace_back(&RawKV::RawKVImpl::ProcessSubBatchCompareAndSet, this, &sub_batch_state[i]);
  }

  ProcessSubBatchCompareAndSet(sub_batch_state.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  std::vector<KeyOpState> tmp_states;
  for (auto& state : sub_batch_state) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "rpc: " << state.rpc->Method() << " send to region: " << state.region->RegionId()
                         << " fail: " << state.status.ToString();
      if (result.IsOK()) {
        // only return first fail status
        result = state.status;
      }
    } else {
      tmp_states.insert(tmp_states.end(), std::make_move_iterator(state.key_op_states.begin()),
                        std::make_move_iterator(state.key_op_states.end()));
    }
  }

  states = std::move(tmp_states);

  return result;
}

// TODO: abstract range scanner
Status RawKV::RawKVImpl::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                              std::vector<KVPair>& kvs) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  auto meta_cache = stub_.GetMetaCache();

  {
    // precheck: return not found if no region in [start, end_key)
    std::shared_ptr<Region> region;
    Status ret = meta_cache->LookupRegionBetweenRange(start_key, end_key, region);
    if (!ret.IsOK()) {
      if (ret.IsNotFound()) {
        DINGO_LOG(WARNING) << fmt::format("region not found between [{},{}), no need retry, status:{}", start_key,
                                          end_key, ret.ToString());
      } else {
        DINGO_LOG(WARNING) << fmt::format("lookup region fail between [{},{}), need retry, status:{}", start_key,
                                          end_key, ret.ToString());
      }
      return ret;
    }
  }

  std::string next_start = start_key;
  std::vector<KVPair> tmp_kvs;

  DINGO_LOG(INFO) << fmt::format("scan start between [{},{}), next_start:{}", start_key, end_key, next_start);

  while (next_start < end_key) {
    std::shared_ptr<Region> region;
    Status ret = meta_cache->LookupRegionBetweenRange(next_start, end_key, region);

    if (ret.IsNotFound()) {
      DINGO_LOG(WARNING) << fmt::format("region not found  between [{},{}), start_key:{} status:{}", next_start,
                                        end_key, start_key, ret.ToString());
      kvs = std::move(tmp_kvs);
      return Status::OK();
    }

    if (!ret.IsOK()) {
      DINGO_LOG(WARNING) << fmt::format("region look fail between [{},{}), start_key:{} status:{}", next_start, end_key,
                                        start_key, ret.ToString());
      return ret;
    }

    std::unique_ptr<RegionScanner> scanner;
    CHECK(stub_.GetRegionScannerFactory()->NewRegionScanner(stub_, region, scanner).IsOK());
    ret = scanner->Open();
    if (!ret.IsOK()) {
      DINGO_LOG(WARNING) << fmt::format("region scanner open fail, region:{}, status:{}", region->RegionId(),
                                        ret.ToString());
      return ret;
    }

    DINGO_LOG(INFO) << fmt::format("region:{} scan start, region range:({}-{})", region->RegionId(),
                                   region->Range().start_key(), region->Range().end_key());

    while (scanner->HasMore()) {
      std::vector<KVPair> scan_kvs;
      ret = scanner->NextBatch(scan_kvs);
      if (!ret.IsOK()) {
        DINGO_LOG(WARNING) << fmt::format("region scanner NextBatch fail, region:{}, status:{}", region->RegionId(),
                                          ret.ToString());
        return ret;
      }

      if (!scan_kvs.empty()) {
        tmp_kvs.insert(tmp_kvs.end(), std::make_move_iterator(scan_kvs.begin()),
                       std::make_move_iterator(scan_kvs.end()));

        if (limit != 0 && (tmp_kvs.size() >= limit)) {
          tmp_kvs.resize(limit);
          break;
        }
      } else {
        DINGO_LOG(INFO) << fmt::format("region:{} scanner NextBatch is empty", region->RegionId());
        CHECK(!scanner->HasMore());
      }
    }

    if (limit != 0 && (tmp_kvs.size() >= limit)) {
      DINGO_LOG(INFO) << fmt::format(
          "region:{} scan finished, stop to scan between [{},{}), next_start:{}, limit:{}, scan_cnt:{}",
          region->RegionId(), start_key, end_key, next_start, limit, tmp_kvs.size());
      break;
    } else {
      next_start = region->Range().end_key();
      DINGO_LOG(INFO) << fmt::format("region:{} scan finished, continue to scan between [{},{}), next_start:{}, ",
                                     region->RegionId(), start_key, end_key, next_start);
      continue;
    }
  }

  DINGO_LOG(INFO) << fmt::format("scan end between [{},{}), next_start:{}", start_key, end_key, next_start);

  kvs = std::move(tmp_kvs);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb