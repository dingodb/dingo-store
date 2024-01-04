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

#include "sdk/rawkv/raw_kv_impl.h"

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
#include "sdk/common/common.h"
#include "sdk/meta_cache.h"
#include "sdk/rawkv/raw_kv_batch_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_batch_delete_task.h"
#include "sdk/rawkv/raw_kv_batch_get_task.h"
#include "sdk/rawkv/raw_kv_batch_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_batch_put_task.h"
#include "sdk/rawkv/raw_kv_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_delete_task.h"
#include "sdk/rawkv/raw_kv_get_task.h"
#include "sdk/rawkv/raw_kv_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_put_task.h"
#include "sdk/region_scanner.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RawKV::RawKVImpl::RawKVImpl(const ClientStub& stub) : stub_(stub) {}

Status RawKV::RawKVImpl::Get(const std::string& key, std::string& value) {
  RawKvGetTask task(stub_, key, value);
  return task.Run();
}

Status RawKV::RawKVImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  RawKvBatchGetTask task(stub_, keys, kvs);
  return task.Run();
}

Status RawKV::RawKVImpl::Put(const std::string& key, const std::string& value) {
  RawKvPutTask task(stub_, key, value);
  return task.Run();
}

Status RawKV::RawKVImpl::BatchPut(const std::vector<KVPair>& kvs) {
  RawKvBatchPutTask task(stub_, kvs);
  return task.Run();
}

Status RawKV::RawKVImpl::PutIfAbsent(const std::string& key, const std::string& value, bool& state) {
  RawKvPutIfAbsentTask task(stub_, key, value, state);
  return task.Run();
}

Status RawKV::RawKVImpl::BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& states) {
  RawKvBatchPutIfAbsentTask task(stub_, kvs, states);
  return task.Run();
}

Status RawKV::RawKVImpl::Delete(const std::string& key) {
  RawKvDeleteTask task(stub_, key);
  return task.Run();
}

Status RawKV::RawKVImpl::BatchDelete(const std::vector<std::string>& keys) {
  RawKvBatchDeleteTask task(stub_, keys);
  return task.Run();
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
  RawKvCompareAndSetTask task(stub_, key, value, expected_value, state);
  return task.Run();
}

Status RawKV::RawKVImpl::BatchCompareAndSet(const std::vector<KVPair>& kvs,
                                            const std::vector<std::string>& expected_values,
                                            std::vector<KeyOpState>& states) {
  if (kvs.size() != expected_values.size()) {
    return Status::InvalidArgument(
        fmt::format("kvs size:{} must equal expected_values size:{}", kvs.size(), expected_values.size()));
  }
  RawKvBatchCompareAndSetTask task(stub_, kvs, expected_values, states);
  return task.Run();
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
      DINGO_LOG(INFO) << fmt::format("region not found  between [{},{}), start_key:{} status:{}", next_start, end_key,
                                     start_key, ret.ToString());
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