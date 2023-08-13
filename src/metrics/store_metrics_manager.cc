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

#include "metrics/store_metrics_manager.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_manager.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

namespace store {

std::string RegionMetrics::Serialize() { return inner_region_metrics_.SerializeAsString(); }

void RegionMetrics::DeSerialize(const std::string& data) {
  inner_region_metrics_.ParsePartialFromArray(data.data(), data.size());
}

void RegionMetrics::UpdateMaxAndMinKey(const PbKeyValues& kvs) {
  for (const auto& kv : kvs) {
    if (inner_region_metrics_.min_key().empty() || kv.key() < inner_region_metrics_.min_key()) {
      inner_region_metrics_.set_min_key(kv.key());
    } else if (kv.key() > inner_region_metrics_.max_key()) {
      inner_region_metrics_.set_max_key(kv.key());
    }
  }
}

void RegionMetrics::UpdateMaxAndMinKeyPolicy(const PbKeys& keys) {
  for (const auto& key : keys) {
    if (key == inner_region_metrics_.min_key()) {
      need_update_min_key_ = true;
    } else if (key == inner_region_metrics_.max_key()) {
      need_update_max_key_ = true;
    }
  }
}

void RegionMetrics::UpdateMaxAndMinKeyPolicy(const PbRanges& ranges) {
  for (const auto& range : ranges) {
    if (range.start_key() <= inner_region_metrics_.min_key() && inner_region_metrics_.min_key() < range.end_key()) {
      need_update_min_key_ = true;
    }
    if (range.start_key() <= inner_region_metrics_.max_key() && inner_region_metrics_.max_key() < range.end_key()) {
      need_update_max_key_ = true;
    }
  }
}

void RegionMetrics::UpdateMaxAndMinKeyPolicy() {
  need_update_min_key_ = true;
  need_update_max_key_ = true;
}

}  // namespace store

bool StoreMetrics::Init() { return CollectMetrics(); }

bool StoreMetrics::CollectMetrics() {
  std::map<std::string, uint64_t> output;

  auto role = Server::GetInstance()->GetRole();
  auto config = ConfigManager::GetInstance()->GetConfig(role);

  // system disk capacity
  if (!Helper::GetSystemDiskCapacity(config->GetString("store.path"), output)) {
    return false;
  }

  metrics_->set_system_total_capacity(output["system_total_capacity"]);
  metrics_->set_system_free_capacity(output["system_free_capacity"]);

  // system memory info
  output.clear();
  if (!Helper::GetSystemMemoryInfo(output)) {
    return false;
  }

  metrics_->set_system_total_memory(output["system_total_memory"]);
  metrics_->set_system_free_memory(output["system_free_memory"]);
  metrics_->set_system_total_swap(output["system_total_swap"]);
  metrics_->set_system_free_swap(output["system_free_swap"]);

  // system cpu usage
  output.clear();
  if (!Helper::GetSystemCpuUsage(output)) {
    return false;
  }

  metrics_->set_system_cpu_usage(output["system_cpu_usage"]);

  // process memory info
  output.clear();
  if (!Helper::GetProcessMemoryInfo(output)) {
    return false;
  }

  metrics_->set_process_used_memory(output["process_used_memory"]);

  return true;
}

bool StoreRegionMetrics::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "Scan store region metrics failed!";
    return false;
  }
  DINGO_LOG(INFO) << "Init Store region metrics num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

std::string StoreRegionMetrics::GetRegionMinKey(store::RegionPtr region) {
  DINGO_LOG(INFO) << fmt::format("GetRegionMinKey... region {} range[{}-{}]", region->Id(),
                                 Helper::StringToHex(region->RawRange().start_key()),
                                 Helper::StringToHex(region->RawRange().end_key()));
  IteratorOptions options;
  options.upper_bound = region->RawRange().end_key();
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);
  iter->Seek(region->RawRange().start_key());

  if (!iter->Valid()) {
    return "";
  }

  auto min_key = iter->Key();
  return std::string(min_key.data(), min_key.size());
}

std::string StoreRegionMetrics::GetRegionMaxKey(store::RegionPtr region) {
  DINGO_LOG(INFO) << fmt::format("GetRegionMaxKey... region {} range[{}-{}]", region->Id(),
                                 Helper::StringToHex(region->RawRange().start_key()),
                                 Helper::StringToHex(region->RawRange().end_key()));
  IteratorOptions options;
  options.lower_bound = region->RawRange().start_key();
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);
  iter->SeekForPrev(region->RawRange().end_key());

  if (!iter->Valid()) {
    return "";
  }

  auto max_key = iter->Key();
  return std::string(max_key.data(), max_key.size());
}

uint64_t StoreRegionMetrics::GetRegionKeyCount(store::RegionPtr region) {
  auto reader = raw_engine_->NewReader(Constant::kStoreDataCF);

  uint64_t count = 0;
  reader->KvCount(region->RawRange().start_key(), region->RawRange().end_key(), count);

  return count;
}

std::vector<uint64_t> StoreRegionMetrics::GetRegionApproximateSize(std::vector<store::RegionPtr> regions) {
  std::vector<pb::common::Range> ranges;
  ranges.reserve(regions.size());
  for (const auto& region : regions) {
    ranges.push_back(region->RawRange());
  }

  return raw_engine_->GetApproximateSizes(Constant::kStoreDataCF, ranges);
}

bool StoreRegionMetrics::CollectMetrics() {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();
  auto region_metricses = GetAllMetrics();

  std::vector<store::RegionPtr> need_collect_regions;
  for (const auto& region_metrics : region_metricses) {
    auto raft_meta = store_raft_meta->GetRaftMeta(region_metrics->Id());
    if (raft_meta == nullptr) {
      continue;
    }
    uint64_t applied_index = raft_meta->applied_index();
    if (applied_index != 0 && region_metrics->LastLogIndex() >= applied_index) {
      continue;
    }

    region_metrics->SetLastLogIndex(applied_index);

    auto region = store_region_meta->GetRegion(region_metrics->Id());
    if (region == nullptr) {
      continue;
    }
    need_collect_regions.push_back(region);

    uint64_t start_time = Helper::TimestampMs();
    // Get min key
    bool is_collect_min_key = false;
    if (region_metrics->NeedUpdateMinKey()) {
      is_collect_min_key = true;
      region_metrics->SetNeedUpdateMinKey(false);
      region_metrics->SetMinKey(GetRegionMinKey(region));
    }
    // Get max key
    bool is_collect_max_key = false;
    if (region_metrics->NeedUpdateMaxKey()) {
      is_collect_max_key = true;
      region_metrics->SetNeedUpdateMaxKey(false);
      region_metrics->SetMaxKey(GetRegionMaxKey(region));
    }

    // Get region key counts
    if (region_metrics->NeedUpdateKeyCount()) {
      region_metrics->SetKeyCount(GetRegionKeyCount(region));
    } else {
      region_metrics->SetNeedUpdateKeyCount(true);
    }

    // vector index
    bool vector_index_has_data = false;
    std::shared_ptr<VectorIndexManager> vector_index_mgr = Server::GetInstance()->GetVectorIndexManager();
    if (vector_index_mgr) {
      std::shared_ptr<VectorIndex> vector_index = vector_index_mgr->GetVectorIndex(region_metrics->Id());
      if (vector_index) {
        if (pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE != vector_index->VectorIndexType()) {
          region_metrics->SetVectorIndexType(vector_index->VectorIndexType());

          uint64_t current_count = 0;
          vector_index->GetCount(current_count);
          region_metrics->SetVectorCurrentCount(current_count);

          uint64_t deleted_count = 0;
          vector_index->GetDeletedCount(deleted_count);
          region_metrics->SetVectorDeletedCount(deleted_count);

          std::shared_ptr<Context> ctx = std::make_shared<Context>();
          ctx->SetRegionId(region_metrics->Id());

          auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
          uint64_t max_id = 0;

          reader->VectorGetBorderId(ctx, max_id, false);
          region_metrics->SetVectorMaxId(max_id);

          uint64_t min_id = 0;
          reader->VectorGetBorderId(ctx, min_id, true);
          region_metrics->SetVectorMinId(min_id);

          uint64_t total_memory_usage = 0;
          vector_index->GetMemorySize(total_memory_usage);
          region_metrics->SetVectorMemoryBytes(total_memory_usage);

          vector_index_has_data = true;
        }
      }
    }

    if (vector_index_has_data) {
      DINGO_LOG(DEBUG) << fmt::format(
          "Collect region metrics, region {} min_key[{}] max_key[{}] key_count[true] region_size[true]  "
          "vector_type[{}] vector_index_count[{}] vector_index_deleted_count[{}] vector_index_max_id[{}] "
          "vector_index_min_id[{}] "
          "vector_index_memory_bytes[{}]"
          "elapsed[{} ms]",
          region->Id(), is_collect_min_key ? "true" : "false", is_collect_max_key ? "true" : "false",
          static_cast<int>(region_metrics->GetVectorIndexType()), region_metrics->GetVectorCurrentCount(),
          region_metrics->GetVectorDeletedCount(), region_metrics->GetVectorMaxId(), region_metrics->GetVectorMinId(),
          region_metrics->GetVectorMemoryBytes(), Helper::TimestampMs() - start_time);
    } else {  //  no vector index data
      DINGO_LOG(DEBUG) << fmt::format(
          "Collect region metrics, region {} min_key[{}] max_key[{}] key_count[true] region_size[true] elapsed[{} "
          "ms]",
          region->Id(), is_collect_min_key ? "true" : "false", is_collect_max_key ? "true" : "false",
          Helper::TimestampMs() - start_time);
    }

    meta_writer_->Put(TransformToKv(region_metrics));
  }

  // Get approximate size
  uint64_t start_time = Helper::TimestampMs();
  auto sizes = GetRegionApproximateSize(need_collect_regions);
  for (int i = 0; i < sizes.size(); ++i) {
    auto region_metrics = GetMetrics(need_collect_regions[i]->Id());
    if (region_metrics != nullptr) {
      region_metrics->SetRegionSize(sizes[i]);
    }
  }

  DINGO_LOG(DEBUG) << fmt::format("Get region approximate size elapsed[{} ms]", Helper::TimestampMs() - start_time);

  return true;
}

store::RegionMetricsPtr StoreRegionMetrics::NewMetrics(uint64_t region_id) {
  auto metrics = std::make_shared<store::RegionMetrics>();
  metrics->SetId(region_id);
  return metrics;
}

void StoreRegionMetrics::AddMetrics(store::RegionMetricsPtr metrics) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.insert_or_assign(metrics->Id(), metrics);
  }

  meta_writer_->Put(TransformToKv(metrics));
}

void StoreRegionMetrics::DeleteMetrics(uint64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

store::RegionMetricsPtr StoreRegionMetrics::GetMetrics(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = metricses_.find(region_id);
  if (it == metricses_.end()) {
    return nullptr;
  }

  return it->second;
}

std::vector<store::RegionMetricsPtr> StoreRegionMetrics::GetAllMetrics() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<store::RegionMetricsPtr> metricses;
  metricses.reserve(metricses_.size());
  for (auto [_, metrics] : metricses_) {
    metricses.push_back(metrics);
  }

  return metricses;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMetrics::TransformToKv(std::any obj) {
  auto region_metrics = std::any_cast<store::RegionMetricsPtr>(obj);
  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region_metrics->Id()));
  kv->set_value(region_metrics->Serialize());

  return kv;
}

void StoreRegionMetrics::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    auto region_metrics = std::make_shared<store::RegionMetrics>();
    region_metrics->DeSerialize(kv.value());
    metricses_.insert_or_assign(region_id, region_metrics);
  }
}

bool StoreMetricsManager::Init() {
  if (!store_metrics_->Init()) {
    DINGO_LOG(ERROR) << "Init store metrics failed!";
    return false;
  }

  if (!region_metrics_->Init()) {
    DINGO_LOG(ERROR) << "Init store region metrics failed!";
    return false;
  }

  return true;
}

void StoreMetricsManager::CollectMetrics() {
  if (is_collecting_.load()) {
    DINGO_LOG(WARNING) << "Already exist collecting metrics.";
    return;
  }

  is_collecting_.store(true);

  store_metrics_->CollectMetrics();
  region_metrics_->CollectMetrics();

  is_collecting_.store(false);
}

}  // namespace dingodb