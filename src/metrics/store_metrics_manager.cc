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
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_manager.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_double(min_system_disk_capacity_free_ratio, 0.05, "Min system disk capacity free ratio");
DEFINE_double(min_system_memory_capacity_free_ratio, 0.10, "Min system memory capacity free ratio");

namespace store {

RegionMetrics::RegionMetrics(int64_t region_id) {
  inner_region_metrics_.set_id(region_id);
  bthread_mutex_init(&mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.RegionMetrics][id({})]", region_id);
}

RegionMetrics::~RegionMetrics() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.RegionMetrics][id({})]", Id());
  bthread_mutex_destroy(&mutex_);
}

std::string RegionMetrics::Serialize() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_metrics_.SerializeAsString();
}

void RegionMetrics::DeSerialize(const std::string& data) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_metrics_.ParsePartialFromArray(data.data(), data.size());
}

void RegionMetrics::UpdateMaxAndMinKey(const PbKeyValues& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    if (inner_region_metrics_.min_key().empty() || kv.key() < inner_region_metrics_.min_key()) {
      inner_region_metrics_.set_min_key(kv.key());
    } else if (kv.key() > inner_region_metrics_.max_key()) {
      inner_region_metrics_.set_max_key(kv.key());
    }
  }
}

void RegionMetrics::UpdateMaxAndMinKeyPolicy(const PbKeys& keys) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& key : keys) {
    if (key == inner_region_metrics_.min_key()) {
      need_update_min_key_ = true;
    } else if (key == inner_region_metrics_.max_key()) {
      need_update_max_key_ = true;
    }
  }
}

void RegionMetrics::UpdateMaxAndMinKeyPolicy(const PbRanges& ranges) {
  BAIDU_SCOPED_LOCK(mutex_);
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
  BAIDU_SCOPED_LOCK(mutex_);
  need_update_min_key_ = true;
  need_update_max_key_ = true;
}

}  // namespace store

bool StoreMetrics::Init() { return CollectMetrics(); }

bool StoreMetrics::CollectMetrics() {
  std::map<std::string, int64_t> output;

  auto config = ConfigManager::GetInstance().GetRoleConfig();

  // system disk capacity
  if (!Helper::GetSystemDiskCapacity(config->GetString("store.path"), output)) {
    return false;
  }

  metrics_->mutable_store_own_metrics()->set_id(Server::GetInstance().Id());
  metrics_->mutable_store_own_metrics()->set_system_total_capacity(output["system_total_capacity"]);
  metrics_->mutable_store_own_metrics()->set_system_free_capacity(output["system_free_capacity"]);

  // system memory info
  output.clear();
  if (!Helper::GetSystemMemoryInfo(output)) {
    return false;
  }

  metrics_->mutable_store_own_metrics()->set_system_total_memory(output["system_total_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_free_memory(output["system_free_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_shared_memory(output["system_shared_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_buffer_memory(output["system_buffer_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_cached_memory(output["system_cached_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_available_memory(output["system_available_memory"]);
  metrics_->mutable_store_own_metrics()->set_system_total_swap(output["system_total_swap"]);
  metrics_->mutable_store_own_metrics()->set_system_free_swap(output["system_free_swap"]);

  // system cpu usage
  output.clear();
  if (!Helper::GetSystemCpuUsage(output)) {
    return false;
  }

  metrics_->mutable_store_own_metrics()->set_system_cpu_usage(output["system_cpu_usage"]);

  // process memory info
  output.clear();
  if (!Helper::GetProcessMemoryInfo(output)) {
    return false;
  }

  metrics_->mutable_store_own_metrics()->set_process_used_memory(output["process_used_memory"]);

  // calc is_read_only for self store
  bool self_store_is_read_only = false;
  int64_t free_capacity = metrics_->store_own_metrics().system_free_capacity();
  int64_t total_capacity = metrics_->store_own_metrics().system_total_capacity();
  if (total_capacity != 0) {
    double disk_free_capacity_ratio = static_cast<double>(free_capacity) / static_cast<double>(total_capacity);
    if (disk_free_capacity_ratio < FLAGS_min_system_disk_capacity_free_ratio) {
      std::string s = fmt::format("Disk capacity is not enough, capacity({} / {} / {:2.2})", free_capacity,
                                  total_capacity, disk_free_capacity_ratio);
      DINGO_LOG(WARNING) << s;
      self_store_is_read_only = true;
    }
  }

  int64_t available_memory = metrics_->store_own_metrics().system_available_memory();
  int64_t total_memory = metrics_->store_own_metrics().system_total_memory();
  if (total_memory != 0 && available_memory != INT64_MAX) {
    double memory_free_capacity_ratio = static_cast<double>(available_memory) / static_cast<double>(total_memory);
    if (memory_free_capacity_ratio < FLAGS_min_system_memory_capacity_free_ratio) {
      std::string s = fmt::format("Memory capacity is not enough, capacity({} / {} / {:2.2})", available_memory,
                                  total_memory, memory_free_capacity_ratio);
      DINGO_LOG(WARNING) << s;
      self_store_is_read_only = true;
    }
  }

  metrics_->mutable_store_own_metrics()->set_is_ready_only(self_store_is_read_only);

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
  DINGO_LOG(INFO) << fmt::format("[metrics.region][region({})] get region min key, range[{}-{}]", region->Id(),
                                 Helper::StringToHex(region->Range().start_key()),
                                 Helper::StringToHex(region->Range().end_key()));
  IteratorOptions options;
  options.upper_bound = region->Range().end_key();
  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  auto iter = raw_engine->Reader()->NewIterator(Constant::kStoreDataCF, options);
  iter->Seek(region->Range().start_key());

  if (!iter->Valid()) {
    return "";
  }

  auto min_key = iter->Key();
  return std::string(min_key.data(), min_key.size());
}

std::string StoreRegionMetrics::GetRegionMaxKey(store::RegionPtr region) {
  DINGO_LOG(INFO) << fmt::format("[metrics.region][region({})] get region max key, range[{}-{}]", region->Id(),
                                 Helper::StringToHex(region->Range().start_key()),
                                 Helper::StringToHex(region->Range().end_key()));
  IteratorOptions options;
  options.lower_bound = region->Range().start_key();
  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  auto iter = raw_engine->Reader()->NewIterator(Constant::kStoreDataCF, options);
  iter->SeekForPrev(region->Range().end_key());

  if (!iter->Valid()) {
    return "";
  }

  auto max_key = iter->Key();
  return std::string(max_key.data(), max_key.size());
}

int64_t StoreRegionMetrics::GetRegionKeyCount(store::RegionPtr region) {
  int64_t count = 0;
  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  raw_engine->Reader()->KvCount(Constant::kStoreDataCF, region->Range().start_key(), region->Range().end_key(), count);

  return count;
}

std::vector<std::pair<int64_t, int64_t>> StoreRegionMetrics::GetRegionApproximateSize(
    std::vector<store::RegionPtr> regions) {
  std::vector<pb::common::Range> ranges;
  std::vector<store::RegionPtr> valid_regions;
  std::vector<std::pair<int64_t, int64_t>> region_sizes;
  ranges.reserve(regions.size());
  valid_regions.reserve(regions.size());
  region_sizes.reserve(regions.size());
  for (const auto& region : regions) {
    auto range = region->Range();
    if (range.start_key() >= range.end_key()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[metrics.region][region({})] get region approximate size failed, invalid range [{}-{})", region->Id(),
          Helper::StringToHex(range.start_key()), Helper::StringToHex(range.end_key()));
      continue;
    }

    ranges.push_back(range);
    valid_regions.push_back(region);
    region_sizes.push_back(std::make_pair(region->Id(), 0));
  }

  auto raw_engine = Server::GetInstance().GetRawEngine(regions[0]->GetRawEngineType());
  auto column_family_names = Helper::GetColumnFamilyNamesExecptMetaByRole();

  // generate txn range
  std::vector<pb::common::Range> txn_ranges;
  txn_ranges.reserve(ranges.size());
  for (const auto& range : ranges) {
    pb::common::Range txn_range = Helper::GetMemComparableRange(range);
    txn_ranges.push_back(txn_range);
    DINGO_LOG(INFO) << "[metrics.region] txn range: " << Helper::StringToHex(txn_range.start_key()) << " "
                    << Helper::StringToHex(txn_range.end_key());
    DINGO_LOG(INFO) << "[metrics.region] raw range: " << Helper::StringToHex(range.start_key()) << " "
                    << Helper::StringToHex(range.end_key());
  }

  // for raw cf, use region's range to get approximate size
  // for txn cf, use txn range to get approximate size
  for (const auto& cf_name : column_family_names) {
    if (Helper::IsTxnColumnFamilyName(cf_name)) {
      auto sizes = raw_engine->GetApproximateSizes(cf_name, txn_ranges);
      for (int i = 0; i < sizes.size(); ++i) {
        region_sizes[i].second += sizes[i];
        DINGO_LOG(INFO) << "[metrics.region] txn region_size: " << sizes[i] << " region_id: " << valid_regions[i]->Id()
                        << " cf_name: " << cf_name;
      }
    } else {
      auto sizes = raw_engine->GetApproximateSizes(cf_name, ranges);
      for (int i = 0; i < sizes.size(); ++i) {
        region_sizes[i].second += sizes[i];
        DINGO_LOG(INFO) << "[metrics.region] raw region_size: " << sizes[i] << " region_id: " << valid_regions[i]->Id()
                        << " cf_name: " << cf_name;
      }
    }
  }

  return region_sizes;
}

std::vector<std::vector<store::RegionPtr>> GenBatchRegion(std::vector<store::RegionPtr> regions) {
  if (regions.size() <= Constant::kCollectApproximateSizeBatchSize) {
    return {regions};
  }

  std::vector<std::vector<store::RegionPtr>> result;
  std::vector<store::RegionPtr> batch_regions;
  batch_regions.reserve(Constant::kCollectApproximateSizeBatchSize);
  for (auto& region : regions) {
    batch_regions.push_back(region);
    if (batch_regions.size() >= Constant::kCollectApproximateSizeBatchSize) {
      result.push_back(batch_regions);
      batch_regions.clear();
    }
  }

  if (!batch_regions.empty()) {
    result.push_back(batch_regions);
  }

  return result;
}

DEFINE_int64(collect_approximate_size_log_index_interval, 10,
             "Collecting approximate size log index interval, the region has raft log index greater than this value "
             "will be collected region size");

bool StoreRegionMetrics::CollectApproximateSizeMetrics() {
  auto store_region_meta = GET_STORE_REGION_META;
  auto region_metricses = GetAllMetrics();

  std::vector<store::RegionPtr> need_collect_rocks_regions;
  std::vector<store::RegionPtr> need_collect_bdb_regions;

  for (const auto& region_metrics : region_metricses) {
    DINGO_LOG(DEBUG) << fmt::format(
        "[metrics.region] collect approximate size metrics start, region({}) log_index_id({})", region_metrics->Id(),
        region_metrics->LastLogIndex());

    auto region = store_region_meta->GetRegion(region_metrics->Id());
    if (region == nullptr) {
      DINGO_LOG(INFO) << fmt::format("[metrics.region] skip collect approximate size metrics, region({}) not exist",
                                     region_metrics->Id());
      continue;
    }

    if (region->State() != pb::common::NORMAL) {
      DINGO_LOG(INFO) << fmt::format(
          "[metrics.region] skip collect approximate size metrics for state not NORMAL, region({}) state({})",
          region->Id(), pb::common::StoreRegionState_Name(region->State()));
      continue;
    }

    // first update region_version in region metrics
    auto region_version = region->Epoch().version();
    if (region_version > region_metrics->RegionVersion()) {
      region_metrics->SetRegionVersion(region_version);
    }

    // get region log index id
    auto log_index_id = region_metrics->LastLogIndex();

    // get last update metrics log index and version
    auto last_update_metrics_log_index = region_metrics->LastUpdateMetricsLogIndex();
    auto last_update_metrics_version = region_metrics->LastUpdateMetricsVersion();

    // if the region version has been updated, update the last update metrics version, this is done in SetRegionSize
    // else if the region version is equal and the difference between the current log index and the last update
    // metrics log index is less than the interval, skip it
    if (region_version <= last_update_metrics_version &&
        (log_index_id - last_update_metrics_log_index < FLAGS_collect_approximate_size_log_index_interval)) {
      DINGO_LOG(INFO) << fmt::format(
          "[metrics.region] skip collect approximate size metrics, region({}) log_index_id({}) region_version({}) "
          "last_update_metrics_log_index({}) last_update_metrics_version({})",
          region->Id(), log_index_id, region_version, last_update_metrics_log_index, last_update_metrics_version);

      continue;
    }

    DINGO_LOG(INFO) << fmt::format(
        "[metrics.region] collect approximate size metrics, region({}) log_index_id({}) version({}) "
        "last_update_metrics_log_index({}) last_update_metrics_version({})",
        region->Id(), log_index_id, region_version, last_update_metrics_log_index, last_update_metrics_version);

    if (region->GetRawEngineType() == pb::common::RAW_ENG_ROCKSDB) {
      need_collect_rocks_regions.push_back(region);
    } else if (region->GetRawEngineType() == pb::common::RAW_ENG_BDB) {
      need_collect_bdb_regions.push_back(region);
    }
  }

  // Get approximate size rocksdb
  if (!need_collect_rocks_regions.empty()) {
    int64_t start_time = Helper::TimestampMs();
    auto batch_regions = GenBatchRegion(need_collect_rocks_regions);
    for (auto& regions : batch_regions) {
      auto region_sizes = GetRegionApproximateSize(regions);
      for (auto& item : region_sizes) {
        int64_t region_id = item.first;
        int64_t size = item.second;

        auto region_metrics = GetMetrics(region_id);
        if (region_metrics != nullptr) {
          region_metrics->SetRegionSize(size);
          region_metrics->UpdateLastUpdateMetricsLogIndex();

          DINGO_LOG(INFO) << fmt::format(
              "[metrics.region] get rocksdb region approximate size region({}) size({}) elapsed time[{} ms]", region_id,
              size, Helper::TimestampMs() - start_time);
        }
      }
    }

    DINGO_LOG(INFO) << fmt::format(
        "[metrics.region] get rocksdb region approximate size total size({}) batch size({}) elapsed time[{} ms]",
        need_collect_rocks_regions.size(), batch_regions.size(), Helper::TimestampMs() - start_time);
  }

  // Get approximate size bdb
  if (!need_collect_bdb_regions.empty()) {
    int64_t start_time = Helper::TimestampMs();
    auto batch_regions = GenBatchRegion(need_collect_bdb_regions);
    for (auto& regions : batch_regions) {
      auto region_sizes = GetRegionApproximateSize(regions);
      for (auto& item : region_sizes) {
        int64_t region_id = item.first;
        int64_t size = item.second;

        auto region_metrics = GetMetrics(region_id);
        if (region_metrics != nullptr) {
          region_metrics->SetRegionSize(size);
          region_metrics->UpdateLastUpdateMetricsLogIndex();

          DINGO_LOG(INFO) << fmt::format(
              "[metrics.region] get bdb region approximate size region({}) size({}) elapsed time[{} ms]", region_id,
              size, Helper::TimestampMs() - start_time);
        }
      }
    }

    DINGO_LOG(INFO) << fmt::format(
        "[metrics.region] get bdb region approximate size total size({}) batch size({}) elapsed time[{} ms]",
        need_collect_bdb_regions.size(), batch_regions.size(), Helper::TimestampMs() - start_time);
  }

  return true;
}

bool StoreRegionMetrics::CollectMetrics() {
  auto store_region_meta = GET_STORE_REGION_META;
  auto store_raft_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta();
  auto region_metricses = GetAllMetrics();

  std::vector<store::RegionPtr> need_collect_regions;
  for (const auto& region_metrics : region_metricses) {
    auto raft_meta = store_raft_meta->GetRaftMeta(region_metrics->Id());
    if (raft_meta == nullptr) {
      continue;
    }
    int64_t applied_index = raft_meta->AppliedId();
    if (applied_index != 0 && region_metrics->LastLogIndex() >= applied_index) {
      continue;
    }

    region_metrics->SetLastLogIndex(applied_index);

    auto region = store_region_meta->GetRegion(region_metrics->Id());
    if (region == nullptr) {
      continue;
    }
    need_collect_regions.push_back(region);

    int64_t start_time = Helper::TimestampMs();
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
    auto vector_index_wrapper = region->VectorIndexWrapper();
    auto vector_reader = engine_->NewVectorReader(region->GetRawEngine());

    if (vector_index_wrapper != nullptr && vector_reader != nullptr) {
      if (BAIDU_UNLIKELY(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE != vector_index_wrapper->Type())) {
        region_metrics->SetVectorIndexType(vector_index_wrapper->Type());

        int64_t current_count = 0;
        vector_index_wrapper->GetCount(current_count);
        region_metrics->SetVectorCurrentCount(current_count);

        int64_t deleted_count = 0;
        vector_index_wrapper->GetDeletedCount(deleted_count);
        region_metrics->SetVectorDeletedCount(deleted_count);

        int64_t max_id = 0;
        vector_reader->VectorGetBorderId(region->Range(), false, max_id);
        region_metrics->SetVectorMaxId(max_id);

        int64_t min_id = 0;
        vector_reader->VectorGetBorderId(region->Range(), true, min_id);
        region_metrics->SetVectorMinId(min_id);

        int64_t total_memory_usage = 0;
        vector_index_wrapper->GetMemorySize(total_memory_usage);
        region_metrics->SetVectorMemoryBytes(total_memory_usage);

        vector_index_has_data = true;
      }
    }

    if (vector_index_has_data) {
      DINGO_LOG(DEBUG) << fmt::format(
          "[metrics.region][region({})] collect region metrics, min_key[{}] max_key[{}] key_count[true] "
          "region_size[true]  "
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
          "[metrics.region][region({})] collect region metrics, min_key[{}] max_key[{}] key_count[true] "
          "region_size[true] elapsed[{} "
          "ms]",
          region->Id(), is_collect_min_key ? "true" : "false", is_collect_max_key ? "true" : "false",
          Helper::TimestampMs() - start_time);
    }

    meta_writer_->Put(TransformToKv(region_metrics));
  }

  return true;
}

store::RegionMetricsPtr StoreRegionMetrics::NewMetrics(int64_t region_id) {
  return std::make_shared<store::RegionMetrics>(region_id);
}

void StoreRegionMetrics::AddMetrics(store::RegionMetricsPtr metrics) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.insert_or_assign(metrics->Id(), metrics);
  }

  meta_writer_->Put(TransformToKv(metrics));
}

void StoreRegionMetrics::DeleteMetrics(int64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

store::RegionMetricsPtr StoreRegionMetrics::GetMetrics(int64_t region_id) {
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
    int64_t region_id = ParseRegionId(kv.key());
    auto region_metrics = StoreRegionMetrics::NewMetrics(region_id);
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

void StoreMetricsManager::CollectApproximateSizeMetrics() {
  if (is_collecting_approximate_size_.load()) {
    DINGO_LOG(WARNING) << "Already exist collecting approxximate size metrics.";
    return;
  }

  is_collecting_approximate_size_.store(true);

  region_metrics_->CollectApproximateSizeMetrics();

  is_collecting_approximate_size_.store(false);
}

void StoreMetricsManager::CollectStoreMetrics() {
  if (is_collecting_store_.load()) {
    DINGO_LOG(WARNING) << "Already exist collecting store metrics.";
    return;
  }

  is_collecting_store_.store(true);

  store_metrics_->CollectMetrics();

  is_collecting_store_.store(false);
}

void StoreMetricsManager::CollectStoreRegionMetrics() {
  if (is_collecting_.load()) {
    DINGO_LOG(WARNING) << "Already exist collecting store region metrics.";
    return;
  }

  is_collecting_.store(true);

  region_metrics_->CollectMetrics();

  is_collecting_.store(false);
}

}  // namespace dingodb