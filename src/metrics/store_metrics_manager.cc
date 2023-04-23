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

#include "common/helper.h"
#include "config/config_manager.h"
#include "server/server.h"

namespace dingodb {

bool StoreMetrics::Init() { return CollectMetrics(); }

bool StoreMetrics::CollectMetrics() {
  std::map<std::string, uint64_t> output;

  auto config = ConfigManager::GetInstance()->GetConfig(pb::common::ClusterRole::STORE);

  if (!Helper::GetDiskCapacity(config->GetString("store.dbPath"), output)) {
    return false;
  }

  metrics_->set_total_capacity(output["TotalCapacity"]);
  metrics_->set_free_capacity(output["FreeCcapacity"]);

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

bool StoreRegionMetrics::CollectMetrics() { return true; }

std::shared_ptr<pb::common::RegionMetrics> StoreRegionMetrics::NewMetrics(uint64_t region_id) {
  auto metrics = std::make_shared<pb::common::RegionMetrics>();
  metrics->set_id(region_id);
  return metrics;
}

void StoreRegionMetrics::AddMetrics(std::shared_ptr<pb::common::RegionMetrics> metrics) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.insert_or_assign(metrics->id(), metrics);
  }

  meta_writer_->Put(TransformToKv(&metrics));
}

void StoreRegionMetrics::DeleteMetrics(uint64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    metricses_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

std::shared_ptr<pb::common::RegionMetrics> StoreRegionMetrics::GetMetrics(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = metricses_.find(region_id);
  if (it == metricses_.end()) {
    return nullptr;
  }

  return it->second;
}

std::vector<std::shared_ptr<pb::common::RegionMetrics>> StoreRegionMetrics::GetAllMetrics() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::common::RegionMetrics>> metricses;
  metricses.reserve(metricses_.size());
  for (auto [_, metrics] : metricses_) {
    metricses.push_back(metrics);
  }

  return metricses;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMetrics::TransformToKv(void* obj) {
  auto region_metrics = *static_cast<std::shared_ptr<pb::common::RegionMetrics>*>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region_metrics->id()));
  kv->set_value(region_metrics->SerializeAsString());

  return kv;
}

void StoreRegionMetrics::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    auto region_metrics = std::make_shared<pb::common::RegionMetrics>();
    region_metrics->ParsePartialFromArray(kv.value().data(), kv.value().size());
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
  store_metrics_->CollectMetrics();
  region_metrics_->CollectMetrics();
}

}  // namespace dingodb