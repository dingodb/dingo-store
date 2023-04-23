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

#ifndef DINGODB_STROE_METRICS_MANAGER_H_
#define DINGODB_STROE_METRICS_MANAGER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "common/constant.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

class StoreMetrics {
 public:
  explicit StoreMetrics() : metrics_(std::make_shared<pb::common::StoreMetrics>()) {}
  ~StoreMetrics() = default;

  StoreMetrics(const StoreMetrics&) = delete;
  const StoreMetrics& operator=(const StoreMetrics&) = delete;

  bool Init();

  bool CollectMetrics();

  std::shared_ptr<pb::common::StoreMetrics> Metrics() { return metrics_; }

 private:
  std::shared_ptr<pb::common::StoreMetrics> metrics_;
};

class StoreRegionMetrics : public TransformKvAble {
 public:
  StoreRegionMetrics(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRegionMetricsPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRegionMetrics() override = default;

  StoreRegionMetrics(const StoreRegionMetrics&) = delete;
  const StoreRegionMetrics& operator=(const StoreRegionMetrics&) = delete;

  bool Init();

  bool CollectMetrics();

  static std::shared_ptr<pb::common::RegionMetrics> NewMetrics(uint64_t region_id);

  void AddMetrics(std::shared_ptr<pb::common::RegionMetrics> metrics);
  void DeleteMetrics(uint64_t region_id);
  std::shared_ptr<pb::common::RegionMetrics> GetMetrics(uint64_t region_id);
  std::vector<std::shared_ptr<pb::common::RegionMetrics>> GetAllMetrics();

  std::shared_ptr<pb::common::KeyValue> TransformToKv(void* obj) override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;
  std::map<uint64_t, std::shared_ptr<pb::common::RegionMetrics>> metricses_;
};

class StoreMetricsManager {
 public:
  explicit StoreMetricsManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : store_metrics_(std::make_shared<StoreMetrics>()),
        region_metrics_(std::make_shared<StoreRegionMetrics>(meta_reader, meta_writer)) {}
  ~StoreMetricsManager() = default;

  StoreMetricsManager(const StoreMetricsManager&) = delete;
  void operator=(const StoreMetricsManager&) = delete;

  bool Init();

  void CollectMetrics();

  std::shared_ptr<StoreMetrics> GetStoreMetrics() { return store_metrics_; }
  std::shared_ptr<StoreRegionMetrics> GetStoreRegionMetrics() { return region_metrics_; }

 private:
  std::shared_ptr<StoreMetrics> store_metrics_;
  std::shared_ptr<StoreRegionMetrics> region_metrics_;
};

}  // namespace dingodb

#endif  // DINGODB_STROE_METRICS_MANAGER_H_