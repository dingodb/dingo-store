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

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "common/constant.h"
#include "engine/raw_engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/store_meta_manager.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"

namespace dingodb {

namespace store {

class RegionMetrics {
 public:
  RegionMetrics()
      : last_log_index_(0), need_update_min_key_(true), need_update_max_key_(true), need_update_key_count_(true) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~RegionMetrics() { bthread_mutex_destroy(&mutex_); }

  std::string Serialize();
  void DeSerialize(const std::string& data);

  int64_t LastLogIndex() {
    BAIDU_SCOPED_LOCK(mutex_);
    return last_log_index_;
  }
  void SetLastLogIndex(int64_t last_log_index) { last_log_index_ = last_log_index; }

  bool NeedUpdateMinKey() {
    BAIDU_SCOPED_LOCK(mutex_);
    return need_update_min_key_;
  }
  void SetNeedUpdateMinKey(bool need_update_min_key) {
    BAIDU_SCOPED_LOCK(mutex_);
    need_update_min_key_ = need_update_min_key;
  }

  bool NeedUpdateMaxKey() {
    BAIDU_SCOPED_LOCK(mutex_);
    return need_update_max_key_;
  }
  void SetNeedUpdateMaxKey(bool need_update_max_key) { need_update_max_key_ = need_update_max_key; }

  bool NeedUpdateKeyCount() {
    BAIDU_SCOPED_LOCK(mutex_);
    return need_update_key_count_;
  }
  void SetNeedUpdateKeyCount(bool need_update_key_count) {
    BAIDU_SCOPED_LOCK(mutex_);
    need_update_key_count_ = need_update_key_count;
  }

  int64_t Id() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.id();
  }
  void SetId(int64_t region_id) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.set_id(region_id);
  }

  const std::string& MinKey() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.min_key();
  }
  void SetMinKey(const std::string& min_key) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.set_min_key(min_key);
  }

  const std::string& MaxKey() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.max_key();
  }
  void SetMaxKey(const std::string& max_key) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.set_max_key(max_key);
  }

  int64_t RegionSize() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.region_size();
  }
  void SetRegionSize(int64_t region_size) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.set_region_size(region_size);
  }

  int64_t KeyCount() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.row_count();
  }
  void SetKeyCount(int64_t key_count) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.set_row_count(key_count);
  }

  // vector index start
  pb::common::VectorIndexType GetVectorIndexType() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().vector_index_type();
  }

  void SetVectorIndexType(pb::common::VectorIndexType vector_index_type) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_vector_index_type(vector_index_type);
  }

  int64_t GetVectorCurrentCount() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().current_count();
  }

  void SetVectorCurrentCount(int64_t current_count) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_current_count(current_count);
  }

  int64_t GetVectorDeletedCount() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().deleted_count();
  }

  void SetVectorDeletedCount(int64_t deleted_count) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_deleted_count(deleted_count);
  }

  int64_t GetVectorMaxId() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().max_id();
  }

  void SetVectorMaxId(int64_t max_id) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_max_id(max_id);
  }

  int64_t GetVectorMinId() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().min_id();
  }

  void SetVectorMinId(int64_t min_id) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_min_id(min_id);
  }

  int64_t GetVectorMemoryBytes() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_.vector_index_metrics().memory_bytes();
  }

  void SetVectorMemoryBytes(int64_t memory_bytes) {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_metrics_.mutable_vector_index_metrics()->set_memory_bytes(memory_bytes);
  }

  // vector index end

  const pb::common::RegionMetrics& InnerRegionMetrics() {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_metrics_;
  }

  using PbKeyValues = google::protobuf::RepeatedPtrField<pb::common::KeyValue>;
  using PbKeys = google::protobuf::RepeatedPtrField<std::string>;
  using PbRanges = google::protobuf::RepeatedPtrField<pb::common::Range>;

  void UpdateMaxAndMinKey(const PbKeyValues& kvs);
  void UpdateMaxAndMinKeyPolicy(const PbKeys& keys);
  void UpdateMaxAndMinKeyPolicy(const PbRanges& ranges);
  void UpdateMaxAndMinKeyPolicy();

 private:
  // update metrics until raft log index
  int64_t last_log_index_;
  // need update region min key
  bool need_update_min_key_;
  // need update region max key
  bool need_update_max_key_;
  // need update region key count
  bool need_update_key_count_;

  pb::common::RegionMetrics inner_region_metrics_;
  // protect inner_region_metrics_
  bthread_mutex_t mutex_;
};

using RegionMetricsPtr = std::shared_ptr<RegionMetrics>;

}  // namespace store

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
  StoreRegionMetrics(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<MetaReader> meta_reader,
                     std::shared_ptr<MetaWriter> meta_writer, std::shared_ptr<Engine> engine)
      : TransformKvAble(Constant::kStoreRegionMetricsPrefix),
        raw_engine_(raw_engine),
        meta_reader_(meta_reader),
        meta_writer_(meta_writer),
        engine_(engine) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRegionMetrics() override { bthread_mutex_destroy(&mutex_); }

  StoreRegionMetrics(const StoreRegionMetrics&) = delete;
  const StoreRegionMetrics& operator=(const StoreRegionMetrics&) = delete;

  bool Init();

  // Only collect approximate size metrics.
  bool CollectApproximateSizeMetrics();
  // Collect other metrics, e.g. min_key/max_key/key_count.
  bool CollectMetrics();

  static store::RegionMetricsPtr NewMetrics(int64_t region_id);

  void AddMetrics(store::RegionMetricsPtr metrics);
  void DeleteMetrics(int64_t region_id);
  store::RegionMetricsPtr GetMetrics(int64_t region_id);
  std::vector<store::RegionMetricsPtr> GetAllMetrics();

  std::string GetRegionMinKey(store::RegionPtr region);
  std::string GetRegionMaxKey(store::RegionPtr region);

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Todo: later optimize
  int64_t GetRegionKeyCount(store::RegionPtr region);
  std::vector<std::pair<int64_t, int64_t>> GetRegionApproximateSize(std::vector<store::RegionPtr> regions);

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  std::shared_ptr<RawEngine> raw_engine_;

  std::shared_ptr<Engine> engine_;
  bthread_mutex_t mutex_;
  std::map<int64_t, store::RegionMetricsPtr> metricses_;
};

class StoreMetricsManager {
 public:
  explicit StoreMetricsManager(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<MetaReader> meta_reader,
                               std::shared_ptr<MetaWriter> meta_writer, std::shared_ptr<Engine> engine)
      : is_collecting_(false),
        is_collecting_store_(false),
        is_collecting_approximate_size_(false),
        store_metrics_(std::make_shared<StoreMetrics>()),
        region_metrics_(std::make_shared<StoreRegionMetrics>(raw_engine, meta_reader, meta_writer, engine)) {}
  ~StoreMetricsManager() = default;

  StoreMetricsManager(const StoreMetricsManager&) = delete;
  void operator=(const StoreMetricsManager&) = delete;

  bool Init();

  void CollectApproximateSizeMetrics();
  void CollectStoreMetrics();
  void CollectStoreRegionMetrics();

  std::shared_ptr<StoreMetrics> GetStoreMetrics() { return store_metrics_; }
  std::shared_ptr<StoreRegionMetrics> GetStoreRegionMetrics() { return region_metrics_; }

 private:
  // Is collecting metrics, just one collecting at the same time.
  std::atomic<bool> is_collecting_;
  std::atomic<bool> is_collecting_store_;
  std::atomic<bool> is_collecting_approximate_size_;
  std::shared_ptr<StoreMetrics> store_metrics_;
  std::shared_ptr<StoreRegionMetrics> region_metrics_;
};

}  // namespace dingodb

#endif  // DINGODB_STROE_METRICS_MANAGER_H_