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

#ifndef DINGODB_VECTOR_INDEX_H_
#define DINGODB_VECTOR_INDEX_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

// Vector index abstract base class.
// One region own one vector index(region_id==vector_index_id)
// But one region can refer other vector index when region split.
class VectorIndex {
 public:
  VectorIndex(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
              uint64_t save_snapshot_threshold_write_key_num)
      : id(id),
        version(1),
        status(pb::common::VECTOR_INDEX_STATUS_NONE),
        snapshot_doing(false),
        switching_region_id(0),
        apply_log_index(0),
        snapshot_log_index(0),
        write_key_count(0),
        last_save_write_key_count(0),
        save_snapshot_threshold_write_key_num(save_snapshot_threshold_write_key_num),
        vector_index_parameter(vector_index_parameter) {
    vector_index_type = vector_index_parameter.vector_index_type();
    switching_cond = std::make_shared<BthreadCond>();
  }

  virtual ~VectorIndex() = default;

  VectorIndex(const VectorIndex& rhs) = delete;
  VectorIndex& operator=(const VectorIndex& rhs) = delete;
  VectorIndex(VectorIndex&& rhs) = delete;
  VectorIndex& operator=(VectorIndex&& rhs) = delete;

  class FilterFunctor {
   public:
    virtual ~FilterFunctor() = default;
    virtual void Build([[maybe_unused]] std::vector<faiss::idx_t>& id_map) {}
    virtual bool Check(uint64_t vector_id) = 0;
  };

  // Range filter
  class RangeFilterFunctor : public FilterFunctor {
   public:
    RangeFilterFunctor(uint64_t min_vector_id, uint64_t max_vector_id)
        : min_vector_id_(min_vector_id), max_vector_id_(max_vector_id) {}
    bool Check(uint64_t vector_id) override { return vector_id >= min_vector_id_ && vector_id < max_vector_id_; }

   private:
    uint64_t min_vector_id_;
    uint64_t max_vector_id_;
  };

  // Range filter just for flat
  // Range transform list
  class FlatRangeFilterFunctor : public FilterFunctor {
   public:
    FlatRangeFilterFunctor(uint64_t min_vector_id, uint64_t max_vector_id)
        : min_vector_id_(min_vector_id), max_vector_id_(max_vector_id) {}

    void Build(std::vector<faiss::idx_t>& id_map) override { this->id_map_ = &id_map; }

    bool Check(uint64_t index) override {
      return (*id_map_)[index] >= min_vector_id_ && (*id_map_)[index] < max_vector_id_;
    }

   private:
    uint64_t min_vector_id_;
    uint64_t max_vector_id_;
    std::vector<faiss::idx_t>* id_map_{nullptr};
  };

  // List filter
  // be careful not to use the parent class to release,
  // otherwise there will be memory leaks
  class HnswListFilterFunctor : public FilterFunctor {
   public:
    HnswListFilterFunctor(const HnswListFilterFunctor&) = delete;
    HnswListFilterFunctor(HnswListFilterFunctor&&) = delete;
    HnswListFilterFunctor& operator=(const HnswListFilterFunctor&) = delete;
    HnswListFilterFunctor& operator=(HnswListFilterFunctor&&) = delete;

    explicit HnswListFilterFunctor(const std::vector<uint64_t>& vector_ids) {
      for (auto vector_id : vector_ids) {
        vector_ids_.insert(vector_id);
      }
    }

    ~HnswListFilterFunctor() override = default;

    bool Check(uint64_t vector_id) override { return vector_ids_.find(vector_id) != vector_ids_.end(); }

   private:
    std::unordered_set<uint64_t> vector_ids_;
  };

  // List filter just for flat
  class FlatListFilterFunctor : public FilterFunctor {
   public:
    FlatListFilterFunctor(std::vector<uint64_t>&& vector_ids)
        : vector_ids_(std::forward<std::vector<uint64_t>>(vector_ids)) {}
    FlatListFilterFunctor(const FlatListFilterFunctor&) = delete;
    FlatListFilterFunctor(FlatListFilterFunctor&&) = delete;
    FlatListFilterFunctor& operator=(const FlatListFilterFunctor&) = delete;
    FlatListFilterFunctor& operator=(FlatListFilterFunctor&&) = delete;

    void Build(std::vector<faiss::idx_t>& id_map) override {
      this->id_map_ = &id_map;

      for (auto vector_id : vector_ids_) {
        array_indexs_.insert(vector_id);
      }
    }

    bool Check(uint64_t index) override {
      if (index >= (*id_map_).size()) {
        return false;
      }

      auto vector_id = (*id_map_)[index];
      return array_indexs_.find(vector_id) != array_indexs_.end();
    }

   private:
    std::vector<uint64_t> vector_ids_;
    std::unordered_set<uint64_t> array_indexs_;
    std::vector<faiss::idx_t>* id_map_{nullptr};
  };

  pb::common::VectorIndexType VectorIndexType() const;

  virtual int32_t GetDimension() = 0;
  virtual butil::Status GetCount([[maybe_unused]] uint64_t& count);
  virtual butil::Status GetDeletedCount([[maybe_unused]] uint64_t& deleted_count);
  virtual butil::Status GetMemorySize([[maybe_unused]] uint64_t& memory_size);

  virtual butil::Status NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                      [[maybe_unused]] uint64_t last_save_log_behind);
  virtual butil::Status NeedToSave([[maybe_unused]] bool& need_to_save, [[maybe_unused]] uint64_t last_save_log_behind);

  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Delete([[maybe_unused]] const std::vector<uint64_t>& delete_ids) = 0;

  virtual butil::Status Save([[maybe_unused]] const std::string& path);

  virtual butil::Status Load([[maybe_unused]] const std::string& path);

  virtual butil::Status Search([[maybe_unused]] std::vector<pb::common::VectorWithId> vector_with_ids,
                               [[maybe_unused]] uint32_t topk,
                               [[maybe_unused]] std::vector<std::shared_ptr<FilterFunctor>> filters,
                               std::vector<pb::index::VectorWithDistanceResult>& results,
                               [[maybe_unused]] bool reconstruct = false) = 0;

  virtual void LockWrite() = 0;
  virtual void UnlockWrite() = 0;

  uint64_t Id() const { return id; }

  uint64_t Version() const { return version; }
  void SetVersion(uint64_t version) { this->version = version; }

  pb::common::RegionVectorIndexStatus Status() { return status.load(); }
  void SetStatus(pb::common::RegionVectorIndexStatus status) {
    if (this->status.load() != pb::common::VECTOR_INDEX_STATUS_DELETE) {
      this->status.store(status);
    }
  }

  uint64_t SwitchingRegionId() { return switching_region_id.load(); }

  void SetSwitchingRegionId(uint64_t expected, uint64_t switching_region_id) {
    while (!this->switching_region_id.compare_exchange_weak(expected, switching_region_id)) {
      switching_cond->IncreaseWait();
    }
    if (switching_cond->Count() > 0) {
      switching_cond->DecreaseSignal();
    }
  }

  bool SnapshotDoing() { return snapshot_doing.load(std::memory_order_relaxed); }
  void SetSnapshotDoing(bool doing) { snapshot_doing.store(doing, std::memory_order_relaxed); }

  uint64_t ApplyLogIndex() const;
  void SetApplyLogIndex(uint64_t apply_log_index);

  uint64_t SnapshotLogIndex() const;
  void SetSnapshotLogIndex(uint64_t snapshot_log_index);

 protected:
  // vector index id
  uint64_t id;

  // vector index version
  uint64_t version;

  // status
  std::atomic<pb::common::RegionVectorIndexStatus> status;

  // control do snapshot concurrency
  std::atomic<bool> snapshot_doing;

  // current switching region id
  std::atomic<uint64_t> switching_region_id;
  // protect switching_region_id
  std::shared_ptr<BthreadCond> switching_cond;

  // apply max log index
  std::atomic<uint64_t> apply_log_index;
  // last snapshot log index
  std::atomic<uint64_t> snapshot_log_index;

  // write(add/update/delete) key count
  uint64_t write_key_count;
  uint64_t last_save_write_key_count;
  // save snapshot threshold write key num
  uint64_t save_snapshot_threshold_write_key_num;

  pb::common::VectorIndexType vector_index_type;

  pb::common::VectorIndexParameter vector_index_parameter;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
