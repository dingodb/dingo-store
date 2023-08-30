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
#include "common/runnable.h"
#include "common/synchronization.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_snapshot.h"

namespace dingodb {

// Vector index abstract base class.
// One region own one vector index(region_id==vector_index_id)
// But one region can refer other vector index when region split.
class VectorIndex {
 public:
  VectorIndex(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
              const pb::common::Range& range)
      : id(id), apply_log_id(0), snapshot_log_id(0), vector_index_parameter(vector_index_parameter), range(range) {
    vector_index_type = vector_index_parameter.vector_index_type();
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

  virtual int32_t GetDimension() = 0;
  virtual butil::Status GetCount([[maybe_unused]] uint64_t& count);
  virtual butil::Status GetDeletedCount([[maybe_unused]] uint64_t& deleted_count);
  virtual butil::Status GetMemorySize([[maybe_unused]] uint64_t& memory_size);
  virtual bool IsExceedsMaxElements() = 0;

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

  pb::common::VectorIndexType VectorIndexType() { return vector_index_type; }

  uint64_t ApplyLogId() const;
  void SetApplyLogId(uint64_t apply_log_id);

  uint64_t SnapshotLogId() const;
  void SetSnapshotLogId(uint64_t snapshot_log_id);

  pb::common::Range Range() { return range; }

 protected:
  // vector index id
  uint64_t id;
  // vector index type, e.g. hnsw/flat
  pb::common::VectorIndexType vector_index_type;

  // apply max log id
  std::atomic<uint64_t> apply_log_id;
  // last snapshot log id
  std::atomic<uint64_t> snapshot_log_id;

  pb::common::Range range;

  pb::common::VectorIndexParameter vector_index_parameter;
};

using VectorIndexPtr = std::shared_ptr<VectorIndex>;

class VectorIndexWrapper {
 public:
  VectorIndexWrapper(uint64_t id, pb::common::VectorIndexParameter index_parameter,
                     int64_t save_snapshot_threshold_write_key_num)
      : id_(id),
        version_(0),
        vector_index_type_(index_parameter.vector_index_type()),
        ready_(false),
        stop_(false),
        is_switching_vector_index_(false),
        apply_log_id_(0),
        snapshot_log_id_(0),
        active_index_(0),
        index_parameter_(index_parameter),
        write_key_count_(0),
        last_save_write_key_count_(0),
        save_snapshot_threshold_write_key_num_(save_snapshot_threshold_write_key_num) {
    worker_ = Worker::New();
    snapshot_set_ = vector_index::SnapshotMetaSet::New(id);
    bthread_mutex_init(&vector_index_mutex_, nullptr);
  }
  ~VectorIndexWrapper();

  static std::shared_ptr<VectorIndexWrapper> New(uint64_t id, pb::common::VectorIndexParameter index_parameter);

  bool Init();
  void Destroy();
  bool Recover();

  butil::Status SaveMeta();
  butil::Status LoadMeta();

  uint64_t Id() const { return id_; }

  uint64_t Version() const { return version_; }
  void SetVersion(uint64_t version) { version_ = version; }

  bool IsReady() { return ready_.load(); }

  bool IsStop() { return stop_.load(); }

  pb::common::VectorIndexType Type() { return vector_index_type_; }

  pb::common::VectorIndexParameter IndexParameter() { return index_parameter_; }

  uint64_t ApplyLogId();
  void SetApplyLogId(uint64_t apply_log_id);
  void SaveApplyLogId(uint64_t apply_log_id);

  uint64_t SnapshotLogId();
  void SetSnapshotLogId(uint64_t snapshot_log_id);
  void SaveSnapshotLogId(uint64_t snapshot_log_id);

  bool IsSwitchingVectorIndex();
  void SetIsSwitchingVectorIndex(bool is_switching);

  vector_index::SnapshotMetaSetPtr SnapshotSet() { return snapshot_set_; }

  void UpdateVectorIndex(VectorIndexPtr vector_index);
  void ClearVectorIndex();

  VectorIndexPtr GetOwnVectorIndex();
  VectorIndexPtr GetVectorIndex();

  VectorIndexPtr ShareVectorIndex();
  void SetShareVectorIndex(VectorIndexPtr vector_index);

  bool ExecuteTask(TaskRunnable* task);

  int32_t GetDimension();
  butil::Status GetCount(uint64_t& count);
  butil::Status GetDeletedCount(uint64_t& deleted_count);
  butil::Status GetMemorySize(uint64_t& memory_size);
  bool IsExceedsMaxElements();

  bool NeedToRebuild();
  bool NeedToSave(uint64_t last_save_log_behind);

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Delete(const std::vector<uint64_t>& delete_ids);
  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       const pb::common::Range& region_range,
                       std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                       std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct = false);

 private:
  // vector index id
  uint64_t id_;
  // vector index version
  uint64_t version_;
  // vector index is ready
  std::atomic<bool> ready_;
  // stop vector index
  std::atomic<bool> stop_;
  // status
  std::atomic<pb::common::RegionVectorIndexStatus> status_;
  // vector index type, e.g. hnsw/flat
  pb::common::VectorIndexType vector_index_type_;

  // vector index definition parameter
  pb::common::VectorIndexParameter index_parameter_;

  // apply max log id
  std::atomic<uint64_t> apply_log_id_;
  // last snapshot log id
  std::atomic<uint64_t> snapshot_log_id_;

  // Indicate switching vector index.
  std::atomic<bool> is_switching_vector_index_;

  // Active vector index position
  std::atomic<int> active_index_;
  VectorIndexPtr vector_indexs_[2] = {nullptr, nullptr};
  bthread_mutex_t vector_index_mutex_;

  // Share other vector index.
  VectorIndexPtr share_vector_index_;

  // Snapshot set
  vector_index::SnapshotMetaSetPtr snapshot_set_;

  // Run long time task, e.g. rebuild
  WorkerPtr worker_;

  // write(add/update/delete) key count
  uint64_t write_key_count_;
  uint64_t last_save_write_key_count_;
  // save snapshot threshold write key num
  uint64_t save_snapshot_threshold_write_key_num_;
};

using VectorIndexWrapperPtr = std::shared_ptr<VectorIndexWrapper>;

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
