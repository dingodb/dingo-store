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
#include <utility>
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
  VectorIndex(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
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
    virtual bool Check(int64_t vector_id) = 0;
  };

  // Range filter
  class RangeFilterFunctor : public FilterFunctor {
   public:
    RangeFilterFunctor(int64_t min_vector_id, int64_t max_vector_id)
        : min_vector_id_(min_vector_id), max_vector_id_(max_vector_id) {}
    bool Check(int64_t vector_id) override { return vector_id >= min_vector_id_ && vector_id < max_vector_id_; }

   private:
    int64_t min_vector_id_;
    int64_t max_vector_id_;
  };

  // Range filter just for flat
  // Range transform list
  class FlatRangeFilterFunctor : public FilterFunctor {
   public:
    FlatRangeFilterFunctor(int64_t min_vector_id, int64_t max_vector_id)
        : min_vector_id_(min_vector_id), max_vector_id_(max_vector_id) {}

    void Build(std::vector<faiss::idx_t>& id_map) override { this->id_map_ = &id_map; }

    bool Check(int64_t index) override {
      return (*id_map_)[index] >= min_vector_id_ && (*id_map_)[index] < max_vector_id_;
    }

   private:
    int64_t min_vector_id_;
    int64_t max_vector_id_;
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

    explicit HnswListFilterFunctor(const std::vector<int64_t>& vector_ids) {
      for (auto vector_id : vector_ids) {
        vector_ids_.insert(vector_id);
      }
    }

    ~HnswListFilterFunctor() override = default;

    bool Check(int64_t vector_id) override { return vector_ids_.find(vector_id) != vector_ids_.end(); }

   private:
    std::unordered_set<int64_t> vector_ids_;
  };

  class FlatListFilterFunctor : public FilterFunctor {
   public:
    explicit FlatListFilterFunctor(std::vector<int64_t>&& vector_ids)  // NOLINT
        : vector_ids_(std::forward<std::vector<int64_t>>(vector_ids)) {
      // highly optimized code, do not modify it
      array_indexs_.rehash(vector_ids_.size());
      array_indexs_.insert(vector_ids_.begin(), vector_ids_.end());
    }
    FlatListFilterFunctor(const FlatListFilterFunctor&) = delete;
    FlatListFilterFunctor(FlatListFilterFunctor&&) = delete;
    FlatListFilterFunctor& operator=(const FlatListFilterFunctor&) = delete;
    FlatListFilterFunctor& operator=(FlatListFilterFunctor&&) = delete;

    bool Check(int64_t index) override { return array_indexs_.find(index) != array_indexs_.end(); }

   private:
    std::vector<int64_t> vector_ids_;
    std::unordered_set<int64_t> array_indexs_;
  };

  // List filter just for ivf flat
  class IvfFlatListFilterFunctor : public FilterFunctor {
   public:
    explicit IvfFlatListFilterFunctor(std::vector<int64_t>&& vector_ids)  // NOLINT
        : vector_ids_(std::forward<std::vector<int64_t>>(vector_ids)) {
      // highly optimized code, do not modify it
      array_indexs_.rehash(vector_ids_.size());
      array_indexs_.insert(vector_ids_.begin(), vector_ids_.end());
    }
    IvfFlatListFilterFunctor(const IvfFlatListFilterFunctor&) = delete;
    IvfFlatListFilterFunctor(IvfFlatListFilterFunctor&&) = delete;
    IvfFlatListFilterFunctor& operator=(const IvfFlatListFilterFunctor&) = delete;
    IvfFlatListFilterFunctor& operator=(IvfFlatListFilterFunctor&&) = delete;

    bool Check(int64_t index) override { return array_indexs_.find(index) != array_indexs_.end(); }

   private:
    std::vector<int64_t> vector_ids_;
    std::unordered_set<int64_t> array_indexs_;
  };

  virtual int32_t GetDimension() = 0;
  virtual butil::Status GetCount([[maybe_unused]] int64_t& count);
  virtual butil::Status GetDeletedCount([[maybe_unused]] int64_t& deleted_count);
  virtual butil::Status GetMemorySize([[maybe_unused]] int64_t& memory_size);
  virtual bool IsExceedsMaxElements() = 0;

  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Delete([[maybe_unused]] const std::vector<int64_t>& delete_ids) = 0;

  virtual butil::Status Save([[maybe_unused]] const std::string& path);

  virtual butil::Status Load([[maybe_unused]] const std::string& path);

  virtual butil::Status Search([[maybe_unused]] std::vector<pb::common::VectorWithId> vector_with_ids,
                               [[maybe_unused]] uint32_t topk,
                               [[maybe_unused]] std::vector<std::shared_ptr<FilterFunctor>> filters,
                               std::vector<pb::index::VectorWithDistanceResult>& results,  // NOLINT
                               [[maybe_unused]] bool reconstruct = false,
                               [[maybe_unused]] const pb::common::VectorSearchParameter& parameter = {}) = 0;

  virtual void LockWrite() = 0;
  virtual void UnlockWrite() = 0;
  virtual butil::Status Train(const std::vector<float>& train_datas) = 0;
  virtual butil::Status Train(const std::vector<pb::common::VectorWithId>& vectors) = 0;
  virtual bool NeedToRebuild() = 0;
  virtual bool NeedTrain() { return false; }
  virtual bool IsTrained() { return true; }
  virtual bool SupportSave() { return false; }

  virtual uint32_t WriteOpParallelNum() { return 1; }

  int64_t Id() const { return id; }

  pb::common::VectorIndexType VectorIndexType() { return vector_index_type; }

  int64_t ApplyLogId() const;
  void SetApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId() const;
  void SetSnapshotLogId(int64_t snapshot_log_id);

  pb::common::Range Range() { return range; }

 protected:
  // vector index id
  int64_t id;
  // vector index type, e.g. hnsw/flat
  pb::common::VectorIndexType vector_index_type;

  // apply max log id
  std::atomic<int64_t> apply_log_id;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id;

  pb::common::Range range;

  pb::common::VectorIndexParameter vector_index_parameter;
};

using VectorIndexPtr = std::shared_ptr<VectorIndex>;

class VectorIndexWrapper : public std::enable_shared_from_this<VectorIndexWrapper> {
 public:
  VectorIndexWrapper(int64_t id, pb::common::VectorIndexParameter index_parameter,
                     int64_t save_snapshot_threshold_write_key_num)
      : id_(id),
        vector_index_type_(index_parameter.vector_index_type()),
        ready_(false),
        stop_(false),
        is_switching_vector_index_(false),
        apply_log_id_(0),
        snapshot_log_id_(0),
        active_index_(0),
        index_parameter_(index_parameter),
        is_hold_vector_index_(false),
        pending_task_num_(0),
        save_snapshot_threshold_write_key_num_(save_snapshot_threshold_write_key_num) {
    worker_ = Worker::New();
    snapshot_set_ = vector_index::SnapshotMetaSet::New(id);
    bthread_mutex_init(&vector_index_mutex_, nullptr);
    is_loadorbuilding_.store(false);
    is_rebuilding_.store(false);
  }
  ~VectorIndexWrapper();

  static std::shared_ptr<VectorIndexWrapper> New(int64_t id, pb::common::VectorIndexParameter index_parameter);

  std::shared_ptr<VectorIndexWrapper> GetSelf();

  bool Init();
  void Destroy();
  bool Recover();

  butil::Status SaveMeta();
  butil::Status LoadMeta();

  int64_t Id() const { return id_; }

  int64_t Version() const { return version_; }
  void SetVersion(int64_t version) { version_ = version; }

  bool IsReady() { return ready_.load(); }
  bool IsStop() { return stop_.load(); }
  bool IsOwnReady() { return GetOwnVectorIndex() != nullptr; }

  bool IsLoadorbuilding() { return is_loadorbuilding_.load(); }
  void SetLoadoruilding(bool is_building) { return is_loadorbuilding_.store(is_building); }

  bool IsRebuilding() { return is_rebuilding_.load(); }
  void SetIsRebuilding(bool is_building) { return is_rebuilding_.store(is_building); }

  bool IsBuildError() { return build_error_.load(); }

  bool IsRebuildError() { return rebuild_error_.load(); }

  bool SetBuildError() {
    build_error_.store(true);
    return build_error_.load();
  }

  bool SetBuildSuccess() {
    build_error_.store(false);
    return build_error_.load();
  }

  bool SetRebuildError() {
    rebuild_error_.store(true);
    return rebuild_error_.load();
  }

  bool SetRebuildSuccess() {
    rebuild_error_.store(false);
    return rebuild_error_.load();
  }

  pb::common::VectorIndexType Type() { return vector_index_type_; }

  pb::common::VectorIndexParameter IndexParameter() { return index_parameter_; }

  int64_t ApplyLogId();
  void SetApplyLogId(int64_t apply_log_id);
  void SaveApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId();
  void SetSnapshotLogId(int64_t snapshot_log_id);
  void SaveSnapshotLogId(int64_t snapshot_log_id);

  bool IsSwitchingVectorIndex();
  void SetIsSwitchingVectorIndex(bool is_switching);

  bool IsHoldVectorIndex() const;
  void SetIsHoldVectorIndex(bool need);

  vector_index::SnapshotMetaSetPtr SnapshotSet() {
    BAIDU_SCOPED_LOCK(vector_index_mutex_);
    return snapshot_set_;
  }

  void UpdateVectorIndex(VectorIndexPtr vector_index, const std::string& reason);
  void ClearVectorIndex();

  VectorIndexPtr GetOwnVectorIndex();
  VectorIndexPtr GetVectorIndex();

  VectorIndexPtr ShareVectorIndex();
  void SetShareVectorIndex(VectorIndexPtr vector_index);

  bool ExecuteTask(TaskRunnablePtr task);

  int PendingTaskNum();
  void IncPendingTaskNum();
  void DecPendingTaskNum();

  int32_t GetDimension();
  butil::Status GetCount(int64_t& count);
  butil::Status GetDeletedCount(int64_t& deleted_count);
  butil::Status GetMemorySize(int64_t& memory_size);
  bool IsExceedsMaxElements();

  bool NeedToRebuild();
  bool NeedToSave(int64_t last_save_log_behind);
  bool SupportSave();

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Delete(const std::vector<int64_t>& delete_ids);
  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       const pb::common::Range& region_range,
                       std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                       std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct = false,
                       const pb::common::VectorSearchParameter& parameter = {});

 private:
  // vector index id
  int64_t id_;
  // vector index version
  int64_t version_{0};
  // vector index is ready
  std::atomic<bool> ready_;
  // vector is loadorbuilding
  std::atomic<bool> is_loadorbuilding_;
  // vector is rebuilding
  std::atomic<bool> is_rebuilding_;
  // stop vector index
  std::atomic<bool> stop_;
  // vector index build status
  std::atomic<bool> build_error_{false};
  // vector index rebuild status
  std::atomic<bool> rebuild_error_{false};
  // status
  std::atomic<pb::common::RegionVectorIndexStatus> status_;
  // vector index type, e.g. hnsw/flat
  pb::common::VectorIndexType vector_index_type_;

  // vector index definition parameter
  pb::common::VectorIndexParameter index_parameter_;

  // apply max log id
  std::atomic<int64_t> apply_log_id_;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id_;

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
  std::atomic<int> pending_task_num_;

  // write(add/update/delete) key count
  int64_t write_key_count_{0};
  int64_t last_save_write_key_count_{0};
  // save snapshot threshold write key num
  int64_t save_snapshot_threshold_write_key_num_;

  // need hold vector index
  std::atomic<bool> is_hold_vector_index_;
};

using VectorIndexWrapperPtr = std::shared_ptr<VectorIndexWrapper>;

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
