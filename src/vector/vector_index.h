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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/runnable.h"
#include "common/threadpool.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_snapshot.h"

namespace dingodb {

// Vector index abstract base class.
// One region own one vector index(region_id==vector_index_id)
// But one region can refer other vector index when region split.
class VectorIndex {
 public:
  VectorIndex(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
              const pb::common::RegionEpoch& epoch, const pb::common::Range& range, ThreadPoolPtr thread_pool);
  virtual ~VectorIndex();

  VectorIndex(const VectorIndex& rhs) = delete;
  VectorIndex& operator=(const VectorIndex& rhs) = delete;
  VectorIndex(VectorIndex&& rhs) = delete;
  VectorIndex& operator=(VectorIndex&& rhs) = delete;

  class FilterFunctor {
   public:
    virtual ~FilterFunctor() = default;
    virtual void Build(std::vector<faiss::idx_t>& id_map) {}
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

  class ConcreteFilterFunctor : public FilterFunctor, public faiss::IDSelectorBatch {
   public:
    ConcreteFilterFunctor(const ConcreteFilterFunctor&) = delete;
    ConcreteFilterFunctor(ConcreteFilterFunctor&&) = delete;
    ConcreteFilterFunctor& operator=(const ConcreteFilterFunctor&) = delete;
    ConcreteFilterFunctor& operator=(ConcreteFilterFunctor&&) = delete;

    explicit ConcreteFilterFunctor(const std::vector<int64_t>& vector_ids)
        : IDSelectorBatch(vector_ids.size(), vector_ids.data()) {}

    ~ConcreteFilterFunctor() override = default;

    bool Check(int64_t vector_id) override { return is_member(vector_id); }
  };

  virtual int32_t GetDimension() = 0;
  virtual pb::common::MetricType GetMetricType() = 0;
  virtual butil::Status GetCount(int64_t& count);
  virtual butil::Status GetDeletedCount(int64_t& deleted_count);
  virtual butil::Status GetMemorySize(int64_t& memory_size);
  virtual bool IsExceedsMaxElements() = 0;

  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;
  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_priority);
  butil::Status AddByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_priority = false);

  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;
  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_priority);
  butil::Status UpsertByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                 bool is_priority = false);

  virtual butil::Status Delete(const std::vector<int64_t>& delete_ids) = 0;
  virtual butil::Status Delete(const std::vector<int64_t>& delete_ids, bool is_priority);

  virtual butil::Status Save(const std::string& path);

  virtual butil::Status Load(const std::string& path);

  virtual butil::Status Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                               const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                               const pb::common::VectorSearchParameter& parameter,
                               std::vector<pb::index::VectorWithDistanceResult>& results) = 0;

  butil::Status SearchByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                 const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                                 const pb::common::VectorSearchParameter& parameter,
                                 std::vector<pb::index::VectorWithDistanceResult>& results);

  virtual butil::Status RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                                    const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                    bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                    std::vector<pb::index::VectorWithDistanceResult>& results) = 0;

  butil::Status RangeSearchByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                                      const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                      bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                      std::vector<pb::index::VectorWithDistanceResult>& results);

  virtual void LockWrite() = 0;
  virtual void UnlockWrite() = 0;
  virtual butil::Status Train(const std::vector<float>& train_datas) = 0;
  virtual butil::Status Train(const std::vector<pb::common::VectorWithId>& vectors) = 0;
  virtual bool NeedToRebuild() = 0;
  virtual bool NeedTrain() { return false; }
  virtual bool IsTrained() { return true; }
  virtual bool NeedToSave(int64_t last_save_log_behind) = 0;
  virtual bool SupportSave() { return false; }

  virtual uint32_t WriteOpParallelNum() { return 1; }

  int64_t Id() const { return id; }

  pb::common::VectorIndexType VectorIndexType() { return vector_index_type; }

  // Fix ivf pq internal using flat search.
  // if not use this function . maybe search filter transform.
  // this will poor performance and poor maintainability.
  virtual pb::common::VectorIndexType VectorIndexSubType() {
    return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  }

  pb::common::VectorIndexParameter VectorIndexParameter() { return vector_index_parameter; }

  int64_t ApplyLogId() const;
  void SetApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId() const;
  void SetSnapshotLogId(int64_t snapshot_log_id);

  pb::common::RegionEpoch Epoch() const;
  pb::common::Range Range() const;
  void SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range);

 protected:
  // vector index id
  int64_t id;
  // vector index type, e.g. hnsw/flat
  pb::common::VectorIndexType vector_index_type;

  // apply max log id
  std::atomic<int64_t> apply_log_id;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id;

  pb::common::RegionEpoch epoch;
  pb::common::Range range;

  pb::common::VectorIndexParameter vector_index_parameter;

  // vector index thread pool
  ThreadPoolPtr thread_pool;
};

using VectorIndexPtr = std::shared_ptr<VectorIndex>;

class VectorIndexWrapper : public std::enable_shared_from_this<VectorIndexWrapper> {
 public:
  VectorIndexWrapper(int64_t id, pb::common::VectorIndexParameter index_parameter,
                     int64_t save_snapshot_threshold_write_key_num);
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

  int64_t LastBuildEpochVersion();

  bool IsReady() { return ready_.load(); }
  bool IsStop() { return stop_.load(); }
  bool IsOwnReady() { return GetOwnVectorIndex() != nullptr; }

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

  // Fix ivf pq internal using flat search.
  // if not use this function . maybe search filter transform.
  // this will poor performance and poor maintainability.
  pb::common::VectorIndexType SubType() {
    BAIDU_SCOPED_LOCK(vector_index_mutex_);

    return vector_index_ != nullptr ? vector_index_->VectorIndexSubType()
                                    : pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  }

  pb::common::VectorIndexParameter IndexParameter() { return index_parameter_; }

  int64_t ApplyLogId();
  void SetApplyLogId(int64_t apply_log_id);
  void SaveApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId();
  void SetSnapshotLogId(int64_t snapshot_log_id);
  void SaveSnapshotLogId(int64_t snapshot_log_id);

  bool IsSwitchingVectorIndex();
  void SetIsSwitchingVectorIndex(bool is_switching);

  void SetIsTempHoldVectorIndex(bool need);

  // check temp hold vector index
  bool IsTempHoldVectorIndex() const;
  // check permanent hold vector index
  static bool IsPermanentHoldVectorIndex(int64_t region_id);

  vector_index::SnapshotMetaSetPtr SnapshotSet() {
    BAIDU_SCOPED_LOCK(vector_index_mutex_);
    return snapshot_set_;
  }

  void UpdateVectorIndex(VectorIndexPtr vector_index, const std::string& trace);
  void ClearVectorIndex(const std::string& trace);

  VectorIndexPtr GetOwnVectorIndex();
  VectorIndexPtr GetVectorIndex();

  VectorIndexPtr ShareVectorIndex();
  void SetShareVectorIndex(VectorIndexPtr vector_index);

  VectorIndexPtr SiblingVectorIndex();
  void SetSiblingVectorIndex(VectorIndexPtr vector_index);

  bool ExecuteTask(TaskRunnablePtr task);

  int32_t PendingTaskNum();
  void IncPendingTaskNum();
  void DecPendingTaskNum();

  int32_t LoadorbuildingNum();
  void IncLoadoruildingNum();
  void DecLoadoruildingNum();

  int32_t RebuildingNum();
  void IncRebuildingNum();
  void DecRebuildingNum();

  int32_t SavingNum();
  void IncSavingNum();
  void DecSavingNum();

  int32_t GetDimension();
  pb::common::MetricType GetMetricType();
  butil::Status GetCount(int64_t& count);
  butil::Status GetDeletedCount(int64_t& deleted_count);
  butil::Status GetMemorySize(int64_t& memory_size);
  bool IsExceedsMaxElements();

  bool NeedToRebuild();
  bool NeedToSave(std::string& reason);
  bool SupportSave();

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status Delete(const std::vector<int64_t>& delete_ids);
  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       const pb::common::Range& region_range,
                       std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
                       const pb::common::VectorSearchParameter& parameter,
                       std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                            const pb::common::Range& region_range,
                            std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters, bool reconstruct,
                            const pb::common::VectorSearchParameter& parameter,
                            std::vector<pb::index::VectorWithDistanceResult>& results);

  static butil::Status SetVectorIndexRangeFilter(
      VectorIndexPtr vector_index,
      std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,  // NOLINT
      int64_t min_vector_id, int64_t max_vector_id);

 private:
  // vector index id
  int64_t id_;
  // vector index version
  int64_t version_{0};
  // vector index is ready
  std::atomic<bool> ready_;
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

  // Own vector index
  VectorIndexPtr vector_index_;
  // Share other vector index.
  VectorIndexPtr share_vector_index_;
  // Sibling vector index by merge source region.
  VectorIndexPtr sibling_vector_index_;

  // Protect vector_index_/share_vector_index_
  bthread_mutex_t vector_index_mutex_;

  // Snapshot set
  vector_index::SnapshotMetaSetPtr snapshot_set_;

  std::atomic<int32_t> pending_task_num_;
  // vector index loadorbuilding num
  std::atomic<int32_t> loadorbuilding_num_;
  // vector index rebuilding num
  std::atomic<int32_t> rebuilding_num_;
  // vector index saving num
  std::atomic<int32_t> saving_num_;

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
