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

#ifndef DINGODB_DOCUMENT_INDEX_H_
#define DINGODB_DOCUMENT_INDEX_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/runnable.h"
#include "common/threadpool.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"

namespace dingodb {

// Document index abstract base class.
// One region own one document index(region_id==document_index_id)
// But one region can refer other document index when region split.
class DocumentIndex {
 public:
  DocumentIndex(int64_t id, const pb::common::DocumentIndexParameter& document_index_parameter,
                const pb::common::RegionEpoch& epoch, const pb::common::Range& range, ThreadPoolPtr thread_pool);
  virtual ~DocumentIndex();

  DocumentIndex(const DocumentIndex& rhs) = delete;
  DocumentIndex& operator=(const DocumentIndex& rhs) = delete;
  DocumentIndex(DocumentIndex&& rhs) = delete;
  DocumentIndex& operator=(DocumentIndex&& rhs) = delete;

  class FilterFunctor {
   public:
    virtual ~FilterFunctor() = default;
    virtual void Build(std::vector<faiss::idx_t>& id_map) {}
    virtual bool Check(int64_t document_id) = 0;
  };

  // Range filter
  class RangeFilterFunctor : public FilterFunctor {
   public:
    RangeFilterFunctor(int64_t min_document_id, int64_t max_document_id)
        : min_document_id_(min_document_id), max_document_id_(max_document_id) {}
    bool Check(int64_t document_id) override {
      return document_id >= min_document_id_ && document_id < max_document_id_;
    }

   private:
    int64_t min_document_id_;
    int64_t max_document_id_;
  };

  class ConcreteFilterFunctor : public FilterFunctor, public faiss::IDSelectorBatch {
   public:
    ConcreteFilterFunctor(const ConcreteFilterFunctor&) = delete;
    ConcreteFilterFunctor(ConcreteFilterFunctor&&) = delete;
    ConcreteFilterFunctor& operator=(const ConcreteFilterFunctor&) = delete;
    ConcreteFilterFunctor& operator=(ConcreteFilterFunctor&&) = delete;

    explicit ConcreteFilterFunctor(const std::vector<int64_t>& document_ids, bool is_negation = false)
        : IDSelectorBatch(document_ids.size(), document_ids.data()), is_negation_(is_negation) {}

    ~ConcreteFilterFunctor() override = default;

    bool Check(int64_t document_id) override {
      bool exist = is_member(document_id);
      return !is_negation_ ? exist : !exist;
    }

   private:
    bool is_negation_{false};
  };

  virtual int32_t GetDimension() = 0;
  virtual pb::common::MetricType GetMetricType() = 0;
  virtual butil::Status GetCount(int64_t& count);
  virtual butil::Status GetDeletedCount(int64_t& deleted_count);
  virtual butil::Status GetMemorySize(int64_t& memory_size);
  virtual bool IsExceedsMaxElements() = 0;

  virtual butil::Status Add(const std::vector<pb::common::DocumentWithId>& document_with_ids) = 0;
  virtual butil::Status Add(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool is_priority);
  butil::Status AddByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                              bool is_priority = false);

  virtual butil::Status Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids) = 0;
  virtual butil::Status Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool is_priority);
  butil::Status UpsertByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                 bool is_priority = false);

  virtual butil::Status Delete(const std::vector<int64_t>& delete_ids) = 0;
  virtual butil::Status Delete(const std::vector<int64_t>& delete_ids, bool is_priority);
  virtual butil::Status DeleteByParallel(const std::vector<int64_t>& delete_ids, bool is_priority);

  virtual butil::Status Save(const std::string& path);

  virtual butil::Status Load(const std::string& path);

  virtual butil::Status Search(const std::vector<pb::common::DocumentWithId>& document_with_ids, uint32_t topk,
                               const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                               const pb::common::DocumentSearchParameter& parameter,
                               std::vector<pb::document::DocumentWithScoreResult>& results) = 0;

  butil::Status SearchByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids, uint32_t topk,
                                 const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                                 const pb::common::DocumentSearchParameter& parameter,
                                 std::vector<pb::document::DocumentWithScoreResult>& results);

  virtual butil::Status RangeSearch(const std::vector<pb::common::DocumentWithId>& document_with_ids, float radius,
                                    const std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,
                                    bool reconstruct, const pb::common::DocumentSearchParameter& parameter,
                                    std::vector<pb::document::DocumentWithScoreResult>& results) = 0;

  virtual void LockWrite() = 0;
  virtual void UnlockWrite() = 0;
  virtual butil::Status Train(std::vector<float>& train_datas) = 0;
  virtual butil::Status TrainByParallel(std::vector<float>& train_datas);
  virtual butil::Status Train(const std::vector<pb::common::DocumentWithId>& vectors) = 0;
  virtual bool NeedToRebuild() = 0;
  virtual bool NeedTrain() { return false; }
  virtual bool IsTrained() { return true; }
  virtual bool NeedToSave(int64_t last_save_log_behind) = 0;
  virtual bool SupportSave() { return false; }

  virtual uint32_t WriteOpParallelNum() { return 1; }

  int64_t Id() const { return id; }

  pb::common::DocumentIndexParameter DocumentIndexParameter() { return document_index_parameter; }

  int64_t ApplyLogId() const;
  void SetApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId() const;
  void SetSnapshotLogId(int64_t snapshot_log_id);

  pb::common::RegionEpoch Epoch() const;
  pb::common::Range Range() const;
  void SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range);

 protected:
  // document index id
  int64_t id;

  // apply max log id
  std::atomic<int64_t> apply_log_id;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id;

  pb::common::RegionEpoch epoch;
  pb::common::Range range;

  pb::common::DocumentIndexParameter document_index_parameter;

  // vector index thread pool
  ThreadPoolPtr thread_pool;
};

using DocumentIndexPtr = std::shared_ptr<DocumentIndex>;

class DocumentIndexWrapper : public std::enable_shared_from_this<DocumentIndexWrapper> {
 public:
  DocumentIndexWrapper(int64_t id, pb::common::DocumentIndexParameter index_parameter,
                       int64_t save_snapshot_threshold_write_key_num);
  ~DocumentIndexWrapper();

  static std::shared_ptr<DocumentIndexWrapper> New(int64_t id, pb::common::DocumentIndexParameter index_parameter);

  std::shared_ptr<DocumentIndexWrapper> GetSelf();

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
  bool IsOwnReady() { return GetOwnDocumentIndex() != nullptr; }

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

  pb::common::DocumentIndexParameter IndexParameter() { return index_parameter_; }

  int64_t ApplyLogId();
  void SetApplyLogId(int64_t apply_log_id);
  void SaveApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId();
  void SetSnapshotLogId(int64_t snapshot_log_id);
  void SaveSnapshotLogId(int64_t snapshot_log_id);

  bool IsSwitchingDocumentIndex();
  void SetIsSwitchingDocumentIndex(bool is_switching);

  void SetIsTempHoldDocumentIndex(bool need);

  // check temp hold vector index
  bool IsTempHoldDocumentIndex() const;
  // check permanent hold vector index
  static bool IsPermanentHoldDocumentIndex(int64_t region_id);

  void UpdateDocumentIndex(DocumentIndexPtr vector_index, const std::string& trace);
  void ClearDocumentIndex(const std::string& trace);

  DocumentIndexPtr GetOwnDocumentIndex();
  DocumentIndexPtr GetDocumentIndex();

  DocumentIndexPtr ShareDocumentIndex();
  void SetShareDocumentIndex(DocumentIndexPtr vector_index);

  DocumentIndexPtr SiblingDocumentIndex();
  void SetSiblingDocumentIndex(DocumentIndexPtr vector_index);

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

  butil::Status Add(const std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status Delete(const std::vector<int64_t>& delete_ids);
  butil::Status Search(std::vector<pb::common::DocumentWithId> document_with_ids, uint32_t topk,
                       const pb::common::Range& region_range,
                       std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters, bool reconstruct,
                       const pb::common::DocumentSearchParameter& parameter,
                       std::vector<pb::document::DocumentWithScoreResult>& results);

  static butil::Status SetDocumentIndexRangeFilter(
      DocumentIndexPtr vector_index,
      std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,  // NOLINT
      int64_t min_document_id, int64_t max_document_id);

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

  // document index definition parameter
  pb::common::DocumentIndexParameter index_parameter_;

  // apply max log id
  std::atomic<int64_t> apply_log_id_;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id_;

  // Indicate switching vector index.
  std::atomic<bool> is_switching_vector_index_;

  // Own vector index
  DocumentIndexPtr vector_index_;
  // Share other vector index.
  DocumentIndexPtr share_vector_index_;
  // Sibling vector index by merge source region.
  DocumentIndexPtr sibling_vector_index_;

  // Protect vector_index_/share_vector_index_
  bthread_mutex_t vector_index_mutex_;

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

using DocumentIndexWrapperPtr = std::shared_ptr<DocumentIndexWrapper>;

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_INDEX_H_  // NOLINT
