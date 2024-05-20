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
#include "common/synchronization.h"
#include "document/document_index_snapshot.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"

namespace dingodb {

// Document index abstract base class.
// One region own one document index(region_id==document_index_id)
// But one region can refer other document index when region split.
class DocumentIndex {
 public:
  DocumentIndex(int64_t id, const std::string& index_path,
                const pb::common::DocumentIndexParameter& document_index_parameter,
                const pb::common::RegionEpoch& epoch, const pb::common::Range& range);

  static butil::Status RemoveIndexFiles(int64_t id, const std::string& index_path);

  ~DocumentIndex();

  DocumentIndex(const DocumentIndex& rhs) = delete;
  DocumentIndex& operator=(const DocumentIndex& rhs) = delete;
  DocumentIndex(DocumentIndex&& rhs) = delete;
  DocumentIndex& operator=(DocumentIndex&& rhs) = delete;

  butil::Status GetCount(int64_t& count);

  butil::Status Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool reload_reader);

  butil::Status Add(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool reload_reader);

  butil::Status Delete(const std::vector<int64_t>& delete_ids);

  butil::Status Save(const std::string& path);

  butil::Status Load(const std::string& path);

  butil::Status Search(uint32_t topk, const std::string& query_string, bool use_range_filter, int64_t start_id,
                       int64_t end_id, bool use_id_filter, const std::vector<uint64_t>& alive_ids,
                       const std::vector<std::string>& column_names,
                       std::vector<pb::common::DocumentWithScore>& results);

  void LockWrite();
  void UnlockWrite();

  bool NeedToRebuild() { return false; }                                               // NOLINT
  bool NeedTrain() { return false; }                                                   // NOLINT
  bool IsTrained() { return true; }                                                    // NOLINT
  bool NeedToSave(int64_t last_save_log_behind) { return last_save_log_behind > 10; }  // NOLINT
  bool SupportSave() { return false; }                                                 // NOLINT

  uint32_t WriteOpParallelNum() { return 1; }  // NOLINT

  int64_t Id() const { return id; }

  pb::common::DocumentIndexParameter DocumentIndexParameter() { return document_index_parameter; }

  int64_t ApplyLogId() const;
  void SetApplyLogId(int64_t apply_log_id);

  int64_t SnapshotLogId() const;
  void SetSnapshotLogId(int64_t snapshot_log_id);

  pb::common::RegionEpoch Epoch() const;
  pb::common::Range Range() const;
  void SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range);

  void SetDestroyed() {
    RWLockWriteGuard guard(&rw_lock_);
    is_destroyed_ = true;
  }

  bool IsDestroyed() {
    RWLockReadGuard guard(&rw_lock_);
    return is_destroyed_;
  }

  std::string IndexPath() { return index_path; }

 protected:
  // document index id
  int64_t id;

  // tantivy index path
  std::string index_path;

  // apply max log id
  std::atomic<int64_t> apply_log_id;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id;

  pb::common::RegionEpoch epoch;
  pb::common::Range range;

  pb::common::DocumentIndexParameter document_index_parameter;

 private:
  RWLock rw_lock_;
  bool is_destroyed_{false};
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

  // void SetIsTempHoldDocumentIndex(bool need);

  // check temp hold vector index
  // bool IsTempHoldDocumentIndex() const;
  // check permanent hold vector index
  static bool IsPermanentHoldDocumentIndex(int64_t region_id);

  document_index::SnapshotMetaSetPtr SnapshotSet() {
    BAIDU_SCOPED_LOCK(document_index_mutex_);
    return snapshot_set_;
  }

  void UpdateDocumentIndex(DocumentIndexPtr document_index, const std::string& trace);
  void ClearDocumentIndex(const std::string& trace);

  DocumentIndexPtr GetOwnDocumentIndex();
  DocumentIndexPtr GetDocumentIndex();

  DocumentIndexPtr ShareDocumentIndex();
  void SetShareDocumentIndex(DocumentIndexPtr document_index);

  DocumentIndexPtr SiblingDocumentIndex();
  void SetSiblingDocumentIndex(DocumentIndexPtr document_index);

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

  butil::Status GetCount(int64_t& count);

  bool NeedToRebuild();
  bool NeedToSave(std::string& reason);
  bool SupportSave();

  butil::Status Add(const std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status Delete(const std::vector<int64_t>& delete_ids);
  butil::Status Search(const pb::common::Range& region_range, const pb::common::DocumentSearchParameter& parameter,
                       std::vector<pb::common::DocumentWithScore>& results);

  // static butil::Status SetDocumentIndexRangeFilter(
  //     DocumentIndexPtr document_index,
  //     std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,  // NOLINT
  //     int64_t min_document_id, int64_t max_document_id);

 private:
  // document index id
  int64_t id_;
  // document index version
  int64_t version_{0};
  // document index is ready
  std::atomic<bool> ready_;
  // stop document index
  std::atomic<bool> stop_;
  // document index build status
  std::atomic<bool> build_error_{false};
  // document index rebuild status
  std::atomic<bool> rebuild_error_{false};

  // document index definition parameter
  pb::common::DocumentIndexParameter index_parameter_;

  // apply max log id
  std::atomic<int64_t> apply_log_id_;
  // last snapshot log id
  std::atomic<int64_t> snapshot_log_id_;

  // Indicate switching document index.
  std::atomic<bool> is_switching_document_index_;

  // Own document index
  DocumentIndexPtr document_index_;
  // Share other document index.
  DocumentIndexPtr share_document_index_;
  // Sibling document index by merge source region.
  DocumentIndexPtr sibling_document_index_;

  // Protect document_index_/share_document_index_
  bthread_mutex_t document_index_mutex_;

  // Snapshot set
  document_index::SnapshotMetaSetPtr snapshot_set_;

  std::atomic<int32_t> pending_task_num_;
  // document index loadorbuilding num
  std::atomic<int32_t> loadorbuilding_num_;
  // document index rebuilding num
  std::atomic<int32_t> rebuilding_num_;
  // document index saving num
  std::atomic<int32_t> saving_num_;

  // write(add/update/delete) key count
  int64_t write_key_count_{0};
  int64_t last_save_write_key_count_{0};
  // save snapshot threshold write key num
  int64_t save_snapshot_threshold_write_key_num_;

  // need hold document index
  // std::atomic<bool> is_hold_document_index_;
};

using DocumentIndexWrapperPtr = std::shared_ptr<DocumentIndexWrapper>;

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_INDEX_H_  // NOLINT
