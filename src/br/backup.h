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

#ifndef DINGODB_BR_BACKUP_H_
#define DINGODB_BR_BACKUP_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "br/backup_data.h"
#include "br/backup_meta.h"
#include "br/interation.h"
#include "br/parameter.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class Backup : public std::enable_shared_from_this<Backup> {
 public:
  Backup(const BackupParams& params);
  ~Backup();

  Backup(const Backup&) = delete;
  const Backup& operator=(const Backup&) = delete;
  Backup(Backup&&) = delete;
  Backup& operator=(Backup&&) = delete;

  std::shared_ptr<Backup> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status DoRun();
  butil::Status ParamsCheck();
  butil::Status ParamsCheckForStorage();
  butil::Status GetGcSafePoint();
  butil::Status SetGcStop();
  butil::Status SetGcStart();
  butil::Status RegisterBackupToCoordinator(bool is_first, ServerInteractionPtr coordinator_interaction);
  butil::Status UnregisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status DoAsyncRegisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status DoRegisterBackupToCoordinatorInternal(ServerInteractionPtr coordinator_interaction);

  butil::Status DisableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status EnableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) const;

  butil::Status DisableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                    ServerInteractionPtr index_interaction);
  butil::Status EnableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                   ServerInteractionPtr index_interaction) const;

  static butil::Status GetVersionFromCoordinator(ServerInteractionPtr coordinator_interaction,
                                                 dingodb::pb::common::VersionInfo& version_info);

  static butil::Status CompareVersion(const dingodb::pb::common::VersionInfo& version_info_local,
                                      const dingodb::pb::common::VersionInfo& version_info_remote);

  butil::Status DoFinish();
  static butil::Status GetJobListCheck();

  std::string coor_url_;
  std::string br_type_;
  std::string br_backup_type_;
  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;

  // gc is stop or not
  bool is_gc_stop_;

  // gc is enable after finish or not
  bool is_gc_enable_after_finish_;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;

  // backup task id. use uuid to generate.
  std::string backup_task_id_;

  // is already register backup to coordinator or not
  bool is_already_register_backup_to_coordinator_;

  // is exit register backup to coordinator thread. default true.
  std::atomic<bool> is_exit_register_backup_to_coordinator_thread_;

  bool region_auto_split_enable_after_finish_;
  bool region_auto_merge_enable_after_finish_;

  bool balance_leader_enable_after_finish_;
  bool balance_region_enable_after_finish_;

  // last error
  butil::Status last_error_;

  std::shared_ptr<BackupMeta> backup_meta_;

  std::shared_ptr<BackupData> backup_data_;

  // statistics
  int64_t start_time_ms_;

  // statistics
  int64_t end_time_ms_;

  bthread_mutex_t mutex_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_H_