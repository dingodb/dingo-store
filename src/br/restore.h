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

#ifndef DINGODB_BR_RESTORE_H_
#define DINGODB_BR_RESTORE_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/parameter.h"
#include "br/restore_data.h"
#include "br/restore_meta.h"
#include "bthread/types.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class Restore : public std::enable_shared_from_this<Restore> {
 public:
  Restore(const RestoreParams& params, uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
          int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num);
  ~Restore();

  Restore(const Restore&) = delete;
  const Restore& operator=(const Restore&) = delete;
  Restore(Restore&&) = delete;
  Restore& operator=(Restore&&) = delete;

  std::shared_ptr<Restore> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status DoRun();
  butil::Status ParamsCheck();
  butil::Status ParamsCheckForStorage();
  butil::Status CheckGcSafePoint();
  butil::Status SetGcStop();
  butil::Status SetGcStart();
  butil::Status DisableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status EnableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) const;
  butil::Status DisableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                    ServerInteractionPtr index_interaction);
  butil::Status EnableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                   ServerInteractionPtr index_interaction) const;
  static butil::Status RegisterBackupStatusToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status RegisterRestoreToCoordinator(bool is_first, ServerInteractionPtr coordinator_interaction);
  butil::Status UnregisterRestoreToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status DoAsyncRegisterRestoreToCoordinator(ServerInteractionPtr coordinator_interaction);
  butil::Status DoRegisterRestoreToCoordinatorInternal(ServerInteractionPtr coordinator_interaction);
  static butil::Status GetAllRegionMapFromCoordinator(ServerInteractionPtr coordinator_interaction);
  static butil::Status GetVersionFromCoordinator(ServerInteractionPtr coordinator_interaction,
                                                 dingodb::pb::common::VersionInfo& version_info);
  static butil::Status CompareVersion(const dingodb::pb::common::VersionInfo& version_info_local,
                                      const dingodb::pb::common::VersionInfo& version_info_remote,
                                      const dingodb::pb::common::VersionInfo& version_info_in_backup);
  butil::Status DoFinish();

  std::string coor_url_;
  std::string store_url_;
  std::string index_url_;
  std::string document_url_;
  std::string br_type_;
  std::string br_restore_type_;
  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  uint32_t create_region_concurrency_;
  uint32_t restore_region_concurrency_;
  int64_t create_region_timeout_s_;
  int64_t restore_region_timeout_s_;
  int32_t replica_num_;

  std::map<std::string, std::string> backupmeta_file_kvs_;

  // gc is stop or not
  bool is_gc_stop_;

  // gc is enable after finish or not
  bool is_gc_enable_after_finish_;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;

  // restore task id. use uuid to generate.
  std::string restore_task_id_;

  // is already register restore to coordinator or not
  bool is_already_register_restore_to_coordinator_;

  // is exit register restore to coordinator thread. default true.
  bool is_exit_register_restore_to_coordinator_thread_;

  bool region_auto_split_enable_after_finish_;
  bool region_auto_merge_enable_after_finish_;

  bool balance_leader_enable_after_finish_;
  bool balance_region_enable_after_finish_;

  // last error
  butil::Status last_error_;

  std::shared_ptr<RestoreMeta> restore_meta_;

  std::shared_ptr<RestoreData> restore_data_;

  // statistics
  int64_t start_time_ms_;

  // statistics
  int64_t end_time_ms_;

  bthread_mutex_t mutex_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_H_