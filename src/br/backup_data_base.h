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

#ifndef DINGODB_BR_BACKUP_DATA_BASE_H_
#define DINGODB_BR_BACKUP_DATA_BASE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class BackupSdkData;
class BackupSqlData;
class BackupDataBase {
 public:
  BackupDataBase(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                 ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                 const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                 const std::string& storage_internal, const std::string& name);
  virtual ~BackupDataBase();

  BackupDataBase(const BackupDataBase&) = delete;
  const BackupDataBase& operator=(const BackupDataBase&) = delete;
  BackupDataBase(BackupDataBase&&) = delete;
  BackupDataBase& operator=(BackupDataBase&&) = delete;

  void SetRegionMap(std::shared_ptr<dingodb::pb::common::RegionMap> region_map);

  virtual butil::Status Filter();

  virtual butil::Status Run();

  virtual butil::Status Backup();

  std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> GetBackupMeta();

  friend BackupSdkData;
  friend BackupSqlData;

 protected:
  butil::Status BackupRegion();
  butil::Status BackupCfSstMeta();
  butil::Status DoBackupRegion(std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions,
                               const std::string& file_name);
  butil::Status DoBackupCfSstMeta(
      std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
      const std::string& file_name, const std::string& region_from);
  butil::Status DoBackupRegionInternal(
      ServerInteractionPtr interaction, const std::string& service_name,
      std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions,
      std::atomic<int64_t>& already_handle_regions,
      std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
      std::atomic<bool>& is_thread_exit);

 private:
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;
  std::shared_ptr<dingodb::pb::common::RegionMap> region_map_;
  std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_store_regions_;
  std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_index_regions_;
  std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_document_regions_;

  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;

  std::atomic<int64_t> already_handle_regions_;
  std::atomic<int64_t> already_handle_store_regions_;
  std::atomic<int64_t> already_handle_index_regions_;
  std::atomic<int64_t> already_handle_document_regions_;

  std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_store_region_map_;
  std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_index_region_map_;
  std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_document_region_map_;

  std::string name_;

  std::vector<std::string> save_region_files_;
  std::vector<std::string> save_cf_sst_meta_files_;

  std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> backup_data_base_;

  bthread_mutex_t mutex_;

  // last error
  butil::Status last_error_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_DATA_BASE_H_