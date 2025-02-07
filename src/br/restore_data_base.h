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

#ifndef DINGODB_BR_RESTORE_DATA_BASE_H_
#define DINGODB_BR_RESTORE_DATA_BASE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/restore_region_data_manager.h"
#include "br/restore_region_meta_manager.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class RestoreDataBase {
 public:
  RestoreDataBase(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                  ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                  const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                  const std::string& storage_internal,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst,
                  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst,
                  uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                  int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num,
                  const std::string& type_name);
  ~RestoreDataBase();

  RestoreDataBase(const RestoreDataBase&) = delete;
  const RestoreDataBase& operator=(const RestoreDataBase&) = delete;
  RestoreDataBase(RestoreDataBase&&) = delete;
  RestoreDataBase& operator=(RestoreDataBase&&) = delete;

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status CheckStoreRegionDataSst();
  butil::Status CheckStoreCfSstMetaDataSst();
  butil::Status CheckIndexRegionDataSst();
  butil::Status CheckIndexCfSstMetaDataSst();
  butil::Status CheckDocumentRegionDataSst();
  butil::Status CheckDocumentCfSstMetaDataSst();
  butil::Status CheckRegionDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
                                   const std::string& file_name);
  butil::Status CheckCfSstMetaDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> cf_sst_meta_data_sst,
                                      const std::string& file_name, const std::string& exec_node);

  butil::Status ExtractFromStoreRegionDataSst();
  butil::Status ExtractFromStoreCfSstMetaDataSst();
  butil::Status ExtractFromIndexRegionDataSst();
  butil::Status ExtractFromIndexCfSstMetaDataSst();
  butil::Status ExtractFromDocumentRegionDataSst();
  butil::Status ExtractFromDocumentCfSstMetaDataSst();

  butil::Status ExtractFromRegionDataSst(
      std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
      std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>& id_and_region_kvs);

  butil::Status ExtractFromCfSstMetaDataSst(
      std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
      std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>&
          id_and_sst_meta_group_kvs);

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  // sql maybe nullptr. if no any data.
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst_;

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> store_id_and_region_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      store_id_and_sst_meta_group_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> index_id_and_region_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      index_id_and_sst_meta_group_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> document_id_and_region_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      document_id_and_sst_meta_group_kvs_;

  std::shared_ptr<RestoreRegionMetaManager> store_restore_region_meta_manager_;
  std::shared_ptr<RestoreRegionDataManager> store_restore_region_data_manager_;
  std::shared_ptr<RestoreRegionMetaManager> index_restore_region_meta_manager_;
  std::shared_ptr<RestoreRegionDataManager> index_restore_region_data_manager_;
  std::shared_ptr<RestoreRegionMetaManager> document_restore_region_meta_manager_;
  std::shared_ptr<RestoreRegionDataManager> document_restore_region_data_manager_;

  uint32_t create_region_concurrency_;
  uint32_t restore_region_concurrency_;
  int64_t create_region_timeout_s_;
  int64_t restore_region_timeout_s_;
  int32_t replica_num_;
  std::string type_name_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_DATA_BASE_H_