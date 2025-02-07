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

#ifndef DINGODB_BR_RESTORE_SQL_META_H_
#define DINGODB_BR_RESTORE_SQL_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/restore_region_data_manager.h"
#include "br/restore_region_meta_manager.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreSqlMeta : public std::enable_shared_from_this<RestoreSqlMeta> {
 public:
  RestoreSqlMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                 const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                 const std::string& storage_internal,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_meta_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_meta_sst,
                 uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                 int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num);
  ~RestoreSqlMeta();

  RestoreSqlMeta(const RestoreSqlMeta&) = delete;
  const RestoreSqlMeta& operator=(const RestoreSqlMeta&) = delete;
  RestoreSqlMeta(RestoreSqlMeta&&) = delete;
  RestoreSqlMeta& operator=(RestoreSqlMeta&&) = delete;

  std::shared_ptr<RestoreSqlMeta> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status CheckStoreRegionSqlMetaSst();
  butil::Status CheckStoreCfSstMetaSqlMetaSst();
  butil::Status ExtractFromStoreRegionSqlMetaSst();
  butil::Status ExtractFromStoreCfSstMetaSqlMetaSst();
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_meta_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_meta_sst_;
  uint32_t create_region_concurrency_;
  uint32_t restore_region_concurrency_;
  int64_t create_region_timeout_s_;
  int64_t restore_region_timeout_s_;
  int32_t replica_num_;

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      id_and_sst_meta_group_kvs_;

  std::shared_ptr<RestoreRegionMetaManager> restore_region_meta_manager_;
  std::shared_ptr<RestoreRegionDataManager> restore_region_data_manager_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_SQL_META_H_