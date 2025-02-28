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

#ifndef DINGODB_BR_RESTORE_SQL_DATA_H_
#define DINGODB_BR_RESTORE_SQL_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/restore_data_base.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class RestoreSqlData : public RestoreDataBase, public std::enable_shared_from_this<RestoreSqlData> {
 public:
  RestoreSqlData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                 ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                 const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                 const std::string& storage_internal,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sql_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sql_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sql_data_sst,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sql_data_sst,
                 uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                 int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num);
  ~RestoreSqlData();

  RestoreSqlData(const RestoreSqlData&) = delete;
  const RestoreSqlData& operator=(const RestoreSqlData&) = delete;
  RestoreSqlData(RestoreSqlData&&) = delete;
  RestoreSqlData& operator=(RestoreSqlData&&) = delete;

  std::shared_ptr<RestoreSqlData> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

  std::pair<int64_t, int64_t> GetRegions();

 protected:
 private:
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_SQL_DATA_H_