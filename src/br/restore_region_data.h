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

#ifndef DINGODB_BR_RESTORE_REGION_DATA_H_
#define DINGODB_BR_RESTORE_REGION_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreRegionData : public std::enable_shared_from_this<RestoreRegionData> {
 public:
  RestoreRegionData(
      ServerInteractionPtr coordinator_interaction, ServerInteractionPtr interaction,
      std::shared_ptr<dingodb::pb::common::Region> region, const std::string& restorets, int64_t restoretso_internal,
      const std::string& storage, const std::string& storage_internal,
      std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup> backup_data_file_value_sst_meta_group,
      const std::string& backup_meta_region_cf_name, const std::string& group_belongs_to_whom,
      int64_t restore_region_timeout_s);
  ~RestoreRegionData();

  RestoreRegionData(const RestoreRegionData&) = delete;
  const RestoreRegionData& operator=(const RestoreRegionData&) = delete;
  RestoreRegionData(RestoreRegionData&&) = delete;
  RestoreRegionData& operator=(RestoreRegionData&&) = delete;

  std::shared_ptr<RestoreRegionData> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status RegionDataToStore();
  butil::Status RegionDataToIndex();
  butil::Status RegionDataToDocument();
  butil::Status RegionMetaToStore();
  butil::Status GetRegionEpoch();

  template <typename Request, typename Response>
  butil::Status SendRegionRequest(const std::string& service_name, const std::string& api_name);

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr interaction_;
  std::shared_ptr<dingodb::pb::common::Region> region_;

  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup> backup_data_file_value_sst_meta_group_;
  std::string backup_meta_region_cf_name_;
  // group_belongs_to_whom_ = dingodb::Constant::kRestoreMeta or dingodb::Constant::kRestoreData
  std::string group_belongs_to_whom_;
  dingodb::pb::common::RegionEpoch region_epoch_;
  std::string group_debug_info_;
  int64_t restore_region_timeout_s_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_REGION_DATA_H_