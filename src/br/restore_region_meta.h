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

#ifndef DINGODB_BR_RESTORE_REGION_META_H_
#define DINGODB_BR_RESTORE_REGION_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreRegionMeta : public std::enable_shared_from_this<RestoreRegionMeta> {
 public:
  RestoreRegionMeta(ServerInteractionPtr coordinator_interaction, std::shared_ptr<dingodb::pb::common::Region> region,
                    int64_t replica_num, const std::string& backup_meta_region_name, int64_t create_region_timeout_s);
  ~RestoreRegionMeta();

  RestoreRegionMeta(const RestoreRegionMeta&) = delete;
  const RestoreRegionMeta& operator=(const RestoreRegionMeta&) = delete;
  RestoreRegionMeta(RestoreRegionMeta&&) = delete;
  RestoreRegionMeta& operator=(RestoreRegionMeta&&) = delete;

  std::shared_ptr<RestoreRegionMeta> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status CreateRegionToCoordinator();
  ServerInteractionPtr coordinator_interaction_;
  std::shared_ptr<dingodb::pb::common::Region> region_;
  int64_t replica_num_;
  // region_belongs_to_whom_ = dingodb::Constant::kStoreRegionName or dingodb::Constant::kIndexRegionName or
  // dingodb::Constant::kDocumentRegionName
  std::string backup_meta_region_name_;
  std::string region_debug_info_;
  int64_t create_region_timeout_s_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_REGION_META_H_