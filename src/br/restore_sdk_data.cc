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

#include "br/restore_sdk_data.h"

#include <cstdint>
#include <memory>
#include <string>

#include "common/constant.h"
#include "fmt/core.h"

namespace br {

RestoreSdkData::RestoreSdkData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                               ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                               const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                               const std::string& storage_internal,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst,
                               uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                               int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num)

    : RestoreDataBase(coordinator_interaction, store_interaction, index_interaction, document_interaction, restorets,
                      restoretso_internal, storage, storage_internal, store_region_sdk_data_sst,
                      store_cf_sst_meta_sdk_data_sst, index_region_sdk_data_sst, index_cf_sst_meta_sdk_data_sst,
                      document_region_sdk_data_sst, document_cf_sst_meta_sdk_data_sst, create_region_concurrency,
                      restore_region_concurrency, create_region_timeout_s, restore_region_timeout_s, replica_num,
                      dingodb::Constant::kSdkData) {}

RestoreSdkData::~RestoreSdkData() = default;

std::shared_ptr<RestoreSdkData> RestoreSdkData::GetSelf() { return this->shared_from_this(); }

butil::Status RestoreSdkData::Init() { return RestoreDataBase::Init(); }

butil::Status RestoreSdkData::Run() { return RestoreDataBase::Run(); }

butil::Status RestoreSdkData::Finish() { return RestoreDataBase::Finish(); }

std::pair<int64_t, int64_t> RestoreSdkData::GetRegions() { return RestoreDataBase::GetRegions(); }

}  // namespace br