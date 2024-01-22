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

#include "config/config_helper.h"

#include <cmath>
#include <cstdint>
#include <string>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_manager.h"
#include "fmt/core.h"

namespace dingodb {

pb::raft::SplitStrategy ConfigHelper::GetSplitStrategy() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  std::string split_strategy = config != nullptr ? config->GetString("region.split_strategy") : "";
  split_strategy = (split_strategy == "PRE_CREATE_REGION" || split_strategy == "POST_CREATE_REGION")
                       ? split_strategy
                       : Constant::kSplitStrategy;
  return split_strategy == "POST_CREATE_REGION" ? pb::raft::SplitStrategy::POST_CREATE_REGION
                                                : pb::raft::SplitStrategy::PRE_CREATE_REGION;
}

int64_t ConfigHelper::GetRegionMaxSize() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kRegionMaxSizeDefaultValue;
  }
  int64_t region_max_size = config->GetInt64("region.region_max_size");
  if (region_max_size < Constant::kRegionMaxSizeDefaultValue) {
    region_max_size = Constant::kRegionMaxSizeDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] region_max_size is too small, set default value({})",
                                      Constant::kRegionMaxSizeDefaultValue);
  }
  return region_max_size;
}

int64_t ConfigHelper::GetSplitCheckApproximateSize() {
  int64_t region_max_size = GetRegionMaxSize();
  return static_cast<int64_t>(static_cast<double>(Constant::kDefaultSplitCheckApproximateSizeRatio) * region_max_size);
}

std::string ConfigHelper::GetSplitPolicy() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitPolicy;
  }
  std::string split_policy = config->GetString("region.split_policy");
  return split_policy.empty() ? Constant::kDefaultSplitPolicy : split_policy;
}

uint32_t ConfigHelper::GetSplitChunkSize() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kSplitChunkSizeDefaultValue;
  }
  int split_chunk_size = config->GetInt("region.split_chunk_size");
  if (split_chunk_size < Constant::kSplitChunkSizeDefaultValue) {
    split_chunk_size = Constant::kSplitChunkSizeDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_chunk_size is too small, set default value({})",
                                      Constant::kSplitChunkSizeDefaultValue);
  }
  return split_chunk_size;
}

float ConfigHelper::GetSplitSizeRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kSplitRatioDefaultValue;
  }
  float split_ratio = static_cast<float>(config->GetDouble("region.split_size_ratio"));
  if (split_ratio < 0.1 || split_ratio > 0.9) {
    split_ratio = Constant::kSplitRatioDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_size_ratio out of range, set default value({})",
                                      Constant::kSplitRatioDefaultValue);
  }
  return split_ratio;
}

uint32_t ConfigHelper::GetSplitKeysNumber() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kSplitKeysNumberDefaultValue;
  }
  int split_keys_number = config->GetInt("region.split_keys_number");
  if (split_keys_number < Constant::kSplitKeysNumberDefaultValue) {
    split_keys_number = Constant::kSplitKeysNumberDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_keys_number is too small, set default value({})",
                                      Constant::kSplitKeysNumberDefaultValue);
  }
  return split_keys_number;
}

float ConfigHelper::GetSplitKeysRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kSplitKeysRatioDefaultValue;
  }
  float split_keys_ratio = static_cast<float>(config->GetDouble("region.split_keys_ratio"));
  if (split_keys_ratio < 0.1 || split_keys_ratio > 0.9) {
    split_keys_ratio = Constant::kSplitKeysRatioDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_keys_ratio out of range, set default value({})",
                                      Constant::kSplitKeysRatioDefaultValue);
  }
  return split_keys_ratio;
}

uint32_t ConfigHelper::GetElectionTimeout() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kRaftElectionTimeoutSDefaultValue;
  }

  int election_timeout_s = config->GetInt("raft.election_timeout_s");
  if (election_timeout_s <= 0) {
    election_timeout_s = Constant::kRaftElectionTimeoutSDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] election_timeout_s is too small, set default value({})",
                                      Constant::kRaftElectionTimeoutSDefaultValue);
  }
  return election_timeout_s;
}

int ConfigHelper::GetRocksDBBackgroundThreadNum() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kRocksdbBackgroundThreadNumDefault;
  }

  int num = config->GetInt("store.background_thread_num");
  if (num <= 0) {
    double ratio = config->GetDouble("store.background_thread_ratio");
    if (ratio > 0) {
      num = std::round(ratio * static_cast<double>(Helper::GetCoreNum()));
    }
  }

  return num > 0 ? num : Constant::kRocksdbBackgroundThreadNumDefault;
}

int ConfigHelper::GetRocksDBStatsDumpPeriodSec() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kStatsDumpPeriodSecDefault;
  }

  int num = config->GetInt("store.stats_dump_period_s");
  return (num <= 0) ? Constant::kStatsDumpPeriodSecDefault : num;
}

}  // namespace dingodb