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

int64_t ConfigHelper::GetMergeCheckSize() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kAutoMergeRegionMaxSizeDefaultValue;
  }
  int64_t region_max_size = config->GetInt64("region.max_merge_region_size");
  if (region_max_size < Constant::kAutoMergeRegionMaxSizeDefaultValue) {
    region_max_size = Constant::kAutoMergeRegionMaxSizeDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] max_merge_region_size is too small, set default value({})",
                                      Constant::kAutoMergeRegionMaxSizeDefaultValue);
  }
  return region_max_size;
}

int64_t ConfigHelper::GetSplitMergeInterval() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kSplitMergeIntervalDefaultValue;
  }
  int64_t split_merge_interval = config->GetInt64("region.split_merge_interval");
  if (split_merge_interval < Constant::kSplitMergeIntervalDefaultValue) {
    split_merge_interval = Constant::kSplitMergeIntervalDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_merge_interval is too small, set default value({})",
                                      Constant::kSplitMergeIntervalDefaultValue);
  }
  return split_merge_interval;
}

int64_t ConfigHelper::GetMergeCheckKeysCount() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kAutoMergeRegionMaxKeysCountDefaultValue;
  }
  int64_t region_max_keys_count = config->GetInt64("region.max_merge_region_keys");
  if (region_max_keys_count < Constant::kAutoMergeRegionMaxKeysCountDefaultValue) {
    region_max_keys_count = Constant::kAutoMergeRegionMaxKeysCountDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] max_merge_region_keys is too small, set default value({})",
                                      Constant::kAutoMergeRegionMaxKeysCountDefaultValue);
  }
  return region_max_keys_count;
}

float ConfigHelper::GetMergeSizeRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kMergeRatioDefaultValue;
  }
  float merge_ratio = static_cast<float>(config->GetDouble("region.merge_size_ratio"));
  if (merge_ratio < 0.1 || merge_ratio > 0.9) {
    merge_ratio = Constant::kMergeRatioDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] merge_size_ratio out of range, set default value({})",
                                      Constant::kMergeRatioDefaultValue);
  }
  return merge_ratio;
}

float ConfigHelper::GetMergeKeysRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kMergeKeysRatioDefaultValue;
  }
  float merge_ratio = static_cast<float>(config->GetDouble("region.merge_keys_ratio"));
  if (merge_ratio < 0.1 || merge_ratio > 0.9) {
    merge_ratio = Constant::kMergeKeysRatioDefaultValue;
    DINGO_LOG(WARNING) << fmt::format("[config] split_size_ratio out of range, set default value({})",
                                      Constant::kMergeKeysRatioDefaultValue);
  }
  return merge_ratio;
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

uint32_t ConfigHelper::GetLeaderNumWeight() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kLeaderNumWeightDefaultValue;
  }

  int num = config->GetInt("raft.leader_num_weight");
  return (num <= 0) ? Constant::kLeaderNumWeightDefaultValue : num;
}

uint32_t ConfigHelper::GetReserveJobRecentDay() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kReserveJobRecentDayDefaultValue;
  }

  int num = config->GetInt("coordinator.reserve_job_recent_day");
  return (num <= 0) ? Constant::kReserveJobRecentDayDefaultValue : num;
}

std::string ConfigHelper::GetBalanceLeaderInspectionTimePeriod() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return "";
  }

  return config->GetString("coordinator.balance_leader_inspection_time_period");
}

std::string ConfigHelper::GetBalanceRegionInspectionTimePeriod() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return "";
  }

  return config->GetString("coordinator.balance_region_inspection_time_period");
}

float ConfigHelper::GetBalanceRegionCountRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kBalanceRegionDefaultRegionCountRatio;
  }
  float count = config->GetDouble("coordinator.balance_region_default_region_count_ratio");

  return (count < 0 || count > 1) ? Constant::kBalanceRegionDefaultRegionCountRatio : count;
}

int32_t ConfigHelper::GetBalanceRegionDefaultIndexRegionSize() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kBalanceRegionDefaultIndexRegionSize;
  }
  int32_t size = config->GetInt("coordinator.balance_region_default_index_region_size");
  return (size <= 0) ? Constant::kBalanceRegionDefaultIndexRegionSize : size;
}

int32_t ConfigHelper::GetBalanceRegionDefaultStoreRegionSize() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kBalanceRegionDefaultStoreRegionSize;
  }
  int32_t size = config->GetInt("coordinator.balance_region_default_store_region_size");
  return (size <= 0) ? Constant::kBalanceRegionDefaultStoreRegionSize : size;
}

int32_t ConfigHelper::GetWorkerThreadNum() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return -1;
  }

  return config->GetInt("server.worker_thread_num");
}

double ConfigHelper::GetWorkerThreadRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return -1;
  }

  return config->GetDouble("server.worker_thread_ratio");
}

int32_t ConfigHelper::GetRaftWorkerThreadNum() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return -1;
  }

  return config->GetInt("server.raft_worker_thread_num");
}

double ConfigHelper::GetRaftWorkerThreadRatio() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return -1;
  }

  return config->GetDouble("server.raft_worker_thread_ratio");
}

std::string ConfigHelper::GetBlockCacheValue() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return Constant::kBlockCacheDefaultValue;
  }

  int64_t num = config->GetInt64("store.block_cache_size");
  return (num <= 0) ? Constant::kBlockCacheDefaultValue : std::to_string(num);
}

}  // namespace dingodb