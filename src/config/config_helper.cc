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

#include <string>

#include "common/constant.h"
#include "config/config_manager.h"

namespace dingodb {

pb::raft::SplitStrategy ConfigHelper::GetSplitStrategy() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  std::string split_strategy = config != nullptr ? config->GetString("region.split_strategy") : "";
  split_strategy = (split_strategy == "PRE_CREATE_REGION" || split_strategy == "POST_CREATE_REGION")
                       ? split_strategy
                       : Constant::kSplitStrategy;
  return split_strategy == "POST_CREATE_REGION" ? pb::raft::SplitStrategy::POST_CREATE_REGION
                                                : pb::raft::SplitStrategy::PRE_CREATE_REGION;
}

uint32_t ConfigHelper::GetRegionMaxSize() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultRegionMaxSize;
  }
  int region_max_size = config->GetInt("region.region_max_size");
  return region_max_size > 0 ? region_max_size : Constant::kDefaultRegionMaxSize;
}

uint32_t ConfigHelper::GetSplitCheckApproximateSize() {
  int region_max_size = GetRegionMaxSize();
  return static_cast<uint32_t>(static_cast<double>(region_max_size) * Constant::kDefaultSplitCheckApproximateSizeRatio);
}

std::string ConfigHelper::GetSplitPolicy() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitPolicy;
  }
  std::string split_policy = config->GetString("region.split_policy");
  return split_policy.empty() ? Constant::kDefaultSplitPolicy : split_policy;
}

uint32_t ConfigHelper::GetSplitChunkSize() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitChunkSize;
  }
  int split_chunk_size = config->GetInt("region.split_chunk_size");
  return split_chunk_size > 0 ? split_chunk_size : Constant::kDefaultSplitChunkSize;
}

float ConfigHelper::GetSplitSizeRatio() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitRatio;
  }
  float split_ratio = static_cast<float>(config->GetDouble("region.split_size_ratio"));
  return split_ratio > 0 && split_ratio < 1 ? split_ratio : Constant::kDefaultSplitRatio;
}

uint32_t ConfigHelper::GetSplitKeysNumber() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitKeysNumber;
  }
  int split_keys_number = config->GetInt("region.split_keys_number");
  return split_keys_number > 0 ? split_keys_number : Constant::kDefaultSplitKeysNumber;
}

float ConfigHelper::GetSplitKeysRatio() {
  auto config = ConfigManager::GetInstance()->GetConfig();
  if (config == nullptr) {
    return Constant::kDefaultSplitRatio;
  }
  float split_keys_ratio = static_cast<float>(config->GetDouble("region.split_keys_ratio"));
  return split_keys_ratio > 0 && split_keys_ratio < 1 ? split_keys_ratio : Constant::kDefaultSplitRatio;
}

}  // namespace dingodb