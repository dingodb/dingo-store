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

#ifndef DINGODB_CONFIG_HELPER_H_
#define DINGODB_CONFIG_HELPER_H_

#include <cstdint>

#include "proto/raft.pb.h"

namespace dingodb {

class ConfigHelper {
 public:
  static pb::raft::SplitStrategy GetSplitStrategy();

  static uint32_t GetRegionMaxSize();
  static uint32_t GetSplitCheckApproximateSize();
  static std::string GetSplitPolicy();
  static uint32_t GetSplitChunkSize();
  static float GetSplitSizeRatio();
  static uint32_t GetSplitKeysNumber();
  static float GetSplitKeysRatio();

  static uint32_t GetElectionTimeout();

  static uint32_t GetVectorIndexBackgroundWorkerNum();

  static int GetRocksDBBackgroundThreadNum();
  static int GetRocksDBStatsDumpPeriodSec();
};

}  // namespace dingodb

#endif  // DINGODB_CONFIG_HELPER_H_