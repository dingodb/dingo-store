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

#ifndef DINGODB_TXN_ENGINE_HELPER_H_
#define DINGODB_TXN_ENGINE_HELPER_H_

#include <memory>

#include "butil/status.h"
#include "engine/raw_engine.h"

namespace dingodb {

class TxnEngineHelper {
 public:
  static butil::Status GetLockInfo(std::shared_ptr<RawEngine::Reader> reader, const std::string &key,
                                   pb::store::LockInfo &lock_info);
  static butil::Status Rollback(std::shared_ptr<RawEngine> engine, std::vector<std::string> &keys, uint64_t start_ts);
  static butil::Status Commit(std::shared_ptr<RawEngine> engine, std::vector<pb::store::LockInfo> &lock_infos,
                              uint64_t commit_ts);
};

}  // namespace dingodb

#endif  // DINGODB_TXN_ENGINE_HELPER_H_