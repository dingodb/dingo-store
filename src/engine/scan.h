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

#ifndef DINGODB_ENGINE_SCAN_H_  // NOLINT
#define DINGODB_ENGINE_SCAN_H_

#include <chrono>  // NOLINT
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/context.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

enum class ScanContextState : unsigned char {
  kUninitState = 0,
  kOpeningState = 1,
  kOpenedState = 2,
  kBeginningState = 3,
  kBegunState = 4,
  kContinuingState = 5,
  kContinuedState = 6,
  kReleasingState = 7,
  kReleasedState = 8,
  // error or timeout to destroy
  kErrorState = 9,
  kBegunTimeoutState = 10,
  kContinuedTimeoutState = 11,
  kReleasedTimeoutState = 12,
  kAllowImmediateRecycling = 13,
  kDestroyState = 14,
};

class ScanContext {
 public:
  ScanContext();
  ~ScanContext();

  ScanContext(const ScanContext& rhs) = delete;
  ScanContext& operator=(const ScanContext& rhs) = delete;
  ScanContext(ScanContext&& rhs) = delete;
  ScanContext& operator=(ScanContext&& rhs) = delete;

  butil::Status Open(const std::string& scan_id, uint64_t timeout_ms, uint64_t max_bytes_rpc,
                     uint64_t max_fetch_cnt_by_server, std::shared_ptr<RawEngine> engine, const std::string& cf_name);

  butil::Status ScanBegin(uint64_t region_id, const pb::common::PrefixScanRange& range, uint64_t max_fetch_cnt,
                          bool key_only, bool disable_auto_release, std::vector<pb::common::KeyValue>& kvs);  // NOLINT

  butil::Status ScanContinue(const std::string& scan_id, uint64_t max_fetch_cnt,
                             std::vector<pb::common::KeyValue>& kvs);  // NOLINT

  butil::Status ScanRelease(const std::string& scan_id);

  // Is it possible to delete this object
  bool IsRecyclable();

 protected:
  //
 private:
  void Close();
  static std::chrono::milliseconds GetCurrentTime();
  void GetKeyValue(std::vector<pb::common::KeyValue>& kvs);  // NOLINT

  std::string scan_id_;

  uint64_t region_id_;

  pb::common::PrefixScanRange range_;

  uint64_t max_fetch_cnt_;

  bool key_only_;

  bool disable_auto_release_;

  std::vector<pb::common::KeyValue> kvs_;

  pb::common::KeyValue kv_;

  ScanContextState state_;

  std::shared_ptr<RawEngine> engine_;

  std::string cf_name_;

  std::shared_ptr<EngineIterator> iter_;

  // call iter_->Start
  bool is_already_call_start_;

  // millisecond 1s = 1000 millisecond
  std::chrono::milliseconds last_time_ms_;

  // timeout millisecond to destroy
  uint64_t timeout_ms_;

  // Maximum number of bytes per transfer from rpc default 4M
  uint64_t max_bytes_rpc_;

  // kv count per transfer specified by the server
  uint64_t max_fetch_cnt_by_server_;

  bthread_mutex_t mutex_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_SCAN_H_  // NOLINT
