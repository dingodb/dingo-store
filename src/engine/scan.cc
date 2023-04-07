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

#include "engine/scan.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/macros.h"
#include "butil/strings/stringprintf.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

ScanContext::ScanContext()
    : region_id_(0),
      max_fetch_cnt_(0),
      key_only_(false),
      disable_auto_release_(false),
      state_(ScanContextState::kUninitState),
      engine_(nullptr),
      iter_(nullptr),
      is_already_call_start_(false),
      last_time_ms_(),
      timeout_ms_(0),
      max_bytes_rpc_(0),
      max_fetch_cnt_by_server_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}
ScanContext::~ScanContext() { Close(); }

butil::Status ScanContext::Open(const std::string& scan_id, uint64_t timeout_ms, uint64_t max_bytes_rpc,
                                uint64_t max_fetch_cnt_by_server, std::shared_ptr<RawEngine> engine,
                                const std::string& cf_name) {
  if (BAIDU_UNLIKELY(scan_id.empty())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("scan_id empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (BAIDU_UNLIKELY(0 == timeout_ms)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("timeout_ms == 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "timeout_ms is 0");
  }

  if (BAIDU_UNLIKELY(0 == max_bytes_rpc)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("max_bytes_rpc == 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_bytes_rpc is 0");
  }

  if (BAIDU_UNLIKELY(0 == max_fetch_cnt_by_server)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("max_fetch_cnt_by_server == 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_fetch_cnt_by_server is 0");
  }

  if (BAIDU_UNLIKELY(!engine)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("engine empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "engine is empty");
  }

  if (BAIDU_UNLIKELY(cf_name.empty())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("cf_name empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "cf_name is empty");
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (ScanContextState::kUninitState != state_) {
    state_ = ScanContextState::kErrorState;
    DINGO_LOG(ERROR) << butil::StringPrintf("ScanContext::Open failed : %d", static_cast<int>(state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }
  state_ = ScanContextState::kOpeningState;
  scan_id_ = scan_id;
  engine_ = engine;
  timeout_ms_ = timeout_ms;
  cf_name_ = cf_name;
  max_bytes_rpc_ = max_bytes_rpc;
  max_fetch_cnt_by_server_ = max_fetch_cnt_by_server;
  state_ = ScanContextState::kOpenedState;
  return butil::Status();
}

butil::Status ScanContext::ScanBegin(uint64_t region_id, const pb::common::PrefixScanRange& range,
                                     uint64_t max_fetch_cnt, bool key_only, bool disable_auto_release,
                                     std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(range.range().start_key().empty() || range.range().end_key().empty())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("start_key or end_key empty not support ");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (BAIDU_UNLIKELY(range.range().start_key() > range.range().end_key())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("range wrong ");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");

  } else if (BAIDU_UNLIKELY(range.range().start_key() == range.range().end_key())) {
    if (range.with_start() && !range.with_end()) {
      DINGO_LOG(ERROR) << butil::StringPrintf("range wrong");
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
    }
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (ScanContextState::kOpenedState != state_) {
    state_ = ScanContextState::kErrorState;
    DINGO_LOG(ERROR) << butil::StringPrintf("ScanContext::ScanBegin failed : %d", static_cast<int>(state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  state_ = ScanContextState::kBeginningState;

  region_id_ = region_id;
  range_ = range;
  max_fetch_cnt_ = max_fetch_cnt;
  key_only_ = key_only;
  disable_auto_release_ = disable_auto_release;

  std::shared_ptr<RawEngine::Reader> reader = engine_->NewReader(cf_name_);

  iter_ =
      reader->NewIterator(range_.range().start_key(), range_.range().end_key(), range_.with_start(), range_.with_end());

  if (!iter_) {
    state_ = ScanContextState::kErrorState;
    DINGO_LOG(ERROR) << butil::StringPrintf("RawEngine::Reader::NewIterator failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error : create iter failed");
  }

  if (max_fetch_cnt_ > 0) {
    GetKeyValue(kvs);
  }

  state_ = ScanContextState::kBegunState;

  last_time_ms_ = GetCurrentTime();

  return butil::Status();
}

butil::Status ScanContext::ScanContinue(const std::string& scan_id, uint64_t max_fetch_cnt,
                                        std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(scan_id.empty() || scan_id != scan_id_)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("scan_id empty or unequal not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (BAIDU_UNLIKELY(0 == max_fetch_cnt)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("max_fetch_cnt == 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_fetch_cnt == 0");
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (ScanContextState::kBegunState != state_ && ScanContextState::kContinuedState != state_) {
    state_ = ScanContextState::kErrorState;
    DINGO_LOG(ERROR) << butil::StringPrintf("ScanContext::ScanContinue failed : %d", static_cast<int>(state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  max_fetch_cnt_ = max_fetch_cnt;

  state_ = ScanContextState::kContinuingState;

  GetKeyValue(kvs);

  state_ = ScanContextState::kContinuedState;
  last_time_ms_ = GetCurrentTime();

  return butil::Status();
}

butil::Status ScanContext::ScanRelease(const std::string& scan_id) {
  if (BAIDU_UNLIKELY(scan_id.empty() || scan_id != scan_id_)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("scan_id empty or unequal not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (ScanContextState::kBegunState != state_ && ScanContextState::kContinuedState != state_) {
    state_ = ScanContextState::kErrorState;
    DINGO_LOG(ERROR) << butil::StringPrintf("ScanContext::ScanContinue failed : %d", static_cast<int>(state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  state_ = ScanContextState::kReleasingState;

  if (!disable_auto_release_) {
    state_ = ScanContextState::kAllowImmediateRecycling;
  } else {
    state_ = ScanContextState::kReleasedState;
    last_time_ms_ = GetCurrentTime();
  }

  return butil::Status();
}

void ScanContext::Close() {
  scan_id_.clear();
  region_id_ = 0;
  range_.Clear();
  max_fetch_cnt_ = 0;
  key_only_ = false;
  disable_auto_release_ = false;
  kvs_.clear();
  kv_.Clear();
  state_ = ScanContextState::kUninitState;
  engine_ = nullptr;
  cf_name_.clear();
  iter_ = nullptr;
  is_already_call_start_ = false;
  last_time_ms_.zero();
  timeout_ms_ = 0;
  max_bytes_rpc_ = 0;
  max_fetch_cnt_by_server_ = 0;
  bthread_mutex_destroy(&mutex_);
}

std::chrono::milliseconds ScanContext::GetCurrentTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::chrono::nanoseconds nanosec = now.time_since_epoch();
  std::chrono::milliseconds millisec = std::chrono::duration_cast<std::chrono::milliseconds>(nanosec);

  return millisec;
}

void ScanContext::GetKeyValue(std::vector<pb::common::KeyValue>& kvs) {
  if (!is_already_call_start_) {
    iter_->Start();
    is_already_call_start_ = true;
  }

  uint64_t already_bytes = 0;
  for (uint64_t i = 0; iter_->HasNext() && i < max_fetch_cnt_ && i < max_fetch_cnt_by_server_; i++, iter_->Next()) {
    std::string key;
    std::string value;
    if (key_only_) {
      iter_->GetKey(key);

    } else {
      iter_->GetKV(key, value);
    }

    already_bytes += kv_.key().size();
    if (!key_only_) {
      already_bytes += kv_.value().size();
    }
    if (already_bytes >= max_bytes_rpc_) {
      break;
    }

    kv_.set_key(std::move(key));
    if (!key_only_) {
      kv_.set_value(std::move(value));
    }
    kvs_.emplace_back(std::move(kv_));
    kv_.Clear();
  }

  kvs = std::move(kvs_);
  kvs_.clear();
  kv_.Clear();
}

bool ScanContext::IsRecyclable() {
  bool ret = false;
  // speedup
  if (0 == bthread_mutex_trylock(&mutex_)) {
    do {
      if (ScanContextState::kAllowImmediateRecycling == state_ || ScanContextState::kErrorState == state_ ||
          ScanContextState::kBegunTimeoutState == state_ || ScanContextState::kContinuedTimeoutState == state_ ||
          ScanContextState::kReleasedTimeoutState == state_) {
        ret = true;
        break;
      }

      std::chrono::milliseconds now = GetCurrentTime();
      std::chrono::duration<uint64_t, std::milli> diff = now - last_time_ms_;
      if (diff.count() >= timeout_ms_) {
        state_ = ScanContextState::kAllowImmediateRecycling;
        ret = true;
        break;
      }
    } while (false);

    bthread_mutex_unlock(&mutex_);
  }

  return ret;
}

}  // namespace dingodb
