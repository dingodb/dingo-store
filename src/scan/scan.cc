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

#include "scan/scan.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "butil/macros.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/write_data.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#if defined(ENABLE_SCAN_OPTIMIZATION)
#include "bthread/bthread.h"
#endif

namespace dingodb {

// timeout millisecond to destroy
uint64_t ScanContext::timeout_ms_ = 0;

// Maximum number of bytes per transfer from rpc default 4M
uint64_t ScanContext::max_bytes_rpc_ = 0;

// kv count per transfer specified by the server
uint64_t ScanContext::max_fetch_cnt_by_server_ = 0;

ScanContext::ScanContext()
    : region_id_(0),
      max_fetch_cnt_(0),
      key_only_(false),
      disable_auto_release_(false),
      state_(ScanState::kUninit),
      engine_(nullptr),
      iter_(nullptr),
      is_already_call_start_(false),
      last_time_ms_()
#if defined(ENABLE_SCAN_OPTIMIZATION)
      ,
      seek_state_(SeekState::kUninit)
#endif
{
  bthread_mutex_init(&mutex_, nullptr);
}
ScanContext::~ScanContext() { Close(); }

void ScanContext::Init(uint64_t timeout_ms, uint64_t max_bytes_rpc, uint64_t max_fetch_cnt_by_server) {
  timeout_ms_ = timeout_ms;
  max_bytes_rpc_ = max_bytes_rpc;
  max_fetch_cnt_by_server_ = max_fetch_cnt_by_server;
}

butil::Status ScanContext::Open(const std::string& scan_id, std::shared_ptr<RawEngine> engine,
                                const std::string& cf_name) {
  if (BAIDU_UNLIKELY(scan_id.empty())) {
    DINGO_LOG(ERROR) << fmt::format("scan_id empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (BAIDU_UNLIKELY(!engine)) {
    DINGO_LOG(ERROR) << fmt::format("engine empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "engine is empty");
  }

  if (BAIDU_UNLIKELY(cf_name.empty())) {
    DINGO_LOG(ERROR) << fmt::format("cf_name empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "cf_name is empty");
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (ScanState::kUninit != state_) {
    state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanContext::Open failed : {}", static_cast<int>(state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }
  state_ = ScanState::kOpening;
  scan_id_ = scan_id;
  engine_ = engine;
  cf_name_ = cf_name;
  state_ = ScanState::kOpened;
  return butil::Status();
}

void ScanContext::Close() {
  scan_id_.clear();
  region_id_ = 0;
  range_.Clear();
  max_fetch_cnt_ = 0;
  key_only_ = false;
  disable_auto_release_ = false;
  state_ = ScanState::kUninit;
  engine_ = nullptr;
  cf_name_.clear();
  iter_ = nullptr;
  is_already_call_start_ = false;
  last_time_ms_.zero();
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

  pb::common::KeyValue kv;

  uint64_t already_bytes = 0;
  for (uint64_t i = 0; iter_->HasNext() && i < max_fetch_cnt_ && i < max_fetch_cnt_by_server_; i++, iter_->Next()) {
    std::string key;
    std::string value;
    if (key_only_) {
      iter_->GetKey(key);

    } else {
      iter_->GetKV(key, value);
    }

    already_bytes += kv.key().size();
    if (!key_only_) {
      already_bytes += kv.value().size();
    }
    if (already_bytes >= max_bytes_rpc_) {
      break;
    }

    kv.set_key(std::move(key));
    if (!key_only_) {
      kv.set_value(std::move(value));
    }
    kvs.emplace_back(std::move(kv));
    kv.Clear();
  }
}

#if defined(ENABLE_SCAN_OPTIMIZATION)
butil::Status ScanContext::AsyncWork() {
  auto lambda_call = [this]() {
    BAIDU_SCOPED_LOCK(mutex_);
    seek_state_ = ScanContext::SeekState::kInitting;
    if (!is_already_call_start_) {
      iter_->Start();
      is_already_call_start_ = true;
    }
    last_time_ms_ = GetCurrentTime();
    seek_state_ = SeekState::kInitted;
  };

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(pb::error::EINTERNAL, "scan_id is empty");
  }

  return butil::Status();
}

void ScanContext::WaitForReady() {
  int i = 0;
  while (ScanContext::SeekState::kUninit == seek_state_) {
    if (i++ >= 1000) {
      bthread_yield();
      i = 0;
    }
  }
}
butil::Status ScanContext::SeekCheck() {
  if (ScanContext::SeekState::kInitted != seek_state_) {
    state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue failed  state wrong : {}",
                                    static_cast<int>(seek_state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }
  return butil::Status();
}
#endif

bool ScanContext::IsRecyclable() {
  bool ret = false;
  // speedup
  if (0 == bthread_mutex_trylock(&mutex_)) {
    do {
      if (ScanState::kAllowImmediateRecycling == state_ || ScanState::kError == state_ ||
          ScanState::kBegunTimeout == state_ || ScanState::kContinuedTimeout == state_ ||
          ScanState::kReleasedTimeout == state_) {
        ret = true;
        break;
      }

      std::chrono::milliseconds now = GetCurrentTime();
      std::chrono::duration<uint64_t, std::milli> diff = now - last_time_ms_;
      if (diff.count() >= timeout_ms_) {
        state_ = ScanState::kAllowImmediateRecycling;
        ret = true;
        break;
      }
    } while (false);

    bthread_mutex_unlock(&mutex_);
  }

  return ret;
}

butil::Status ScanHandler::ScanBegin(std::shared_ptr<ScanContext> context, uint64_t region_id,
                                     const pb::common::RangeWithOptions& range, uint64_t max_fetch_cnt, bool key_only,
                                     bool disable_auto_release, std::vector<pb::common::KeyValue>* kvs) {
  if (BAIDU_UNLIKELY(range.range().start_key().empty() || range.range().end_key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("start_key or end_key empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (BAIDU_UNLIKELY(range.range().start_key() > range.range().end_key())) {
    DINGO_LOG(ERROR) << fmt::format("range wrong");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");

  } else if (BAIDU_UNLIKELY(range.range().start_key() == range.range().end_key())) {
    if (!range.with_start() || !range.with_end()) {
      DINGO_LOG(ERROR) << fmt::format("range wrong");
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
    }
  }

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kOpened != context->state_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanBegin failed : {}", static_cast<int>(context->state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  context->state_ = ScanState::kBeginning;

  context->region_id_ = region_id;
  context->range_ = range;
  context->max_fetch_cnt_ = max_fetch_cnt;
  context->key_only_ = key_only;
  context->disable_auto_release_ = disable_auto_release;

  std::shared_ptr<RawEngine::Reader> reader = context->engine_->NewReader(context->cf_name_);

  context->iter_ = reader->NewIterator(context->range_.range().start_key(), context->range_.range().end_key(),
                                       context->range_.with_start(), context->range_.with_end());

  if (!context->iter_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("RawEngine::Reader::NewIterator failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error : create iter failed");
  }

  if (context->max_fetch_cnt_ > 0) {
    context->GetKeyValue(*kvs);
#if defined(ENABLE_SCAN_OPTIMIZATION)
    context->seek_state_ = ScanContext::SeekState::kInitted;
#endif
  }
#if defined(ENABLE_SCAN_OPTIMIZATION)
  else {  // NOLINT
    butil::Status s = context->AsyncWork();
    if (!s.ok()) {
      return s;
    }
  }
#endif

  context->state_ = ScanState::kBegun;

  context->last_time_ms_ = context->GetCurrentTime();

  return butil::Status();
}

butil::Status ScanHandler::ScanContinue(std::shared_ptr<ScanContext> context, const std::string& scan_id,
                                        uint64_t max_fetch_cnt, std::vector<pb::common::KeyValue>* kvs) {
  if (BAIDU_UNLIKELY(scan_id.empty() || scan_id != context->scan_id_)) {
    DINGO_LOG(ERROR) << fmt::format("scan_id empty or unequal not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (BAIDU_UNLIKELY(0 == max_fetch_cnt)) {
    DINGO_LOG(ERROR) << fmt::format("max_fetch_cnt == 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_fetch_cnt == 0");
  }

#if defined(ENABLE_SCAN_OPTIMIZATION)
  context->WaitForReady();
#endif

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kBegun != context->state_ && ScanState::kContinued != context->state_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue failed : {}", static_cast<int>(context->state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

#if defined(ENABLE_SCAN_OPTIMIZATION)
  butil::Status s = context->SeekCheck();
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {}",
                                    static_cast<int>(context->seek_state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }
#endif

  context->max_fetch_cnt_ = max_fetch_cnt;

  context->state_ = ScanState::kContinuing;

  context->GetKeyValue(*kvs);

  context->state_ = ScanState::kContinued;
  context->last_time_ms_ = context->GetCurrentTime();

  return butil::Status();
}

butil::Status ScanHandler::ScanRelease(std::shared_ptr<ScanContext> context,
                                       [[maybe_unused]] const std::string& scan_id) {
  if (BAIDU_UNLIKELY(scan_id.empty() || scan_id != context->scan_id_)) {
    DINGO_LOG(ERROR) << fmt::format("scan_id empty or unequal not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

#if defined(ENABLE_SCAN_OPTIMIZATION)
  context->WaitForReady();
#endif

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kBegun != context->state_ && ScanState::kContinued != context->state_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanRelease failed : {}", static_cast<int>(context->state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

#if defined(ENABLE_SCAN_OPTIMIZATION)
  butil::Status s = context->SeekCheck();
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {}",
                                    static_cast<int>(context->seek_state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }
#endif

  context->state_ = ScanState::kReleasing;

  if (!context->disable_auto_release_) {
    context->state_ = ScanState::kAllowImmediateRecycling;
  } else {
    context->state_ = ScanState::kReleased;
    context->last_time_ms_ = context->GetCurrentTime();
  }

  return butil::Status();
}

}  // namespace dingodb
