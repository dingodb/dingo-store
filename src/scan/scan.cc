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

#include <fmt/core.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "butil/macros.h"     // IWYU pragma: keep
#include "common/constant.h"  // IWYU pragma: keep
#include "common/helper.h"    // IWYU pragma: keep
#include "common/logging.h"
#include "coprocessor/coprocessor.h"
#include "coprocessor/coprocessor_v2.h"
#include "coprocessor/utils.h"
#include "engine/write_data.h"  // IWYU pragma: keep
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "scan/scan_filter.h"
#if defined(ENABLE_SCAN_OPTIMIZATION)
#include "bthread/bthread.h"
#endif

namespace dingodb {

ScanContext::ScanContext(bvar::LatencyRecorder* scan_latency)
    : region_id_(0),
      max_fetch_cnt_(0),
      key_only_(false),
      disable_auto_release_(false),
      state_(ScanState::kUninit),
      engine_(nullptr),
      iter_(nullptr),
      last_time_ms_(GetCurrentTime())
#if defined(ENABLE_SCAN_OPTIMIZATION)
      ,
      seek_state_(SeekState::kUninit)
#endif
      ,
      disable_coprocessor_(true),
      timeout_ms_(0),
      max_bytes_rpc_(0),
      max_fetch_cnt_by_server_(0),
      scan_latency_(scan_latency),
      bvar_guard_(scan_latency_) {
  bthread_mutex_init(&mutex_, nullptr);
}
ScanContext::~ScanContext() { Close(); }

void ScanContext::Init(int64_t timeout_ms, int64_t max_bytes_rpc, int64_t max_fetch_cnt_by_server) {
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
    std::string s = fmt::format("ScanContext::Open failed : {} {}", static_cast<int>(state_), GetScanState(state_));
    DINGO_LOG(ERROR) << s;
    state_ = ScanState::kError;
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state  -> ScanState::kError : %s", s.c_str());
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
  last_time_ms_.zero();
  coprocessor_.reset();
  bthread_mutex_destroy(&mutex_);
}

std::chrono::milliseconds ScanContext::GetCurrentTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::chrono::nanoseconds nanosec = now.time_since_epoch();
  std::chrono::milliseconds millisec = std::chrono::duration_cast<std::chrono::milliseconds>(nanosec);

  return millisec;
}

butil::Status ScanContext::GetKeyValue(std::vector<pb::common::KeyValue>& kvs, bool& has_more) {
  if (!disable_coprocessor_) {
    butil::Status status;
    status = coprocessor_->Execute(iter_, key_only_, std::min(max_fetch_cnt_, max_fetch_cnt_by_server_), max_bytes_rpc_,
                                   &kvs, has_more);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Coprocessor::Execute failed");
    }
    return status;
  }

  ScanFilter scan_filter = ScanFilter(key_only_, std::min(max_fetch_cnt_, max_fetch_cnt_by_server_), max_bytes_rpc_);

  has_more = false;
  while (iter_->Valid()) {
    pb::common::KeyValue kv;
    *kv.mutable_key() = iter_->Key();
    if (!key_only_) {
      *kv.mutable_value() = iter_->Value();
    }

    kvs.emplace_back(kv);
    if (scan_filter.UptoLimit(kv)) {
      has_more = true;
      iter_->Next();
      break;
    }
    kv.Clear();

    iter_->Next();
  }

  return butil::Status();
}

#if defined(ENABLE_SCAN_OPTIMIZATION)
butil::Status ScanContext::AsyncWork() {
  auto lambda_call = [this]() {
    BAIDU_SCOPED_LOCK(mutex_);
    seek_state_ = ScanContext::SeekState::kInitting;
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
    std::string s = fmt::format("ScanHandler::ScanContinue failed  state wrong : {} {} seek_state : {} {}",
                                static_cast<int>(state_), GetScanState(state_), static_cast<int>(seek_state_),
                                GetSeekState(seek_state_));
    DINGO_LOG(ERROR) << s;
    state_ = ScanState::kError;
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state  -> ScanState::kError : %s", s.c_str());
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
        std::string s = fmt::format("Recycle Immediate state: {} {} now: {} scan_id: {}", static_cast<int>(state_),
                                    GetScanState(state_), Helper::NowTime(), scan_id_);
        DINGO_LOG(INFO) << s;
        ret = true;
        break;
      }

      std::chrono::milliseconds now = GetCurrentTime();
      std::chrono::duration<int64_t, std::milli> diff = now - last_time_ms_;
      if (diff.count() >= timeout_ms_) {
        std::string last_time_ms_str;
        last_time_ms_str = Helper::FormatMsTime(last_time_ms_.count(), "%Y-%m-%d %H:%M:%S");
        std::string s =
            fmt::format("Recycle Next Loop state: {} {} now: {} last_time: {} scan_id: {}", static_cast<int>(state_),
                        GetScanState(state_), Helper::NowTime(), last_time_ms_str, scan_id_);
        DINGO_LOG(INFO) << s;
        state_ = ScanState::kAllowImmediateRecycling;
        ret = true;
        break;
      }
    } while (false);

    bthread_mutex_unlock(&mutex_);
  }

  return ret;
}

const char* ScanContext::GetScanState(ScanState state) {
  const char* state_str = "Unknow Scan State";
  switch (state) {
    case ScanState::kUninit: {
      state_str = "ScanState::kUninit";
      break;
    }
    case ScanState::kOpening: {
      state_str = "ScanState::kOpening";
      break;
    }
    case ScanState::kOpened: {
      state_str = "ScanState::kOpened";
      break;
    }
    case ScanState::kBeginning: {
      state_str = "ScanState::kBeginning";
      break;
    }
    case ScanState::kBegun: {
      state_str = "ScanState::kBegun";
      break;
    }
    case ScanState::kContinuing: {
      state_str = "ScanState::kContinuing";
      break;
    }
    case ScanState::kContinued: {
      state_str = "ScanState::kContinued";
      break;
    }
    case ScanState::kReleasing: {
      state_str = "ScanState::kReleasing";
      break;
    }
    case ScanState::kReleased: {
      state_str = "ScanState::kReleased";
      break;
    }
    case ScanState::kError: {
      state_str = "ScanState::kError";
      break;
    }
    case ScanState::kBegunTimeout: {
      state_str = "ScanState::kBegunTimeout";
      break;
    }
    case ScanState::kContinuedTimeout: {
      state_str = "ScanState::kContinuedTimeout";
      break;
    }
    case ScanState::kReleasedTimeout: {
      state_str = "ScanState::kReleasedTimeout";
      break;
    }
    case ScanState::kAllowImmediateRecycling: {
      state_str = "ScanState::kAllowImmediateRecycling";
      break;
    }
    case ScanState::kDestroy: {
      state_str = "ScanState::kDestroy";
      break;
    }
    default: {
      break;
    }
  }

  return state_str;
}

#if defined(ENABLE_SCAN_OPTIMIZATION)
const char* ScanContext::GetSeekState(SeekState state) {
  const char* state_str = "Unknow Seek State";
  switch (state) {
    case SeekState::kUninit: {
      state_str = "SeekState::kUninit";
      break;
    }
    case SeekState::kInitting: {
      state_str = "SeekState::kInitting";
      break;
    }
    case SeekState::kInitted: {
      state_str = "SeekState::kInitted";
      break;
    }
    default:
      break;
  }

  return state_str;
}
#endif

ScanContextV1::ScanContextV1(bvar::LatencyRecorder* scan_latency) : ScanContext(scan_latency) {}
ScanContextV1::~ScanContextV1() = default;
bvar::LatencyRecorder* ScanContextV1::GetScanLatency() { return &scan_context_v1_latency; }
bvar::LatencyRecorder ScanContextV1::scan_context_v1_latency("dingo_scan_context_v1_latency");

ScanContextV2::ScanContextV2(bvar::LatencyRecorder* scan_latency) : ScanContext(scan_latency) {}
ScanContextV2::~ScanContextV2() = default;
bvar::LatencyRecorder* ScanContextV2::GetScanLatency() { return &scan_context_v2_latency; }
bvar::LatencyRecorder ScanContextV2::scan_context_v2_latency("dingo_scan_context_v2_latency");

butil::Status ScanHandler::ScanBegin(std::shared_ptr<ScanContext> context, int64_t region_id,
                                     const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                                     bool disable_auto_release, bool disable_coprocessor,
                                     const CoprocessorPbWrapper& coprocessor, std::vector<pb::common::KeyValue>* kvs) {
  if (BAIDU_UNLIKELY(range.start_key().empty() || range.end_key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("start_key or end_key empty not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (BAIDU_UNLIKELY(range.start_key() >= range.end_key())) {
    DINGO_LOG(ERROR) << fmt::format("range wrong");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kOpened != context->state_) {
    std::string s = fmt::format("ScanHandler::ScanBegin failed : {} {}", static_cast<int>(context->state_),
                                context->GetScanState(context->state_));
    DINGO_LOG(ERROR) << s;
    context->state_ = ScanState::kError;
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state  -> ScanState::kError : %s", s.c_str());
  }

  context->state_ = ScanState::kBeginning;

  context->region_id_ = region_id;
  context->range_ = range;
  context->max_fetch_cnt_ = max_fetch_cnt;
  context->key_only_ = key_only;
  context->disable_auto_release_ = disable_auto_release;

  // opt if coprocessor all empty. set disable_coprocessor = true
  if (!disable_coprocessor) {
    const pb::store::Coprocessor* coprocessor_v1 = std::get_if<pb::store::Coprocessor>(&coprocessor);
    if (nullptr != coprocessor_v1) {  // pb::store::CoprocessorV1
      if (Utils::CoprocessorParamEmpty(*coprocessor_v1)) {
        context->disable_coprocessor_ = true;
      } else {
        context->disable_coprocessor_ = disable_coprocessor;
        if (!context->disable_coprocessor_) {
          context->coprocessor_ = std::make_shared<Coprocessor>();
        }
      }
    } else {  // pb::common::CoprocessorV2
      const pb::common::CoprocessorV2* coprocessor_v2 = std::get_if<pb::common::CoprocessorV2>(&coprocessor);
      if (nullptr != coprocessor_v2) {
        context->disable_coprocessor_ = disable_coprocessor;
        context->coprocessor_ = std::make_shared<CoprocessorV2>();
      }
    }

    if (!context->disable_coprocessor_ && context->coprocessor_) {
      butil::Status status = context->coprocessor_->Open(coprocessor);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Coprocessor::Open failed");
        return status;
      }
    }
  }

  auto reader = context->engine_->Reader();

  IteratorOptions options;
  options.upper_bound = context->range_.end_key();

  context->iter_ = reader->NewIterator(context->cf_name_, options);
  if (!context->iter_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("RawEngine::Reader::NewIterator failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error : create iter failed");
  }
  context->iter_->Seek(context->range_.start_key());

  if (context->max_fetch_cnt_ > 0) {
    bool has_more = false;
    butil::Status s = context->GetKeyValue(*kvs, has_more);
    if (!s.ok()) {
      context->state_ = ScanState::kError;
      DINGO_LOG(ERROR) << fmt::format("ScanContext::GetKeyValue failed");
      return s;
    }

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
                                        int64_t max_fetch_cnt, std::vector<pb::common::KeyValue>* kvs, bool& has_more) {
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
    std::string s = fmt::format("ScanHandler::ScanContinue failed : {} {}", static_cast<int>(context->state_),
                                context->GetScanState(context->state_));
    DINGO_LOG(ERROR) << s;
    context->state_ = ScanState::kError;
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state  -> ScanState::kError : %s", s.c_str());
  }
  butil::Status s;
#if defined(ENABLE_SCAN_OPTIMIZATION)
  s = context->SeekCheck();
  if (!s.ok()) {
    std::string str = fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {} {}",
                                  static_cast<int>(context->seek_state_), context->GetScanState(context->state_));
    DINGO_LOG(ERROR) << str;
    return s;
  }
#endif

  context->max_fetch_cnt_ = max_fetch_cnt;

  context->state_ = ScanState::kContinuing;

  s = context->GetKeyValue(*kvs, has_more);
  if (!s.ok()) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanContext::GetKeyValue failed");
    return s;
  }

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
    std::string s = fmt::format("ScanHandler::ScanRelease failed : {} {}", static_cast<int>(context->state_),
                                context->GetScanState(context->state_));
    DINGO_LOG(ERROR) << s;
    context->state_ = ScanState::kError;
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state  -> ScanState::kError : %s", s.c_str());
  }

#if defined(ENABLE_SCAN_OPTIMIZATION)
  butil::Status s = context->SeekCheck();
  if (!s.ok()) {
    std::string str = fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {} {}",
                                  static_cast<int>(context->seek_state_), context->GetScanState(context->state_));
    DINGO_LOG(ERROR) << str;
    return s;
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
