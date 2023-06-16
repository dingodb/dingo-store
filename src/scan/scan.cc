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

#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "butil/macros.h"     // IWYU pragma: keep
#include "common/constant.h"  // IWYU pragma: keep
#include "common/helper.h"    // IWYU pragma: keep
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "engine/write_data.h"  // IWYU pragma: keep
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

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

      ,
      seek_state_(SeekState::kUninit)

      ,
      disable_coprocessor_(true) {
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
  coprocessor_.reset();
  bthread_mutex_destroy(&mutex_);
}

std::chrono::milliseconds ScanContext::GetCurrentTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::chrono::nanoseconds nanosec = now.time_since_epoch();
  std::chrono::milliseconds millisec = std::chrono::duration_cast<std::chrono::milliseconds>(nanosec);

  return millisec;
}

butil::Status ScanContext::GetKeyValue(std::vector<pb::common::KeyValue>& kvs) {
  if (!is_already_call_start_) {
    iter_->Start();
    is_already_call_start_ = true;
  }

  if (!disable_coprocessor_) {
    butil::Status status;
    status = coprocessor_->Execute(iter_, key_only_, std::min(max_fetch_cnt_, max_fetch_cnt_by_server_), max_bytes_rpc_,
                                   &kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Coprocessor::Execute failed");
    }
    return status;
  }

  ScanFilter scan_filter = ScanFilter(key_only_, std::min(max_fetch_cnt_, max_fetch_cnt_by_server_), max_bytes_rpc_);

  while (iter_->HasNext()) {
    pb::common::KeyValue kv;
    std::string key;
    std::string value;
    if (key_only_) {
      iter_->GetKey(key);

    } else {
      iter_->GetKV(key, value);
    }

    kv.set_key(std::move(key));
    if (!key_only_) {
      kv.set_value(std::move(value));
    }

    kvs.emplace_back(kv);
    if (scan_filter.UptoLimit(kv)) {
      iter_->Next();
      break;
    }
    kv.Clear();

    iter_->Next();
  }

  return butil::Status();
}

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
                                     const pb::common::Range& range, uint64_t max_fetch_cnt, bool key_only,
                                     bool disable_auto_release, bool disable_coprocessor,
                                     const pb::store::Coprocessor& coprocessor,
                                     std::vector<pb::common::KeyValue>* kvs) {
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

  // opt if coprocessor all empty. set disable_coprocessor = true
  if (!disable_coprocessor) {
    if (Utils::CoprocessorParamEmpty(coprocessor)) {
      context->disable_coprocessor_ = true;
    } else {
      context->disable_coprocessor_ = disable_coprocessor;
      if (!context->disable_coprocessor_) {
        context->coprocessor_ = std::make_shared<Coprocessor>();
        butil::Status status = context->coprocessor_->Open(coprocessor);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format("Coprocessor::Open failed");
          return status;
        }
      }
    }
  }

  std::shared_ptr<RawEngine::Reader> reader = context->engine_->NewReader(context->cf_name_);

  context->iter_ = reader->NewIterator(context->range_.start_key(), context->range_.end_key());

  if (!context->iter_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("RawEngine::Reader::NewIterator failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error : create iter failed");
  }

  if (context->max_fetch_cnt_ > 0) {
    butil::Status s = context->GetKeyValue(*kvs);
    if (!s.ok()) {
      context->state_ = ScanState::kError;
      DINGO_LOG(ERROR) << fmt::format("ScanContext::GetKeyValue failed");
      return s;
    }

    context->seek_state_ = ScanContext::SeekState::kInitted;

  }

  else {  // NOLINT
    butil::Status s = context->AsyncWork();
    if (!s.ok()) {
      return s;
    }
  }

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

  context->WaitForReady();

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kBegun != context->state_ && ScanState::kContinued != context->state_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue failed : {}", static_cast<int>(context->state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  butil::Status s = context->SeekCheck();
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {}",
                                    static_cast<int>(context->seek_state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  context->max_fetch_cnt_ = max_fetch_cnt;

  context->state_ = ScanState::kContinuing;

  s = context->GetKeyValue(*kvs);
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

  context->WaitForReady();

  BAIDU_SCOPED_LOCK(context->mutex_);
  if (ScanState::kBegun != context->state_ && ScanState::kContinued != context->state_) {
    context->state_ = ScanState::kError;
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanRelease failed : {}", static_cast<int>(context->state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

  butil::Status s = context->SeekCheck();
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanHandler::ScanContinue SeekCheck  failed  state wrong : {}",
                                    static_cast<int>(context->seek_state_));
    return butil::Status(pb::error::EINTERNAL, "Internal error : wrong state");
  }

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
