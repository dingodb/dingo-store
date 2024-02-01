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

#include "sdk/rawkv/raw_kv_task.h"

#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

Status RawKvTask::Run() {
  Status ret;
  Synchronizer sync;
  AsyncRun(sync.AsStatusCallBack(ret));
  sync.Wait();
  return ret;
}

void RawKvTask::AsyncRun(StatusCallback cb) {
  call_back_.swap(cb);
  status_ = Init();
  if (status_.ok()) {
    DoAsync();
  } else {
    FireCallback();
  }
}

Status RawKvTask::Init() { return Status::OK(); }

std::string RawKvTask::ErrorMsg() const { return ""; }

void RawKvTask::DoAsyncDone(const Status& status) {
  status_ = status;
  if (status_.ok()) {
    FireCallback();
  } else {
    FailOrRetry();
  }
}

void RawKvTask::FailOrRetry() {
  if (NeedRetry()) {
    BackoffAndRetry();
  } else {
    FireCallback();
  }
}

bool RawKvTask::NeedRetry() {
  if (status_.IsIncomplete()) {
    auto error_code = status_.Errno();
    if (error_code == pb::error::EREGION_VERSION || error_code == pb::error::EREGION_NOT_FOUND ||
        error_code == pb::error::EKEY_OUT_OF_RANGE) {
      retry_count_++;
      if (retry_count_ < kRawkvMaxRetry) {
        return true;
      } else {
        std::string msg =
            fmt::format("Fail task:{} retry too times:{}, last err:{}", Name(), retry_count_, status_.ToString());
        status_ = Status::Aborted(status_.Errno(), msg);
      }
    }
  }

  return false;
}

void RawKvTask::BackoffAndRetry() {
  stub.GetActuator()->Schedule([this] { DoAsync(); }, kRawkvBackoffMs);
}

void RawKvTask::FireCallback() {
  PostProcess();

  if (!status_.ok()) {
    DINGO_LOG(WARNING) << "Fail task:" << Name() << ", status:" << status_.ToString() << ", error_msg:" << ErrorMsg();
  }
  call_back_(status_);

  StatusCallback cb;
  call_back_.swap(cb);
}

void RawKvTask::PostProcess() {}

}  // namespace sdk
}  // namespace dingodb