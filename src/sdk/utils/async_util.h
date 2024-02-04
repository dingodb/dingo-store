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

#ifndef DINGODB_SDK_ASYNC_UTIL_H_
#define DINGODB_SDK_ASYNC_UTIL_H_

#include <condition_variable>
#include <mutex>

#include "sdk/status.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

class Synchronizer {
 public:
  Synchronizer() = default;

  void Wait() {
    std::unique_lock<std::mutex> lk(lock_);
    while (!fire_) {
      cv_.wait(lk);
    }
  }

  RpcCallback AsRpcCallBack() {
    return [&]() { Fire(); };
  }

  StatusCallback AsStatusCallBack(Status& in_staus) {
    return [&](Status s) {
      in_staus = s;
      Fire();
    };
  }

  void Fire() {
    std::unique_lock<std::mutex> lk(lock_);
    fire_ = true;
    cv_.notify_one();
  }

 private:
  std::mutex lock_;
  std::condition_variable cv_;
  bool fire_{false};
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_ASYNC_UTIL_H_