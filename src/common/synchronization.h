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

#ifndef DINGODB_COMMON_SYNCHRONIZATION_H_
#define DINGODB_COMMON_SYNCHRONIZATION_H_

#include "bthread/bthread.h"
#include "bthread/butex.h"
#include "common/logging.h"

namespace dingodb {

class BthreadCond {
 public:
  BthreadCond(int count = 0) {
    bthread_cond_init(&cond_, nullptr);
    bthread_mutex_init(&mutex_, nullptr);
    count_ = count;
  }
  ~BthreadCond() {
    bthread_mutex_destroy(&mutex_);
    bthread_cond_destroy(&cond_);
  }

  int Count() const { return count_; }

  void Increase() {
    bthread_mutex_lock(&mutex_);
    ++count_;
    bthread_mutex_unlock(&mutex_);
  }

  void DecreaseSignal() {
    bthread_mutex_lock(&mutex_);
    --count_;
    bthread_cond_signal(&cond_);
    bthread_mutex_unlock(&mutex_);
  }

  void DecreaseBroadcast() {
    bthread_mutex_lock(&mutex_);
    --count_;
    bthread_cond_broadcast(&cond_);
    bthread_mutex_unlock(&mutex_);
  }

  int Wait(int cond = 0) {
    int ret = 0;
    bthread_mutex_lock(&mutex_);
    while (count_ > cond) {
      ret = bthread_cond_wait(&cond_, &mutex_);
      if (ret != 0) {
        DINGO_LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    bthread_mutex_unlock(&mutex_);
    return ret;
  }

  int IncreaseWait(int cond = 0) {
    int ret = 0;
    bthread_mutex_lock(&mutex_);
    while (count_ + 1 > cond) {
      ret = bthread_cond_wait(&cond_, &mutex_);
      if (ret != 0) {
        DINGO_LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    ++count_;
    bthread_mutex_unlock(&mutex_);
    return ret;
  }

  int TimedWait(int64_t timeout_us, int cond = 0) {
    int ret = 0;
    timespec tm = butil::microseconds_from_now(timeout_us);
    bthread_mutex_lock(&mutex_);
    while (count_ > cond) {
      ret = bthread_cond_timedwait(&cond_, &mutex_, &tm);
      if (ret != 0) {
        DINGO_LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    bthread_mutex_unlock(&mutex_);
    return ret;
  }

  int IncreaseTimedWait(int64_t timeout_us, int cond = 0) {
    int ret = 0;
    timespec tm = butil::microseconds_from_now(timeout_us);
    bthread_mutex_lock(&mutex_);
    while (count_ + 1 > cond) {
      ret = bthread_cond_timedwait(&cond_, &mutex_, &tm);
      if (ret != 0) {
        DINGO_LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    ++count_;
    bthread_mutex_unlock(&mutex_);
    return ret;
  }

 private:
  int count_;
  bthread_cond_t cond_;
  bthread_mutex_t mutex_;
};

};  // namespace dingodb

#endif  // DINGODB_COMMON_SYNCHRONIZATION_H_