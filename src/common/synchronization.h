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

namespace dingodb {

class BthreadCond {
 public:
  BthreadCond(int count = 0) {
    bthread_cond_init(&_cond, nullptr);
    bthread_mutex_init(&_mutex, nullptr);
    _count = count;
  }
  ~BthreadCond() {
    bthread_mutex_destroy(&_mutex);
    bthread_cond_destroy(&_cond);
  }

  int Count() const { return _count; }

  void Increase() {
    bthread_mutex_lock(&_mutex);
    ++_count;
    bthread_mutex_unlock(&_mutex);
  }

  void DecreaseSignal() {
    bthread_mutex_lock(&_mutex);
    --_count;
    bthread_cond_signal(&_cond);
    bthread_mutex_unlock(&_mutex);
  }

  void DecreaseBroadcast() {
    bthread_mutex_lock(&_mutex);
    --_count;
    bthread_cond_broadcast(&_cond);
    bthread_mutex_unlock(&_mutex);
  }

  int Wait(int cond = 0) {
    int ret = 0;
    bthread_mutex_lock(&_mutex);
    while (_count > cond) {
      ret = bthread_cond_wait(&_cond, &_mutex);
      if (ret != 0) {
        LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    bthread_mutex_unlock(&_mutex);
    return ret;
  }

  int IncreaseWait(int cond = 0) {
    int ret = 0;
    bthread_mutex_lock(&_mutex);
    while (_count + 1 > cond) {
      ret = bthread_cond_wait(&_cond, &_mutex);
      if (ret != 0) {
        LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    ++_count;
    bthread_mutex_unlock(&_mutex);
    return ret;
  }

  int TimedWait(int64_t timeout_us, int cond = 0) {
    int ret = 0;
    timespec tm = butil::microseconds_from_now(timeout_us);
    bthread_mutex_lock(&_mutex);
    while (_count > cond) {
      ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
      if (ret != 0) {
        LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    bthread_mutex_unlock(&_mutex);
    return ret;
  }

  int IncreaseTimedWait(int64_t timeout_us, int cond = 0) {
    int ret = 0;
    timespec tm = butil::microseconds_from_now(timeout_us);
    bthread_mutex_lock(&_mutex);
    while (_count + 1 > cond) {
      ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
      if (ret != 0) {
        LOG(WARNING) << "wait timeout, ret: " << ret;
        break;
      }
    }

    ++_count;
    bthread_mutex_unlock(&_mutex);
    return ret;
  }

 private:
  int _count;
  bthread_cond_t _cond;
  bthread_mutex_t _mutex;
};

};  // namespace dingodb

#endif  // DINGODB_COMMON_SYNCHRONIZATION_H_