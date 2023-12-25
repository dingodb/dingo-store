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

#include "common/synchronization.h"

namespace dingodb {

// BthreadCond
BthreadCond::BthreadCond(int count) {
  bthread_cond_init(&cond_, nullptr);
  bthread_mutex_init(&mutex_, nullptr);
  count_ = count;
}

BthreadCond::~BthreadCond() {
  bthread_mutex_destroy(&mutex_);
  bthread_cond_destroy(&cond_);
}

int BthreadCond::Count() const { return count_; }

void BthreadCond::Increase() {
  bthread_mutex_lock(&mutex_);
  ++count_;
  bthread_mutex_unlock(&mutex_);
}

void BthreadCond::DecreaseSignal() {
  bthread_mutex_lock(&mutex_);
  --count_;
  bthread_cond_signal(&cond_);
  bthread_mutex_unlock(&mutex_);
}

void BthreadCond::DecreaseBroadcast() {
  bthread_mutex_lock(&mutex_);
  --count_;
  bthread_cond_broadcast(&cond_);
  bthread_mutex_unlock(&mutex_);
}

int BthreadCond::Wait(int cond) {
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

int BthreadCond::IncreaseWait(int cond) {
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

int BthreadCond::TimedWait(int64_t timeout_us, int cond) {
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

int BthreadCond::IncreaseTimedWait(int64_t timeout_us, int cond) {
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

// wrapper bthread functions for c++ style
Bthread::Bthread(const bthread_attr_t* attr) : attr_(attr) {}

Bthread::Bthread(const std::function<void()>& call) {
  attr_ = &BTHREAD_ATTR_NORMAL;
  Run(call);
}

Bthread::Bthread(const bthread_attr_t* attr, const std::function<void()>& call) {
  attr_ = attr;
  Run(call);
}

void Bthread::Run(const std::function<void()>& call) {
  std::function<void()>* func_call = new std::function<void()>;
  *func_call = call;
  int ret = bthread_start_background(
      &tid_, attr_,
      [](void* p) -> void* {
        auto* call = static_cast<std::function<void()>*>(p);
        (*call)();
        delete call;
        return nullptr;
      },
      func_call);
  if (ret != 0) {
    DINGO_LOG(FATAL) << "bthread_start_background fail.";
  }
}

void Bthread::RunUrgent(const std::function<void()>& call) {
  std::function<void()>* func_call = new std::function<void()>;
  *func_call = call;
  int ret = bthread_start_urgent(
      &tid_, attr_,
      [](void* p) -> void* {
        auto* call = static_cast<std::function<void()>*>(p);
        (*call)();
        delete call;
        return nullptr;
      },
      func_call);
  if (ret != 0) {
    DINGO_LOG(FATAL) << "bthread_start_urgent fail";
  }
}

void Bthread::Join() const { bthread_join(tid_, nullptr); }

bthread_t Bthread::Id() const { return tid_; }

// ScopeGuard
ScopeGuard::ScopeGuard(std::function<void()> exit_func) : exit_func_(exit_func) {}

ScopeGuard::~ScopeGuard() {
  if (!is_release_) {
    exit_func_();
  }
}

void ScopeGuard::Release() { is_release_ = true; }

// RWLock
bool RWLock::CanRead() const {
  // Readers can read only if there is no active writer and no writers are waiting
  return !active_writer_ && waiting_writers_ == 0;
}

bool RWLock::CanWrite() const {
  // Writers can write only if there are no active readers and no active writer
  return active_readers_ == 0 && !active_writer_;
}

RWLock::RWLock() {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

RWLock::~RWLock() {
  bthread_mutex_destroy(&mutex_);
  bthread_cond_destroy(&cond_);
}

void RWLock::LockRead() {
  BAIDU_SCOPED_LOCK(mutex_);
  while (!CanRead()) {
    bthread_cond_wait(&cond_, &mutex_);
  }
  active_readers_++;
}

void RWLock::UnlockRead() {
  BAIDU_SCOPED_LOCK(mutex_);
  active_readers_--;
  if (active_readers_ == 0) {
    bthread_cond_broadcast(&cond_);
  }
}

void RWLock::LockWrite() {
  BAIDU_SCOPED_LOCK(mutex_);
  waiting_writers_++;
  while (!CanWrite()) {
    bthread_cond_wait(&cond_, &mutex_);
  }
  waiting_writers_--;
  active_writer_ = true;
}

void RWLock::UnlockWrite() {
  BAIDU_SCOPED_LOCK(mutex_);
  active_writer_ = false;
  bthread_cond_broadcast(&cond_);
}

// RWLockReadGuard
RWLockReadGuard::RWLockReadGuard(RWLock* rw_lock) {
  rw_lock_ = rw_lock;
  rw_lock_->LockRead();
}

RWLockReadGuard::~RWLockReadGuard() {
  if (!is_release_) {
    rw_lock_->UnlockRead();
  }
}

// RWLockWriteGuard
void RWLockReadGuard::Release() { is_release_ = true; }

RWLockWriteGuard::RWLockWriteGuard(RWLock* rw_lock) {
  rw_lock_ = rw_lock;
  rw_lock_->LockWrite();
}

RWLockWriteGuard::~RWLockWriteGuard() {
  if (!is_release_) {
    rw_lock_->UnlockWrite();
  }
}

void RWLockWriteGuard::Release() { is_release_ = true; }

}  // namespace dingodb