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

#include "sdk/utils/thread_pool_impl.h"

#include <cassert>
#include <mutex>

#include "glog/logging.h"
#include "sdk/common/param_config.h"

namespace dingodb {
namespace sdk {

ThreadPoolImpl::~ThreadPoolImpl() { JoinThreads(); }

void ThreadPoolImpl::ThreadProc(size_t thread_id) {
  VLOG(kSdkVlogLevel) << "Thread " << thread_id << " started.";

  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);

      condition_.wait(lock, [this] { return exit_ || !tasks_.empty(); });

      if (exit_ && tasks_.empty()) {
        break;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
    }

    (task)();
  }

  VLOG(kSdkVlogLevel) << "Thread " << thread_id << " exit.";
}

void ThreadPoolImpl::Start() {
  std::unique_lock<std::mutex> lg(mutex_);
  threads_.resize(thread_num_);
  for (size_t i = 0; i < thread_num_; i++) {
    threads_[i] = std::thread([this, i] { ThreadProc(i); });
  }
}

void ThreadPoolImpl::JoinThreads() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(!exit_);
    exit_ = true;
    condition_.notify_all();
  }

  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

int ThreadPoolImpl::GetBackgroundThreads() {
  std::lock_guard<std::mutex> lock(mutex_);
  return thread_num_;
}

int ThreadPoolImpl::GetQueueLen() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return tasks_.size();
}

void ThreadPoolImpl::Execute(const std::function<void()>& task) {
  auto cp(task);
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.push(std::move(cp));
  condition_.notify_one();
}

void ThreadPoolImpl::Execute(std::function<void()>&& task) {
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.push(std::move(task));
  condition_.notify_one();
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl(num_threads);
  return thread_pool;
}

}  // namespace sdk
}  // namespace dingodb