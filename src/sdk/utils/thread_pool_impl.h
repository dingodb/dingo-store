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

#ifndef DINGODB_SDK_THREAD_POOL_IMPL_H_
#define DINGODB_SDK_THREAD_POOL_IMPL_H_

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "sdk/utils/thread_pool.h"

namespace dingodb {
namespace sdk {

class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl(int thread_num) : thread_num_(thread_num) {}

  ~ThreadPoolImpl() override;

  void Start() override;

  void JoinThreads() override;

  int GetBackgroundThreads() override;

  int GetQueueLen() const override;

  void Execute(const std::function<void()>& task) override;

  void Execute(std::function<void()>&& task) override;

 private:
  void ThreadProc(size_t thread_id);

  mutable std::mutex mutex_;
  int thread_num_{0};
  bool exit_{false};
  std::condition_variable condition_;
  std::vector<std::thread> threads_;
  std::queue<std::function<void()>> tasks_;
};

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_THREAD_POOL_IMPL_H_