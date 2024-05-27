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

#ifndef DINGODB_SDK_THREAD_POOL_H_
#define DINGODB_SDK_THREAD_POOL_H_

#include <functional>

namespace dingodb {
namespace sdk {

class ThreadPool {
 public:
  virtual ~ThreadPool() = default;

  virtual void Start() = 0;

  virtual void JoinThreads() = 0;

  virtual int GetBackgroundThreads() = 0;

  // Get the number of jobs scheduled in the ThreadPool queue.
  virtual int GetQueueLen() const = 0;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  virtual void Execute(const std::function<void()>&) = 0;

  // This moves the function in for efficiency
  virtual void Execute(std::function<void()>&&) = 0;
};

// NewThreadPool() is a function that could be used to create a ThreadPool
// with `num_threads` background threads.
ThreadPool* NewThreadPool(int num_threads);

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_THREAD_POOL_H_