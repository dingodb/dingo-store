
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

#ifndef DINGODB_SDK_THREAD_POOL_ACTUATOR_H_
#define DINGODB_SDK_THREAD_POOL_ACTUATOR_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "common/threadpool.h"
#include "sdk/utils/actuator.h"

namespace dingodb {
namespace sdk {

class Timer {
 public:
  Timer();
  ~Timer();

  bool Start(Actuator* actuator);

  bool Stop();

  bool Add(std::function<void()> func, int delay_ms);

  bool IsStopped() {
    std::lock_guard<std::mutex> lk(mutex_);
    return !running_;
  }

 private:
  void Run();

  struct FunctionInfo {
    std::function<void()> fn;
    uint64_t next_run_time_us;

    explicit FunctionInfo(std::function<void()> p_fn, uint64_t p_next_run_time_us)
        : fn(std::move(p_fn)), next_run_time_us(p_next_run_time_us) {}
  };

  struct RunTimeOrder {
    bool operator()(const FunctionInfo& f1, const FunctionInfo& f2) {
      return f1.next_run_time_us > f2.next_run_time_us;
    }
  };

  Actuator* actuator_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<std::thread> thread_;
  std::priority_queue<FunctionInfo, std::vector<FunctionInfo>, RunTimeOrder> heap_;
  bool running_;
};

class ThreadPoolActuator final : public Actuator {
 public:
  ThreadPoolActuator();

  ~ThreadPoolActuator() override;

  bool Start(int thread_num) override;

  bool Stop() override;

  bool Execute(std::function<void()> func) override;

  bool Schedule(std::function<void()> func, int delay_ms) override;

  int ThreadNum() const override {
    return thread_num_;
  }

  std::string Name() const override { return InternalName(); }

  static std::string InternalName() { return "ThreadPoolActuator"; }

 private:
  std::unique_ptr<Timer> timer_;
  std::unique_ptr<ThreadPool> pool_;
  std::atomic<bool> running_;
  int thread_num_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_THREAD_POOL_ACTUATOR_H_
