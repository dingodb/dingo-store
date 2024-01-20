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

#include "sdk/utils/thread_pool_actuator.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

#include "glog/logging.h"
#include "sdk/utils/actuator.h"

namespace dingodb {
namespace sdk {
using namespace std::chrono;

Timer::Timer() : thread_(nullptr), running_(false){};

Timer::~Timer() { Stop(); }

bool Timer::Start(Actuator* actuator) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (running_) {
    return false;
  }

  actuator_ = actuator;
  thread_ = std::make_unique<std::thread>(&Timer::Run, this);
  running_ = true;

  return true;
}

bool Timer::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!running_) {
      return false;
    }

    running_ = false;
    while (!heap_.empty()) {
      // TODO: add debug log
      heap_.pop();
    }

    cv_.notify_all();
  }

  if (thread_) {
    thread_->join();
  }

  return true;
}

bool Timer::Add(std::function<void()> func, int delay_ms) {
  CHECK(running_);
  auto now = steady_clock::now().time_since_epoch();
  uint64_t next = duration_cast<microseconds>(now + milliseconds(delay_ms)).count();

  FunctionInfo fn_info(std::move(func), next);
  std::lock_guard<std::mutex> lk(mutex_);
  heap_.push(std::move(fn_info));
  cv_.notify_all();
  return true;
}

void Timer::Run() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (running_) {
    if (heap_.empty()) {
      cv_.wait(lk);
      continue;
    }

    const auto& cur_fn = heap_.top();
    uint64_t now = duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
    if (cur_fn.next_run_time_us <= now) {
      std::function<void()> fn = cur_fn.fn;
      CHECK(actuator_->Execute(std::move(fn)));
      heap_.pop();
    } else {
      cv_.wait_for(lk, microseconds(cur_fn.next_run_time_us - now));
    }
  }
}

ThreadPoolActuator::ThreadPoolActuator() : timer_(nullptr), pool_(nullptr), running_(false), thread_num_(0) {}

ThreadPoolActuator::~ThreadPoolActuator() {
  Stop();
  timer_.reset();
  pool_.reset();
}

bool ThreadPoolActuator::Start(int thread_num) {
  pool_ = std::make_unique<ThreadPool>(InternalName(), thread_num);
  timer_ = std::make_unique<Timer>();
  CHECK(timer_->Start(this));
  thread_num_ = thread_num;
  running_.store(true);
  return true;
}

bool ThreadPoolActuator::Stop() {
  if (running_.load()) {
    CHECK(timer_->Stop());
    running_ = false;
    return true;
  } else {
    return false;
  }
}

bool ThreadPoolActuator::Execute(std::function<void()> func) {
  CHECK(running_);
  pool_->ExecuteTask([=](void*) { func(); }, nullptr);
  return true;
}

bool ThreadPoolActuator::Schedule(std::function<void()> func, int delay_ms) {
  CHECK(running_);
  timer_->Add(std::move(func), delay_ms);
  return true;
}

}  // namespace sdk
}  // namespace dingodb