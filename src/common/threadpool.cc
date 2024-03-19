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

#include "common/threadpool.h"

#include <exception>
#include <string>

#include "fmt/core.h"

namespace dingodb {

ThreadPool::ThreadPool(const std::string &thread_name, uint32_t thread_num)
    : ThreadPool(thread_name, thread_num, nullptr) {}

ThreadPool::ThreadPool(const std::string &thread_name, uint32_t thread_num, std::function<void(void)> init_thread)
    : thread_name_(thread_name),
      stop_(false),
      total_task_count_metrics_(fmt::format("dingo_threadpool_{}_total_task_count", thread_name)),
      pending_task_count_metrics_(fmt::format("dingo_threadpool_{}_pending_task_count", thread_name)) {
  for (size_t i = 0; i < thread_num; ++i)
    workers_.emplace_back([this, i, init_thread] {
      pthread_setname_np(pthread_self(), (thread_name_ + ":" + std::to_string(i)).c_str());
      if (init_thread != nullptr) {
        init_thread();
      }

      for (;;) {
        TaskPtr task;

        {
          std::unique_lock<std::mutex> lock(this->task_mutex_);

          this->task_condition_.wait(lock, [this] { return this->stop_ || !this->tasks_.empty(); });

          if (this->stop_ && this->tasks_.empty()) {
            return;
          }

          task = this->tasks_.top();
          this->tasks_.pop();
        }

        try {
          task->func(task->arg);
        } catch (...) {
          std::exception_ptr ex = std::current_exception();

          try {
            if (ex) {
              std::rethrow_exception(ex);
            }
          } catch (const std::exception &e) {
            LOG(ERROR) << fmt::format("{} exception: {}", this->thread_name_, e.what());
          }
        }
        task->WakeUp();

        this->DecPendingTaskCount();
      }
    });
}

ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(task_mutex_);
    stop_ = true;
  }

  task_condition_.notify_all();
  for (std::thread &worker : workers_) {
    worker.join();
  }
}

ThreadPool::TaskPtr ThreadPool::ExecuteTask(Funcer func, void *arg, int priority) {
  if (stop_) {
    return nullptr;
  }

  auto task = std::make_shared<Task>();
  task->priority = priority;
  task->func = func;
  task->arg = arg;
  task->cond = std::make_shared<BthreadCond>();

  {
    std::unique_lock<std::mutex> lock(task_mutex_);

    tasks_.push(task);
  }

  IncTotalTaskCount();
  IncPendingTaskCount();

  task_condition_.notify_one();

  return task;
}

uint64_t ThreadPool::TotalTaskCount() { return total_task_count_metrics_.get_value(); }

void ThreadPool::IncTotalTaskCount() { total_task_count_metrics_ << 1; }

int64_t ThreadPool::PendingTaskCount() { return pending_task_count_metrics_.get_value(); }

void ThreadPool::IncPendingTaskCount() { pending_task_count_metrics_ << 1; }

void ThreadPool::DecPendingTaskCount() { pending_task_count_metrics_ << -1; }

void ThreadPool::Task::Join() {
  if (cond != nullptr) {
    cond->IncreaseWait();
  }
}

void ThreadPool::Task::WakeUp() {
  if (cond != nullptr) {
    cond->DecreaseSignal();
  }
}

}  // namespace dingodb