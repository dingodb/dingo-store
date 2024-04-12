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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingodb {

ThreadPool::ThreadPool(const std::string &thread_name, uint32_t pool_size)
    : ThreadPool(thread_name, pool_size, nullptr) {}

ThreadPool::ThreadPool(const std::string &thread_name, uint32_t pool_size, std::function<void(void)> init_thread)
    : thread_name_(thread_name),
      init_thread_func_(init_thread),
      total_task_count_metrics_(fmt::format("dingo_threadpool_{}_total_task_count", thread_name)),
      pending_task_count_metrics_(fmt::format("dingo_threadpool_{}_pending_task_count", thread_name)) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    for (int i = 0; i < pool_size; ++i) {
      workers_.push_back(BootstrapThread(i));
    }
  }
}

ThreadPool::~ThreadPool() { Destroy(); }

void ThreadPool::AdjustPoolSize(uint32_t pool_size) {
  std::unique_lock<std::mutex> lock(mutex_);

  uint32_t curr_pool_size = workers_.size();
  if (pool_size < curr_pool_size) {
    ShrinkThreadPool(pool_size);
  } else if (pool_size > curr_pool_size) {
    ExpandTheadPool(pool_size);
  }
}

bool ThreadPool::BindCore(std::vector<uint32_t> threads, std::vector<uint32_t> cores) {
  std::unique_lock<std::mutex> lock(mutex_);

  for (int i = 0; i < threads.size(); ++i) {
    uint32_t offset = threads[i];
    if (offset >= workers_.size()) {
      continue;
    }

    auto thread_entry = workers_[offset];

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cores[i], &cpuset);
    int ret = pthread_setaffinity_np(thread_entry->thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("bind cpu core failed, error: {}", ret);
      return false;
    }
  }

  return true;
}

bool ThreadPool::UnbindCore() {
  std::unique_lock<std::mutex> lock(mutex_);

  int32_t cores = Helper::GetCores();
  cpu_set_t cpuset;
  for (int i = 0; i < cores; ++i) {
    CPU_SET(i, &cpuset);
  }

  for (auto &thread_entry : workers_) {
    int ret = pthread_setaffinity_np(thread_entry->thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("unbind cpu core failed, error: {}", ret);
      return false;
    }
  }

  return true;
}

std::vector<std::pair<std::string, uint32_t>> ThreadPool::GetAffinity() {
  std::vector<std::pair<std::string, uint32_t>> pairs;

  int32_t cores = Helper::GetCores();
  for (auto &thread_entry : workers_) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    pthread_getaffinity_np(thread_entry->thread.native_handle(), sizeof(cpu_set_t), &cpuset);

    if (CPU_COUNT(&cpuset) == 1) {
      for (int i = 0; i < cores; ++i) {
        if (CPU_ISSET(i, &cpuset)) {
          DINGO_LOG(INFO) << fmt::format("thread({}) bind core({})", thread_entry->name, i);
          pairs.push_back(std::make_pair(thread_entry->name, i));
        }
      }
    }
  }

  return pairs;
}

ThreadPool::ThreadEntryPtr ThreadPool::BootstrapThread(int thread_no) {
  auto thread_entry = std::make_shared<ThreadEntry>();
  thread_entry->is_stop = false;

  std::thread th([this, thread_no, thread_entry] {
    thread_entry->name = fmt::format("{}:{}", thread_name_, thread_no);
    pthread_setname_np(pthread_self(), thread_entry->name.c_str());

    if (init_thread_func_ != nullptr) {
      init_thread_func_();
    }

    for (;;) {
      TaskPtr task;

      {
        std::unique_lock<std::mutex> lock(this->task_mutex_);

        this->task_condition_.wait(lock,
                                   [this, thread_entry] { return thread_entry->is_stop || !this->tasks_.empty(); });

        if (thread_entry->is_stop && this->tasks_.empty()) {
          return;
        } else if (this->tasks_.empty()) {
          continue;
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

  thread_entry->thread.swap(th);

  return thread_entry;
}

void ThreadPool::ShrinkThreadPool(uint32_t pool_size) {
  CHECK(pool_size < workers_.size()) << fmt::format("invalid pool_size({}) param, current pool size({}) ", pool_size,
                                                    workers_.size());

  std::vector<ThreadEntryPtr> shrink_workers;
  for (int i = pool_size; i < workers_.size(); ++i) {
    shrink_workers.push_back(workers_[i]);
  }

  workers_.resize(pool_size);

  for (auto &worker : shrink_workers) {
    worker->is_stop = true;
  }

  task_condition_.notify_all();

  for (auto &worker : shrink_workers) {
    if (worker->thread.joinable()) {
      worker->thread.join();
    }
  }
}

void ThreadPool::ExpandTheadPool(uint32_t pool_size) {
  CHECK(pool_size > workers_.size()) << fmt::format("invalid pool_size({}) param, current pool size({}) ", pool_size,
                                                    workers_.size());

  for (int i = workers_.size(); i < pool_size; ++i) {
    workers_.push_back(BootstrapThread(i));
  }
}

ThreadPool::TaskPtr ThreadPool::ExecuteTask(Funcer func, void *arg, int priority) {
  if (is_destroied_.load(std::memory_order_relaxed)) {
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

bool ThreadPool::IsDestroied() {
  bool expect = false;
  return !is_destroied_.compare_exchange_strong(expect, true);
}

void ThreadPool::Destroy() {
  if (IsDestroied()) {
    return;
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);

    for (auto &worker : workers_) {
      worker->is_stop = true;
    }

    task_condition_.notify_all();
    for (auto &worker : workers_) {
      if (worker->thread.joinable()) {
        worker->thread.join();
      }
    }
  }
}

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