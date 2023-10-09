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

#ifndef DINGODB_CRONTAB_CRONTAB_H_
#define DINGODB_CRONTAB_CRONTAB_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "bthread/unstable.h"
#include "proto/common.pb.h"

namespace dingodb {

struct CrontabConfig {
  std::string name;
  std::vector<pb::common::ClusterRole> roles;
  int32_t interval;
  bool async;
  std::function<void(void*)> funcer;
};

class Crontab {
 public:
  Crontab()
      : id(0),
        interval(0),
        max_times(0),
        immediately(false),
        run_count(0),
        pause(false),
        timer_id(0),
        func(nullptr),
        arg(nullptr) {}

  uint32_t id;
  std::string name;
  // unit ms
  int64_t interval;
  // 0 is no limit
  uint32_t max_times;
  // Is immediately run
  bool immediately;
  // Already run count
  int run_count;
  // Is pause crontab
  bool pause;
  // bthread_timer_t handler
  bthread_timer_t timer_id;
  // For run target function
  std::function<void(void*)> func;
  // Delivery to func_'s argument
  void* arg;
};

// Manage crontab use brpc::bthread_timer_add
class CrontabManager {
 public:
  CrontabManager();
  ~CrontabManager();

  CrontabManager(const CrontabManager&) = delete;
  const CrontabManager& operator=(const CrontabManager&) = delete;

  static void Run(void* arg);

  void AddCrontab(std::vector<CrontabConfig>& crontab_configs);

  uint32_t AddCrontab(std::shared_ptr<Crontab> crontab);
  uint32_t AddAndRunCrontab(std::shared_ptr<Crontab> crontab);
  void StartCrontab(uint32_t crontab_id);
  void PauseCrontab(uint32_t crontab_id);
  void DeleteCrontab(uint32_t crontab_id);

  void Destroy();

 private:
  // Allocate crontab id by auto incremental.
  uint32_t AllocCrontabId();

  void InnerPauseCrontab(uint32_t crontab_id);

  // Atomic auto incremental variable
  std::atomic<uint32_t> auinc_crontab_id_;
  // Protect crontabs_ concurrence access.
  bthread_mutex_t mutex_;
  // Store all crontab, key(crontab_id) / value(Crontab)
  std::map<uint32_t, std::shared_ptr<Crontab> > crontabs_;
};

}  // namespace dingodb

#endif  // DINGODB_CRONTAB_CRONTAB_H_