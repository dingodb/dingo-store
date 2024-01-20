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

#include "crontab/crontab.h"

#include "bthread/bthread.h"
#include "bthread/unstable.h"
#include "common/logging.h"
#include "common/role.h"
#include "fmt/core.h"

namespace dingodb {

CrontabManager::CrontabManager() { bthread_mutex_init(&mutex_, nullptr); }

CrontabManager::~CrontabManager() { bthread_mutex_destroy(&mutex_); }

void CrontabManager::Run(void* arg) {
  Crontab* crontab = static_cast<Crontab*>(arg);
  if (crontab->pause) {
    return;
  }
  if (crontab->immediately) {
    try {
      crontab->func(crontab->arg);
    } catch (...) {
      DINGO_LOG(ERROR) << fmt::format("[crontab.run][id({}).name({})] crontab happen exception", crontab->id,
                                      crontab->name);
    }
    ++crontab->run_count;
  } else {
    crontab->immediately = true;
  }

  if (crontab->max_times == 0 || crontab->run_count < crontab->max_times) {
    bthread_timer_add(&crontab->timer_id, butil::milliseconds_from_now(crontab->interval), &Run, crontab);
  }
}

uint32_t CrontabManager::AllocCrontabId() { return auinc_crontab_id_.fetch_add(1); }

void CrontabManager::AddCrontab(std::vector<CrontabConfig>& crontab_configs) {
  for (auto& crontab_config : crontab_configs) {
    // Check whether should add crontab.
    bool should_add_crontab = false;
    for (auto role : crontab_config.roles) {
      if (role == GetRole()) {
        should_add_crontab = true;
      }
    }
    if (!should_add_crontab) {
      continue;
    }

    DINGO_LOG(INFO) << fmt::format("[crontab.add][name({}).interval({}ms).async({})] add crontab task.",
                                   crontab_config.name, crontab_config.interval, crontab_config.async);

    auto crontab = std::make_shared<Crontab>();
    crontab->name = crontab_config.name;
    crontab->interval = crontab_config.interval;
    if (crontab_config.async) {
      crontab->func = [&](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void* arg) -> void* {
              CrontabConfig* crontab_config = static_cast<CrontabConfig*>(arg);
              crontab_config->funcer(nullptr);
              return nullptr;
            },
            &crontab_config);
      };
    } else {
      crontab->func = crontab_config.funcer;
    }

    crontab->arg = nullptr;

    this->AddAndRunCrontab(crontab);
  }
}

uint32_t CrontabManager::AddAndRunCrontab(std::shared_ptr<Crontab> crontab) {
  uint32_t crontab_id = AddCrontab(crontab);
  StartCrontab(crontab_id);

  return crontab_id;
}

uint32_t CrontabManager::AddCrontab(std::shared_ptr<Crontab> crontab) {
  BAIDU_SCOPED_LOCK(mutex_);

  uint32_t crontab_id = AllocCrontabId();
  crontab->id = crontab_id;

  crontabs_[crontab_id] = crontab;
  return crontab_id;
}

void CrontabManager::StartCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = crontabs_.find(crontab_id);
  if (it == crontabs_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[crontab.start][id({})] not exist crontab.", crontab_id);
    return;
  }
  auto crontab = it->second;
  crontab->pause = false;

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        CrontabManager::Run(arg);
        return nullptr;
      },
      crontab.get());
}

void CrontabManager::InnerPauseCrontab(uint32_t crontab_id) {
  auto it = crontabs_.find(crontab_id);
  if (it == crontabs_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[crontab.pause][id({})] not exist crontab.", crontab_id);
    return;
  }
  auto crontab = it->second;

  crontab->pause = true;
  if (crontab->timer_id != 0) {
    bthread_timer_del(crontab->timer_id);
  }
}

void CrontabManager::PauseCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  InnerPauseCrontab(crontab_id);
}

void CrontabManager::DeleteCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  InnerPauseCrontab(crontab_id);

  crontabs_.erase(crontab_id);
}

void CrontabManager::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  for (auto it = crontabs_.begin(); it != crontabs_.end();) {
    bthread_timer_del(it->second->timer_id);

    it = crontabs_.erase(it);
  }
}

}  // namespace dingodb