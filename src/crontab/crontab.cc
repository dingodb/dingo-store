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
#include "butil/strings/stringprintf.h"

namespace dingodb {

CrontabManager::CrontabManager() { bthread_mutex_init(&mutex_, nullptr); }

CrontabManager::~CrontabManager() { bthread_mutex_destroy(&mutex_); }

void CrontabManager::Run(void* arg) {
  Crontab* crontab = static_cast<Crontab*>(arg);
  if (crontab->pause_) {
    return;
  }
  if (crontab->immediately_) {
    try {
      crontab->func_(crontab->arg_);
    } catch (...) {
      LOG(ERROR) << butil::StringPrintf("Crontab %u %s happen exception", crontab->id_, crontab->name_.c_str());
    }
    ++crontab->run_count_;
  } else {
    crontab->immediately_ = true;
  }

  if (crontab->max_times_ == 0 || crontab->run_count_ < crontab->max_times_) {
    bthread_timer_add(&crontab->timer_id_, butil::milliseconds_from_now(crontab->interval_), &Run, crontab);
  }
}

uint32_t CrontabManager::AllocCrontabId() { return auinc_crontab_id_.fetch_add(1); }

uint32_t CrontabManager::AddAndRunCrontab(std::shared_ptr<Crontab> crontab) {
  uint32_t crontab_id = AddCrontab(crontab);
  StartCrontab(crontab_id);

  return crontab_id;
}

uint32_t CrontabManager::AddCrontab(std::shared_ptr<Crontab> crontab) {
  BAIDU_SCOPED_LOCK(mutex_);

  uint32_t crontab_id = AllocCrontabId();
  crontab->id_ = crontab_id;

  crontabs_[crontab_id] = crontab;
  return crontab_id;
}

void CrontabManager::StartCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = crontabs_.find(crontab_id);
  if (it == crontabs_.end()) {
    LOG(WARNING) << "Not exist crontab " << crontab_id;
    return;
  }
  auto crontab = it->second;
  crontab->pause_ = false;

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
    LOG(WARNING) << "Not exist crontab " << crontab_id;
    return;
  }
  auto crontab = it->second;

  crontab->pause_ = true;
  if (crontab->timer_id_ != 0) {
    bthread_timer_del(crontab->timer_id_);
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
    bthread_timer_del(it->second->timer_id_);

    it = crontabs_.erase(it);
  }
}

}  // namespace dingodb