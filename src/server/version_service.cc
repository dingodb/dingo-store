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

#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <vector>
#include <cstdio>
#include <atomic>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "server/version_service.h"

namespace dingodb {

int VersionService::AddListenableVersion(VersionType type, uint64_t id, uint64_t version) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
      return -1;
  }
  VersionListener * version_listener = new VersionListener;
  version_listener->version = version;
  version_listener->ref_count = 0;
  version_listeners_[type].insert({id, version_listener});
  return 0;
}

int VersionService::DelListenableVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    version_listeners_[type].erase(id);
    VersionListener * version_listener = exist->second;
    version_listener->version.store(0);
    while (version_listener->ref_count != 0) {
      lock.unlock();
      version_listener->condition.notify_all();
      bthread_usleep(1000000);
      lock.lock();
    }
    delete version_listener;
    return 0;
  }
  return -1;
}

uint64_t VersionService::GetCurrentVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    return exist->second->version;
  }
  return 0;
}


uint64_t VersionService::GetNewVersion(VersionType type, uint64_t id, uint64_t curr_version, uint wait_seconds) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener * version_listener = exist->second;
    if (version_listener->version != curr_version) {
      return version_listener->version;
    }
    version_listener->ref_count++;
    lock.unlock();
    std::unique_lock<bthread::Mutex> lock(version_listener->mutex);
    version_listener->condition.wait_for(lock, std::chrono::microseconds(std::chrono::seconds(wait_seconds)).count());
    uint64_t version = version_listener->version;
    version_listener->ref_count--;
    return version;
  }
  return 0;
}

int VersionService::UpdateVersion(VersionType type, uint64_t id, uint64_t version) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    version_listener->version = version;
    version_listener->condition.notify_all();
    return 0;
  }
  return -1;
}  
 

uint64_t VersionService::IncVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    uint64_t new_version = version_listener->version++;
    version_listener->condition.notify_all();
    return new_version;
  }
  return 0;
}

VersionService& VersionService::GetInstance() {
  static VersionService service;
  return service;
}

}
