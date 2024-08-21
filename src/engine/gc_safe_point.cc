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

#include "engine/gc_safe_point.h"

#include <cstdint>

#include "bthread/mutex.h"

namespace dingodb {

GCSafePoint::GCSafePoint() : gc_stop_(true), safe_point_ts_(0), last_accomplished_safe_point_ts_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}
GCSafePoint::~GCSafePoint() {
  gc_stop_ = true;
  safe_point_ts_ = 0;
  last_accomplished_safe_point_ts_ = 0;
  bthread_mutex_destroy(&mutex_);
}

void GCSafePoint::SetGcFlagAndSafePointTs(int64_t tenant_id, bool gc_stop, int64_t safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  tenant_id_ = tenant_id;
  gc_stop_ = gc_stop;
  safe_point_ts_ = safe_point_ts;
}

std::pair<bool, int64_t> GCSafePoint::GetGcFlagAndSafePointTs() {
  BAIDU_SCOPED_LOCK(mutex_);
  return {gc_stop_, safe_point_ts_};
}

void GCSafePoint::SetGcStop(bool gc_stop) {
  BAIDU_SCOPED_LOCK(mutex_);
  gc_stop_ = gc_stop;
}

bool GCSafePoint::GetGcStop() {
  BAIDU_SCOPED_LOCK(mutex_);
  return gc_stop_;
}

void GCSafePoint::SetLastAccomplishedSafePointTs(int64_t last_accomplished_safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  last_accomplished_safe_point_ts_ = last_accomplished_safe_point_ts;
}

int64_t GCSafePoint::GetLastAccomplishedSafePointTs() {
  BAIDU_SCOPED_LOCK(mutex_);
  return last_accomplished_safe_point_ts_;
}

int64_t GCSafePoint::GetTenantId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return tenant_id_;
}

/*****************  GCSafePointManager*************************************************/
GCSafePointManager::GCSafePointManager() { bthread_mutex_init(&mutex_, nullptr); }
GCSafePointManager::~GCSafePointManager() { bthread_mutex_destroy(&mutex_); }

void GCSafePointManager::SetGcFlagAndSafePointTs(std::map<int64_t, int64_t> safe_point_ts_group, bool gc_stop) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (auto& [tenant_id, safe_point_ts] : safe_point_ts_group) {
    auto iter = active_safe_points_.find(tenant_id);
    if (iter == active_safe_points_.end()) {
      iter = all_safe_points_.find(tenant_id);
      if (iter == all_safe_points_.end()) {
        auto safe_point = std::make_shared<GCSafePoint>();
        safe_point->SetGcFlagAndSafePointTs(tenant_id, gc_stop, safe_point_ts);
        if (gc_stop) {
          all_safe_points_.insert({tenant_id, safe_point});
        } else {
          active_safe_points_.insert({tenant_id, safe_point});
        }
      } else {
        iter->second->SetGcFlagAndSafePointTs(tenant_id, gc_stop, safe_point_ts);
      }
    } else {
      iter->second->SetGcFlagAndSafePointTs(tenant_id, gc_stop, safe_point_ts);
    }
  }

  for (auto iter = all_safe_points_.begin(); iter != all_safe_points_.end();) {
    if (!iter->second->GetGcStop()) {
      active_safe_points_.insert({iter->first, iter->second});
      iter = all_safe_points_.erase(iter);
    } else {
      ++iter;
    }
  }

  for (auto iter = active_safe_points_.begin(); iter != active_safe_points_.end();) {
    if (iter->second->GetGcStop()) {
      all_safe_points_.insert({iter->first, iter->second});
      iter = active_safe_points_.erase(iter);
    } else {
      ++iter;
    }
  }
}

std::map<int64_t, std::pair<bool, int64_t>> GCSafePointManager::GetAllGcFlagAndSafePointTs() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::map<int64_t, std::pair<bool, int64_t>> result;
  for (auto& [tenant_id, safe_point] : active_safe_points_) {
    result.emplace(tenant_id, safe_point->GetGcFlagAndSafePointTs());
  }
  return result;
}

std::pair<bool, int64_t> GCSafePointManager::GetGcFlagAndSafePointTs(int64_t tenant_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return {true, 0};
    }
    return iter->second->GetGcFlagAndSafePointTs();
  }
  return iter->second->GetGcFlagAndSafePointTs();
}

std::shared_ptr<GCSafePoint> GCSafePointManager::FindSafePoint(int64_t tenant_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return nullptr;
    }
    return iter->second;
  }
  return iter->second;
}

void GCSafePointManager::RemoveSafePoint(int64_t tenant_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter != active_safe_points_.end()) {
    all_safe_points_.insert(std::make_pair(tenant_id, iter->second));
    active_safe_points_.erase(iter);
  }
}

// internal
void GCSafePointManager::SetGcStop(int64_t tenant_id, bool gc_stop) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return;
    }
    iter->second->SetGcStop(gc_stop);
    return;
  }
  iter->second->SetGcStop(gc_stop);
}

bool GCSafePointManager::GetGcStop(int64_t tenant_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return true;
    }
    return iter->second->GetGcStop();
  }
  return iter->second->GetGcStop();
}

void GCSafePointManager::SetLastAccomplishedSafePointTs(int64_t tenant_id, int64_t last_accomplished_safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return;
    }
    iter->second->SetLastAccomplishedSafePointTs(last_accomplished_safe_point_ts);
    return;
  }
  iter->second->SetLastAccomplishedSafePointTs(last_accomplished_safe_point_ts);
}

int64_t GCSafePointManager::GetLastAccomplishedSafePointTs(int64_t tenant_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = active_safe_points_.find(tenant_id);
  if (iter == active_safe_points_.end()) {
    iter = all_safe_points_.find(tenant_id);
    if (iter == all_safe_points_.end()) {
      return 0;
    }
    return iter->second->GetLastAccomplishedSafePointTs();
  }
  return iter->second->GetLastAccomplishedSafePointTs();
}

std::map<int64_t, std::pair<bool, int64_t>> GCSafePointManager::GetAllGcFlagAndSafePointTsForDebug() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::map<int64_t, std::pair<bool, int64_t>> result;
  for (auto& [tenant_id, safe_point] : active_safe_points_) {
    result.emplace(tenant_id, safe_point->GetGcFlagAndSafePointTs());
  }

  for (auto& [tenant_id, safe_point] : all_safe_points_) {
    result.emplace(tenant_id, safe_point->GetGcFlagAndSafePointTs());
  }

  return result;
}

void GCSafePointManager::ClearAll() {
  BAIDU_SCOPED_LOCK(mutex_);
  active_safe_points_.clear();
  all_safe_points_.clear();
}

}  // namespace dingodb
