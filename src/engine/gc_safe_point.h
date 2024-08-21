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

#ifndef DINGODB_GC_SAFE_POINT_H_  // NOLINT
#define DINGODB_GC_SAFE_POINT_H_

#include <sys/stat.h>

#include <cstdint>
#include <map>
#include <memory>
#include <utility>

#include "bthread/types.h"

namespace dingodb {
class GCSafePoint final {
 public:
  GCSafePoint();
  ~GCSafePoint();

  GCSafePoint(const GCSafePoint &rhs) = delete;
  GCSafePoint &operator=(const GCSafePoint &rhs) = delete;
  GCSafePoint(GCSafePoint &&rhs) = delete;
  GCSafePoint &operator=(GCSafePoint &&rhs) = delete;

  void SetGcFlagAndSafePointTs(int64_t tenant_id, bool gc_stop, int64_t safe_point_ts);
  std::pair<bool, int64_t> GetGcFlagAndSafePointTs();

  void SetGcStop(bool gc_stop);
  bool GetGcStop();

  // internal
  void SetLastAccomplishedSafePointTs(int64_t last_accomplished_safe_point_ts);
  int64_t GetLastAccomplishedSafePointTs();

  int64_t GetTenantId();

 private:
  bthread_mutex_t mutex_;
  int64_t tenant_id_;
  volatile int64_t safe_point_ts_;
  volatile bool gc_stop_;

  // internal var
  volatile int64_t last_accomplished_safe_point_ts_;
};

class GCSafePointManager final {
 public:
  GCSafePointManager();
  ~GCSafePointManager();

  GCSafePointManager(const GCSafePointManager &rhs) = delete;
  GCSafePointManager &operator=(const GCSafePointManager &rhs) = delete;
  GCSafePointManager(GCSafePointManager &&rhs) = delete;
  GCSafePointManager &operator=(GCSafePointManager &&rhs) = delete;

  void SetGcFlagAndSafePointTs(std::map<int64_t, int64_t> safe_point_ts_group, bool gc_stop);
  std::map<int64_t, std::pair<bool, int64_t>> GetAllGcFlagAndSafePointTs();

  std::pair<bool, int64_t> GetGcFlagAndSafePointTs(int64_t tenant_id);

  std::shared_ptr<GCSafePoint> FindSafePoint(int64_t tenant_id);
  void RemoveSafePoint(int64_t tenant_id);

  // internal
  void SetGcStop(int64_t tenant_id, bool gc_stop);
  bool GetGcStop(int64_t tenant_id);

  void SetLastAccomplishedSafePointTs(int64_t tenant_id, int64_t last_accomplished_safe_point_ts);
  int64_t GetLastAccomplishedSafePointTs(int64_t tenant_id);

  // debug
  std::map<int64_t, std::pair<bool, int64_t>> GetAllGcFlagAndSafePointTsForDebug();
  void ClearAll();

 private:
  bthread_mutex_t mutex_;
  std::map<int64_t, std::shared_ptr<GCSafePoint>> all_safe_points_;
  std::map<int64_t, std::shared_ptr<GCSafePoint>> active_safe_points_;
};

}  // namespace dingodb

#endif  // DINGODB_GC_SAFE_POINT_HELPER_H_  // NOLINT
