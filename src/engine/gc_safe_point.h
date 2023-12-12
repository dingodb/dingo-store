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

  void SetGcFlagAndSafePointTs(bool gc_stop, int64_t safe_point_ts);
  std::pair<bool, int64_t> GetGcFlagAndSafePointTs();

  // internal
  void SetForceGcStop(bool force_gc_stop);
  bool GetForceGcStop();

  void SetLastAccomplishedSafePointTs(int64_t last_accomplished_safe_point_ts);
  int64_t GetLastAccomplishedSafePointTs();

 private:
  bthread_mutex_t mutex_;
  volatile int64_t safe_point_ts_;
  volatile bool gc_stop_;

  // internal var
  volatile bool force_gc_stop_;
  volatile int64_t last_accomplished_safe_point_ts_;
};

}  // namespace dingodb

#endif  // DINGODB_GC_SAFE_POINT_HELPER_H_  // NOLINT
