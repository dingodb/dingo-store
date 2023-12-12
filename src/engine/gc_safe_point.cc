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

GCSafePoint::GCSafePoint()
    : gc_stop_(true), safe_point_ts_(0), force_gc_stop_(true), last_accomplished_safe_point_ts_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}
GCSafePoint::~GCSafePoint() {
  gc_stop_ = true;
  safe_point_ts_ = 0;
  force_gc_stop_ = true;
  last_accomplished_safe_point_ts_ = 0;
  bthread_mutex_destroy(&mutex_);
}

void GCSafePoint::SetGcFlagAndSafePointTs(bool gc_stop, int64_t safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  gc_stop_ = gc_stop;
  safe_point_ts_ = safe_point_ts;
}

std::pair<bool, int64_t> GCSafePoint::GetGcFlagAndSafePointTs() {
  BAIDU_SCOPED_LOCK(mutex_);
  return {gc_stop_, safe_point_ts_};
}

void GCSafePoint::SetForceGcStop(bool force_gc_stop) {
  BAIDU_SCOPED_LOCK(mutex_);
  force_gc_stop_ = force_gc_stop;
}

bool GCSafePoint::GetForceGcStop() {
  BAIDU_SCOPED_LOCK(mutex_);
  return force_gc_stop_;
}

void GCSafePoint::SetLastAccomplishedSafePointTs(int64_t last_accomplished_safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  last_accomplished_safe_point_ts_ = last_accomplished_safe_point_ts;
}

int64_t GCSafePoint::GetLastAccomplishedSafePointTs() {
  BAIDU_SCOPED_LOCK(mutex_);
  return last_accomplished_safe_point_ts_;
}

}  // namespace dingodb
