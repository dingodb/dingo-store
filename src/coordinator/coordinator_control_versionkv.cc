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

#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "braft/closure_helper.h"
#include "braft/configuration.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "gflags/gflags.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/version.pb.h"

namespace dingodb {

DEFINE_uint64(version_lease_max_ttl_seconds, 300, "max ttl seconds for version lease");
DEFINE_uint64(version_lease_min_ttl_seconds, 5, "min ttl seconds for version lease");

butil::Status CoordinatorControl::LeaseGrant(uint64_t lease_id, uint64_t ttl_seconds, uint64_t &granted_id,
                                             uint64_t &granted_ttl_seconds,
                                             pb::coordinator_internal::MetaIncrement &meta_increment) {
  if (ttl_seconds > FLAGS_version_lease_max_ttl_seconds) {
    DINGO_LOG(WARNING) << "lease_id " << lease_id << ", lease ttl seconds " << ttl_seconds
                       << " is too large, max ttl seconds is " << FLAGS_version_lease_max_ttl_seconds;
    granted_ttl_seconds = FLAGS_version_lease_max_ttl_seconds;
  } else if (ttl_seconds < FLAGS_version_lease_min_ttl_seconds) {
    DINGO_LOG(WARNING) << "lease_id " << lease_id << ", lease ttl seconds " << ttl_seconds
                       << " is too small, min ttl seconds is " << FLAGS_version_lease_min_ttl_seconds;
    granted_ttl_seconds = FLAGS_version_lease_min_ttl_seconds;
  } else {
    granted_ttl_seconds = ttl_seconds;
  }

  if (lease_id == 0) {
    granted_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE, meta_increment);
  } else {
    granted_id = lease_id;
  }

  if (this->version_lease_map_.Exists(granted_id)) {
    DINGO_LOG(WARNING) << "lease id " << granted_id << " already exists";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "lease id %lu already exists", granted_id);
  }

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(granted_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->set_id(granted_id);
  increment_lease->set_ttl_seconds(granted_ttl_seconds);
  increment_lease->set_create_ts_seconds(butil::gettimeofday_s());
  increment_lease->set_last_renew_ts_seconds(butil::gettimeofday_s());
  increment_lease->set_ttl_seconds(granted_ttl_seconds);

  // update version_lease_to_key_map_temp_
  {
    BAIDU_SCOPED_LOCK(this->version_lease_to_key_map_temp_mutex_);
    this->version_lease_to_key_map_temp_.emplace(granted_id, LeaseWithKeys());
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::LeaseRenew(uint64_t lease_id, uint64_t &ttl_seconds,
                                             pb::coordinator_internal::MetaIncrement &meta_increment) {
  pb::coordinator_internal::LeaseInternal lease;
  auto ret = this->version_lease_map_.Get(lease_id, lease);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "lease id %lu not found", lease_id);
  }

  ttl_seconds = lease.ttl_seconds();
  lease.set_last_renew_ts_seconds(butil::gettimeofday_s());

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(lease_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->Swap(&lease);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::LeaseRevoke(uint64_t lease_id,
                                              pb::coordinator_internal::MetaIncrement &meta_increment) {
  pb::coordinator_internal::LeaseInternal lease;
  auto ret = this->version_lease_map_.Get(lease_id, lease);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "lease id %lu not found", lease_id);
  }

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(lease_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->Swap(&lease);

  // TODO: do version_kv delete simultaneously
  // update version_lease_to_key_map_temp_
  {
    BAIDU_SCOPED_LOCK(this->version_lease_to_key_map_temp_mutex_);
    this->version_lease_to_key_map_temp_.erase(lease_id);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ListLeases(std::vector<pb::coordinator_internal::LeaseInternal> &leases) {
  butil::FlatMap<uint64_t, pb::coordinator_internal::LeaseInternal> version_lease_map;
  version_lease_map_.CopyFlatMap(version_lease_map);
  for (auto &it : version_lease_map) {
    leases.emplace_back(std::move(it.second));
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::LeaseTimeToLive(uint64_t lease_id, bool get_keys, uint64_t &granted_ttl_seconds,
                                                  uint64_t &remaining_ttl_seconds, std::set<std::string> &keys) {
  pb::coordinator_internal::LeaseInternal lease;
  auto ret = this->version_lease_map_.Get(lease_id, lease);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "lease id %lu not found", lease_id);
  }

  granted_ttl_seconds = lease.ttl_seconds();
  remaining_ttl_seconds = lease.ttl_seconds() - (butil::gettimeofday_s() - lease.last_renew_ts_seconds());

  if (get_keys) {
    BAIDU_SCOPED_LOCK(this->version_lease_to_key_map_temp_mutex_);
    auto it = this->version_lease_to_key_map_temp_.find(lease_id);
    if (it != this->version_lease_to_key_map_temp_.end()) {
      keys = it->second.keys;
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
