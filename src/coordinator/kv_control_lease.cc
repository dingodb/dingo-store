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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "butil/atomicops.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "gflags/gflags.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/version.pb.h"

namespace dingodb {

DEFINE_int64(version_lease_max_ttl_seconds, 300, "max ttl seconds for version lease");
DEFINE_int64(version_lease_min_ttl_seconds, 3, "min ttl seconds for version lease");
DEFINE_int64(version_lease_max_count, 50000, "max lease count");
DEFINE_int64(version_lease_print_ttl_remaining_seconds, 10, "print ttl remaining seconds if value is less than this");

DEFINE_bool(dingo_log_switch_coor_lease, false, "switch for dingo log of kv control lease");

butil::Status KvControl::LeaseGrant(int64_t lease_id, int64_t ttl_seconds, int64_t &granted_id,
                                    int64_t &granted_ttl_seconds,
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

  if (kv_lease_map_.Size() > FLAGS_version_lease_max_count) {
    DINGO_LOG(ERROR) << "lease_id " << lease_id << ", lease count " << kv_lease_map_.Size()
                     << " is too large, max lease count is " << FLAGS_version_lease_max_count;
    return butil::Status(pb::error::Errno::ELEASE_COUNT_EXCEEDS_LIMIT, "lease count %lu is too large",
                         kv_lease_map_.Size());
  }

  if (!IsLeaseToKeyMapInited()) {
    DINGO_LOG(WARNING) << "lease_to_key_map_ not initialized, please wait and try again, term: "
                       << lease_to_key_map_init_term_.load(butil::memory_order_acquire)
                       << ", leader_term: " << leader_term_.load(butil::memory_order_acquire);
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER,
                         "lease_to_key_map_ not initialized, please wait and try again");
  }

  if (lease_id == 0) {
    granted_id = GetNextId(pb::coordinator::IdEpochType::ID_NEXT_LEASE, meta_increment);
  } else {
    granted_id = lease_id;
  }

  if (kv_lease_map_.Exists(granted_id)) {
    DINGO_LOG(WARNING) << "lease id " << granted_id << " already exists";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu already exists", granted_id);
  }

  DINGO_LOG(INFO) << "will grant lease id " << granted_id << ", ttl seconds " << granted_ttl_seconds;

  pb::coordinator_internal::LeaseInternal lease;
  lease.set_id(granted_id);
  lease.set_ttl_seconds(granted_ttl_seconds);
  lease.set_create_ts_seconds(butil::gettimeofday_s());
  lease.set_last_renew_ts_seconds(butil::gettimeofday_s());
  lease.set_ttl_seconds(granted_ttl_seconds);

  KvLeaseWithKeys lease_with_keys;
  lease_with_keys.lease = lease;

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(granted_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->Swap(&lease);

  // update lease_to_key_map_
  {
    BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);
    lease_to_key_map_.emplace(lease_with_keys.lease.id(), lease_with_keys);
  }

  return butil::Status::OK();
}

butil::Status KvControl::LeaseRenew(int64_t lease_id, int64_t &ttl_seconds,
                                    pb::coordinator_internal::MetaIncrement &meta_increment) {
  if (!IsLeaseToKeyMapInited()) {
    DINGO_LOG(WARNING) << "lease_to_key_map_ not initialized, please wait and try again, term: "
                       << lease_to_key_map_init_term_.load(butil::memory_order_acquire)
                       << ", leader_term: " << leader_term_.load(butil::memory_order_acquire);
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER,
                         "lease_to_key_map_ not initialized, please wait and try again");
  }

  pb::coordinator_internal::LeaseInternal lease;

  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  auto now_time_seconds = butil::gettimeofday_s();

  auto iter = lease_to_key_map_.find(lease_id);
  if (iter != lease_to_key_map_.end()) {
    iter->second.lease.set_last_renew_ts_seconds(now_time_seconds);
  } else {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found, cannot renew";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  auto ret = kv_lease_map_.Get(lease_id, lease);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  ttl_seconds = lease.ttl_seconds();
  lease.set_last_renew_ts_seconds(now_time_seconds);

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(lease_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->Swap(&lease);

  return butil::Status::OK();
}

butil::Status KvControl::LeaseRevoke(int64_t lease_id, pb::coordinator_internal::MetaIncrement &meta_increment,
                                     bool has_mutex_locked) {
  pb::coordinator_internal::LeaseInternal lease;
  if (!has_mutex_locked) {
    bthread_mutex_lock(&lease_to_key_map_mutex_);
  }

  auto iter = lease_to_key_map_.find(lease_id);
  if (iter == lease_to_key_map_.end()) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found, cannot revoke";
    if (!has_mutex_locked) {
      bthread_mutex_unlock(&lease_to_key_map_mutex_);
    }
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  auto ret = kv_lease_map_.Get(lease_id, lease);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found from lease_map_";
    if (!has_mutex_locked) {
      bthread_mutex_unlock(&lease_to_key_map_mutex_);
    }
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  // generate meta_increment
  auto *lease_increment = meta_increment.add_leases();
  lease_increment->set_id(lease_id);
  lease_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  auto *increment_lease = lease_increment->mutable_lease();
  increment_lease->Swap(&lease);

  // do version_kv delete simultaneously
  KvLeaseWithKeys lease_with_keys;
  auto keys_iter = lease_to_key_map_.find(lease_id);
  if (keys_iter == lease_to_key_map_.end()) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found from lease_to_key_map_";
  } else {
    lease_with_keys = std::move(keys_iter->second);

    for (const auto &key : lease_with_keys.keys) {
      std::vector<pb::version::Kv> prev_kvs;
      int64_t deleted_count = 0;
      auto ret_status = KvDeleteRange(key, std::string(), false, false, deleted_count, prev_kvs, meta_increment);
      if (!ret_status.ok()) {
        DINGO_LOG(ERROR) << "DeleteRawKv failed, key: " << key;
      }
    }

    // delete lease from map
    lease_to_key_map_.erase(lease_id);
  }

  if (!has_mutex_locked) {
    bthread_mutex_unlock(&lease_to_key_map_mutex_);
  }

  return butil::Status::OK();
}

butil::Status KvControl::ListLeases(std::vector<pb::coordinator_internal::LeaseInternal> &leases) {
  butil::FlatMap<int64_t, pb::coordinator_internal::LeaseInternal> version_lease_map;
  kv_lease_map_.GetRawMapCopy(version_lease_map);
  for (auto &it : version_lease_map) {
    leases.emplace_back(std::move(it.second));
  }

  return butil::Status::OK();
}

butil::Status KvControl::LeaseQuery(int64_t lease_id, bool get_keys, int64_t &granted_ttl_seconds,
                                    int64_t &remaining_ttl_seconds, std::set<std::string> &keys) {
  if (!IsLeaseToKeyMapInited()) {
    DINGO_LOG(WARNING) << "lease_to_key_map_ not initialized, please wait and try again, term: "
                       << lease_to_key_map_init_term_.load(butil::memory_order_acquire)
                       << ", leader_term: " << leader_term_.load(butil::memory_order_acquire);
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER,
                         "lease_to_key_map_ not initialized, please wait and try again");
  }

  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  auto it = lease_to_key_map_.find(lease_id);
  if (it == lease_to_key_map_.end()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_lease) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  granted_ttl_seconds = it->second.lease.ttl_seconds();
  remaining_ttl_seconds =
      it->second.lease.ttl_seconds() - (butil::gettimeofday_s() - it->second.lease.last_renew_ts_seconds());
  if (remaining_ttl_seconds <= 0) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " expired";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu expired", lease_id);
  }

  if (get_keys) {
    keys = it->second.keys;
  }

  return butil::Status::OK();
}

bool KvControl::IsLeaseToKeyMapInited() {
  return lease_to_key_map_init_term_.load(butil::memory_order_acquire) ==
         leader_term_.load(butil::memory_order_acquire);
}

void KvControl::LeaseTask() {
  auto lease_task_start_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "lease task start, start_time: " << lease_task_start_time_ms;

  if (IsLeader()) {
    // for leader
    // 1. check and build lease_to_key_map_
    // check if lease_to_key_map_ is initialized
    if (!IsLeaseToKeyMapInited()) {
      DINGO_LOG(INFO) << "lease_to_key_map_ not initialized, and this is leader, do BuildLeaseToKeyMap, leader_term: "
                      << leader_term_.load(butil::memory_order_acquire) << ", lease_to_key_map_init_term_: "
                      << lease_to_key_map_init_term_.load(butil::memory_order_relaxed);

      BuildLeaseToKeyMap();

      // print the time after BuildLeaseToKeyMap
      auto lease_task_end_time_ms = Helper::TimestampMs();
      DINGO_LOG(INFO) << "lease task BuildLeaseToKeyMap finish, start_time: " << lease_task_start_time_ms
                      << ", end_time: " << lease_task_end_time_ms
                      << ", cost: " << lease_task_end_time_ms - lease_task_start_time_ms << "ms";
    } else {
      DINGO_LOG(INFO) << "lease_to_key_map_ already initialized, and this is leader, skip BuildLeaseToKeyMap";
    }
  } else {
    // for follower
    // 1. clear lease_to_key_map_
    if (lease_to_key_map_init_term_.load(butil::memory_order_acquire) > 0) {
      DINGO_LOG(INFO) << "lease_to_key_map_ initialized, and this is not leader, clear lease_to_key_map_";

      {
        BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);
        lease_to_key_map_.clear();
      }

      lease_to_key_map_init_term_.store(0, butil::memory_order_release);

      DINGO_LOG(INFO) << "lease_to_key_map_ cleared";
    } else {
      DINGO_LOG(INFO) << "lease_to_key_map_ not initialized, and this is not leader, skip BuildLeaseToKeyMap, return";
    }

    return;
  }

  // for leader, do lease task
  std::vector<int64_t> lease_ids_to_revoke;
  pb::coordinator_internal::MetaIncrement meta_increment;
  {
    BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);
    if (lease_to_key_map_.empty()) {
      return;
    }

    for (const auto &it : lease_to_key_map_) {
      const auto &lease = it.second.lease;
      if (lease.ttl_seconds() + lease.last_renew_ts_seconds() < butil::gettimeofday_s()) {
        DINGO_LOG(INFO) << "lease id " << lease.id() << " expired, will revoke";
        lease_ids_to_revoke.emplace_back(lease.id());
      } else {
        auto remaining_ttl_seconds = lease.ttl_seconds() - (butil::gettimeofday_s() - lease.last_renew_ts_seconds());
        if (remaining_ttl_seconds < FLAGS_version_lease_print_ttl_remaining_seconds) {
          DINGO_LOG(INFO) << "lease id " << lease.id() << " is ok, last_renew_ts_seconds "
                          << lease.last_renew_ts_seconds() << ", ttl_seconds " << lease.ttl_seconds()
                          << ", remaining ttl_seconds " << remaining_ttl_seconds;
        }
      }
    }

    for (const auto &lease_id : lease_ids_to_revoke) {
      LeaseRevoke(lease_id, meta_increment, true);
    }

    // submit meta_increment with mutex locked
    // if we do this without lock, there maybe KvPut before LeaseRevoke, which will cause data inconsistency
    if (meta_increment.ByteSizeLong() > 0 && IsLeader()) {
      auto ret = SubmitMetaIncrementSync(meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "SubmitMetaIncrementSync failed, status: " << ret;
      }
    }
  }

  auto lease_task_end_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "lease task finish, start_time: " << lease_task_start_time_ms
                  << ", end_time: " << lease_task_end_time_ms
                  << ", cost: " << lease_task_end_time_ms - lease_task_start_time_ms << "ms";
}

void KvControl::BuildLeaseToKeyMap() {
  // build lease_to_key_map_

  // get start_time
  auto start_time_ms = Helper::TimestampMs();

  // we need to lock lease_to_key_map_mutex_ to avoid data inconsistency
  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  int64_t leader_term = leader_term_.load(butil::memory_order_acquire);
  if (leader_term < 0) {
    DINGO_LOG(INFO) << "BuildLeaseToKeyMap leader_term is less than 0, return, leader_term: " << leader_term;
    return;
  }

  std::map<int64_t, KvLeaseWithKeys> temp_lease_to_key_map;

  butil::FlatMap<int64_t, pb::coordinator_internal::LeaseInternal> version_lease_to_key_map_copy;
  version_lease_to_key_map_copy.init(10000);
  kv_lease_map_.GetRawMapCopy(version_lease_to_key_map_copy);

  // print time cost
  auto get_raw_map_copy_end_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "BuildLeaseToKeyMap GetRawMapCopy finish, start_time: " << start_time_ms
                  << ", end_time: " << get_raw_map_copy_end_time_ms
                  << ", cost: " << get_raw_map_copy_end_time_ms - start_time_ms << "ms"
                  << ", term: " << leader_term;

  temp_lease_to_key_map.clear();
  for (auto lease : version_lease_to_key_map_copy) {
    KvLeaseWithKeys lease_with_keys;
    lease_with_keys.lease.Swap(&lease.second);
    temp_lease_to_key_map.insert(std::make_pair(lease.first, lease_with_keys));
  }

  // print time cost
  auto build_lease_to_key_map_end_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "BuildLeaseToKeyMap build lease_to_key_map finish, start_time: " << start_time_ms
                  << ", end_time: " << build_lease_to_key_map_end_time_ms
                  << ", cost: " << build_lease_to_key_map_end_time_ms - start_time_ms << "ms"
                  << ", term: " << leader_term;

  // read all keys from version_kv to construct lease list
  std::vector<pb::coordinator_internal::KvIndexInternal> kv_index_values;

  if (kv_index_map_.GetAllValues(kv_index_values, [](pb::coordinator_internal::KvIndexInternal version_kv) -> bool {
        auto generation_count = version_kv.generations_size();
        if (generation_count == 0) {
          return false;
        }
        const auto &latest_generation = version_kv.generations(generation_count - 1);
        return latest_generation.has_create_revision();
      }) < 0) {
    DINGO_LOG(FATAL) << "OnLeaderStart kv_index_map_.GetAllValues failed, term: " << leader_term;
  }

  // print time cost
  auto get_all_values_end_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "BuildLeaseToKeyMap GetAllValues finish, start_time: " << start_time_ms
                  << ", end_time: " << get_all_values_end_time_ms
                  << ", cost: " << get_all_values_end_time_ms - start_time_ms << "ms"
                  << ", term: " << leader_term;

  for (const auto &kv_index_value : kv_index_values) {
    auto generation_count = kv_index_value.generations_size();
    if (generation_count == 0) {
      continue;
    }
    const auto &latest_generation = kv_index_value.generations(generation_count - 1);
    pb::coordinator_internal::KvRevInternal kv_rev;
    auto ret = GetRawKvRev(kv_index_value.mod_revision(), kv_rev);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "GetRawKvRev failed, revision: " << kv_index_value.mod_revision().ShortDebugString()
                       << ", term: " << leader_term;
      continue;
    }

    const auto &kv = kv_rev.kv();
    if (kv.lease() == 0) {
      continue;
    }

    auto it = temp_lease_to_key_map.find(kv.lease());
    if (it != temp_lease_to_key_map.end()) {
      it->second.keys.insert(kv.id());
    }
  }

  // print time cost
  auto build_lease_to_key_map_end_time_ms2 = Helper::TimestampMs();
  DINGO_LOG(INFO) << "BuildLeaseToKeyMap build lease_to_key_map finish, start_time: " << start_time_ms
                  << ", end_time: " << build_lease_to_key_map_end_time_ms2
                  << ", cost: " << build_lease_to_key_map_end_time_ms2 - start_time_ms << "ms"
                  << ", term: " << leader_term;

  // swap lease_to_key_map_
  lease_to_key_map_.swap(temp_lease_to_key_map);

  // update flag to true
  lease_to_key_map_init_term_.store(leader_term, butil::memory_order_release);

  // print time cost
  auto end_time_ms = Helper::TimestampMs();
  DINGO_LOG(INFO) << "BuildLeaseToKeyMap finish, start_time: " << start_time_ms << ", end_time: " << end_time_ms
                  << ", cost: " << end_time_ms - start_time_ms << "ms"
                  << ", term: " << leader_term << ", lease_to_key_map count: " << lease_to_key_map_.size();
}

butil::Status KvControl::LeaseAddKeys(int64_t lease_id, std::set<std::string> &keys) {
  DINGO_LOG(INFO) << "lease id " << lease_id << " add keys " << keys.size();

  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  KvLeaseWithKeys lease_with_keys;
  auto iter = lease_to_key_map_.find(lease_id);
  if (iter == lease_to_key_map_.end()) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  lease_with_keys = std::move(iter->second);

  for (const auto &key : keys) {
    auto ret = lease_with_keys.keys.insert(key);
    if (!ret.second) {
      DINGO_LOG(INFO) << "lease id " << lease_id << " add key " << key << " failed, already exists";
    } else {
      DINGO_LOG(INFO) << "lease id " << lease_id << " add key " << key << " success";
    }
  }

  lease_to_key_map_.insert_or_assign(lease_id, lease_with_keys);

  return butil::Status::OK();
}

butil::Status KvControl::LeaseRemoveKeys(int64_t lease_id, std::set<std::string> &keys) {
  DINGO_LOG(INFO) << "lease id " << lease_id << " remove keys " << keys.size();

  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  KvLeaseWithKeys lease_with_keys;
  auto iter = lease_to_key_map_.find(lease_id);
  if (iter == lease_to_key_map_.end()) {
    DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
    return butil::Status(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED, "lease id %lu not found", lease_id);
  }

  lease_with_keys = std::move(iter->second);

  for (const auto &key : keys) {
    auto ret = lease_with_keys.keys.erase(key);
    if (ret == 0) {
      DINGO_LOG(INFO) << "lease id " << lease_id << " remove key " << key << " failed, not exists";
    } else {
      DINGO_LOG(INFO) << "lease id " << lease_id << " remove key " << key << " success";
    }
  }

  lease_to_key_map_.insert_or_assign(lease_id, lease_with_keys);

  return butil::Status::OK();
}

butil::Status KvControl::LeaseRemoveMultiLeaseKeys(std::map<int64_t, std::set<std::string>> &lease_to_keys) {
  DINGO_LOG(INFO) << "lease remove multi lease keys, lease count " << lease_to_keys.size();

  BAIDU_SCOPED_LOCK(lease_to_key_map_mutex_);

  for (const auto &iter_lease_to_keys : lease_to_keys) {
    auto lease_id = iter_lease_to_keys.first;
    auto keys = iter_lease_to_keys.second;

    DINGO_LOG(INFO) << "lease id " << lease_id << " remove keys " << keys.size();

    KvLeaseWithKeys lease_with_keys;
    auto iter = lease_to_key_map_.find(lease_id);
    if (iter == lease_to_key_map_.end()) {
      DINGO_LOG(WARNING) << "lease id " << lease_id << " not found";
      continue;
    }

    lease_with_keys = std::move(iter->second);

    for (const auto &key : keys) {
      auto ret = lease_with_keys.keys.erase(key);
      if (ret == 0) {
        DINGO_LOG(INFO) << "lease id " << lease_id << " remove key " << key << " failed, not exists";
      } else {
        DINGO_LOG(INFO) << "lease id " << lease_id << " remove key " << key << " success";
      }
    }

    lease_to_key_map_.insert_or_assign(lease_id, lease_with_keys);
  }

  return butil::Status::OK();
}

}  // namespace dingodb
