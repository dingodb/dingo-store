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

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/version.pb.h"
#include "serial/buf.h"

namespace dingodb {

DEFINE_int64(max_kv_key_size, 4096, "max kv put count");
DEFINE_int64(max_kv_value_size, 8192, "max kv put count");
DEFINE_int64(compaction_retention_rev_count, 1000, "max revision count retention for compaction");
DEFINE_bool(auto_compaction, false, "auto compaction on/off");
DEFINE_int64(version_kv_max_count, 100000, "max kv count for version kv");

DEFINE_bool(dingo_log_switch_coor_kv, false, "log switch for kv control");

std::string KvControl::RevisionToString(const pb::coordinator_internal::RevisionInternal &revision) {
  Buf buf(17);
  buf.WriteLong(revision.main());
  buf.Write('_');
  buf.WriteLong(revision.sub());

  std::string result;
  buf.GetBytes(result);

  return result;
}

pb::coordinator_internal::RevisionInternal KvControl::StringToRevision(const std::string &input_string) {
  pb::coordinator_internal::RevisionInternal revision;
  if (input_string.size() != 17) {
    DINGO_LOG(ERROR) << "StringToRevision failed, input_strint size is not 17, value:[" << input_string << "]";
    return revision;
  }

  Buf buf(input_string);
  revision.set_main(buf.ReadLong());
  buf.Read();
  revision.set_sub(buf.ReadLong());

  return revision;
}

butil::Status KvControl::GetRawKvIndex(const std::string &key, pb::coordinator_internal::KvIndexInternal &kv_index) {
  auto ret = kv_index_map_.Get(key, kv_index);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRawKvIndex not found, key:[" << key << "]";
    return butil::Status(EINVAL, "GetRawKvIndex not found");
  }
  return butil::Status::OK();
}

butil::Status KvControl::RangeRawKvIndex(const std::string &key, const std::string &range_end,
                                         std::vector<pb::coordinator_internal::KvIndexInternal> &kv_index_values) {
  // scan kv_index for legal keys
  std::string lower_bound = key;
  std::string upper_bound = range_end;

  if (range_end == std::string(1, '\0')) {
    upper_bound = std::string(FLAGS_max_kv_key_size, '\xff');
  } else if (range_end.empty()) {
    upper_bound = Helper::PrefixNext(key);
  }

  auto ret = kv_index_map_.GetRangeValues(
      kv_index_values, lower_bound, upper_bound, nullptr,
      [&key, &range_end](const pb::coordinator_internal::KvIndexInternal &version_kv) -> bool {
        auto generation_count = version_kv.generations_size();
        if (generation_count == 0) {
          return false;
        }

        const auto &latest_generation = version_kv.generations(generation_count - 1);
        if (!latest_generation.has_create_revision() || latest_generation.revisions_size() == 0) {
          return false;
        }

        if (range_end.empty()) {
          return key == version_kv.id();
        } else if (range_end == std::string(1, '\0')) {
          return version_kv.id().compare(key) >= 0;
        } else {
          return version_kv.id().compare(key) >= 0 && version_kv.id().compare(range_end) < 0;
        }
      });

  if (ret < 0) {
    DINGO_LOG(WARNING) << "RangeRawKvIndex failed, key:[" << key << "]";
    return butil::Status(EINVAL, "RangeRawKvIndex failed");
  } else {
    return butil::Status::OK();
  }
}

butil::Status KvControl::PutRawKvIndex(const std::string &key,
                                       const pb::coordinator_internal::KvIndexInternal &kv_index) {
  auto ret = kv_index_meta_->Put(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "PutRawKvIndex failed, key:[" << key << "], errcode" << ret.error_code()
                     << ", error: " << ret.error_str();
    return ret;
  }

  return butil::Status::OK();
}

butil::Status KvControl::DeleteRawKvIndex(const std::string &key) {
  auto ret = kv_index_meta_->Erase(key);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DeleteRawKvIndex failed, key:[" << key << "], errcode" << ret.error_code()
                     << ", error: " << ret.error_str();
    return ret;
  }

  return butil::Status::OK();
}

butil::Status KvControl::GetRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                     pb::coordinator_internal::KvRevInternal &kv_rev) {
  auto ret = kv_rev_meta_->Get(RevisionToString(revision), kv_rev);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "GetRawKvRev not found, revision:[" << revision.ShortDebugString() << "]";
    return butil::Status(EINVAL, "GetRawKvRev not found");
  }

  return butil::Status::OK();
}

butil::Status KvControl::PutRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                     const pb::coordinator_internal::KvRevInternal &kv_rev) {
  DINGO_LOG(INFO) << "PutRawKvRev success, revision:[" << revision.ShortDebugString() << "], kv_rev:["
                  << kv_rev.ShortDebugString() << "]"
                  << " kv_rev.id: " << Helper::StringToHex(kv_rev.id())
                  << ", revision_string: " << Helper::StringToHex(RevisionToString(revision));

  kv_rev_meta_->Put(RevisionToString(revision), kv_rev);

  return butil::Status::OK();
}

butil::Status KvControl::DeleteRawKvRev(const pb::coordinator_internal::RevisionInternal &revision) {
  kv_rev_meta_->Erase(RevisionToString(revision));

  return butil::Status::OK();
}

// kv functions for api
// KvRange is the get function
// in:  key
// in:  range_end
// in:  limit
// in:  keys_only
// in:  count_only
// out: kv
// out: return_count
// out: has_more
// return: errno
butil::Status KvControl::KvRange(const std::string &key, const std::string &range_end, int64_t limit, bool keys_only,
                                 bool count_only, std::vector<pb::version::Kv> &kv, int64_t &return_count,
                                 bool &has_more) {
  DINGO_LOG(DEBUG) << "KvRange, key: " << key << "(" << Helper::StringToHex(key) << "), range_end: " << range_end << "("
                   << Helper::StringToHex(range_end) << "), limit: " << limit << ", keys_only: " << keys_only
                   << ", count_only: " << count_only;

  has_more = false;
  if (limit == 0) {
    limit = INT64_MAX;
  }

  std::vector<pb::coordinator_internal::KvIndexInternal> kv_index_values;

  if (range_end.empty()) {
    pb::coordinator_internal::KvIndexInternal kv_index;
    auto ret = GetRawKvIndex(key, kv_index);
    if (!ret.ok()) {
      return butil::Status::OK();
    }
    kv_index_values.push_back(kv_index);
  } else {
    // scan kv_index for legal keys
    auto ret = RangeRawKvIndex(key, range_end, kv_index_values);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange kv_index_map_.RangeRawKvIndex failed, key: " << key << "("
                       << Helper::StringToHex(key) << "), range_end: " << range_end << "("
                       << Helper::StringToHex(range_end) << "), error: " << ret.error_str();
      return ret;
    }
  }

  // query kv_rev for values
  uint32_t limit_count = 0;
  for (const auto &kv_index_value : kv_index_values) {
    auto generation_count = kv_index_value.generations_size();
    if (generation_count == 0) {
      DINGO_LOG(INFO) << "KvRange generation_count is 0, key: " << key << "(" << Helper::StringToHex(key) << ")";
      continue;
    }
    const auto &latest_generation = kv_index_value.generations(generation_count - 1);
    if (!latest_generation.has_create_revision() || latest_generation.revisions_size() == 0) {
      DINGO_LOG(INFO) << "KvRange latest_generation is empty, key: " << key << "(" << Helper::StringToHex(key) << ")";
      continue;
    }

    limit_count++;
    if (limit_count > limit) {
      has_more = true;
      break;
    }

    if (count_only) {
      continue;
    }

    DINGO_LOG(DEBUG) << "KvRange will query kv_rev, revision: " << kv_index_value.mod_revision().ShortDebugString()
                     << ", key: " << key << "(" << Helper::StringToHex(key) << ")";

    pb::coordinator_internal::KvRevInternal kv_rev;
    auto ret = GetRawKvRev(kv_index_value.mod_revision(), kv_rev);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "GetRawKvRev failed, revision: " << kv_index_value.mod_revision().ShortDebugString()
                       << ", error: " << ret.error_str();
      continue;
    }

    const auto &kv_in_rev = kv_rev.kv();
    pb::version::Kv kv_temp;
    kv_temp.set_create_revision(kv_in_rev.create_revision().main());
    kv_temp.set_mod_revision(kv_in_rev.mod_revision().main());
    kv_temp.set_version(kv_in_rev.version());
    kv_temp.set_lease(kv_in_rev.lease());
    kv_temp.mutable_kv()->set_key(kv_in_rev.id());
    if (!keys_only) {
      kv_temp.mutable_kv()->set_value(kv_in_rev.value());
    }

    DINGO_LOG(DEBUG) << "KvRange will return kv: " << kv_temp.ShortDebugString()
                     << ", kv_rev: " << kv_rev.ShortDebugString();

    if (kv_temp.kv().key().empty()) {
      DINGO_LOG(ERROR) << "KvRange will return null kv: " << kv_temp.ShortDebugString()
                       << ", kv_rev: " << kv_rev.ShortDebugString()
                       << ", mod_revision: " << kv_index_value.mod_revision().ShortDebugString()
                       << ", original key: " << key << "(" << Helper::StringToHex(key)
                       << "), original range_end : " << range_end << "(" << Helper::StringToHex(range_end)
                       << "), kv_index_internal: " << kv_index_value.ShortDebugString();
    }

    // add to output
    kv.push_back(kv_temp);
  }

  return_count = kv.size();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvRange finish, key: " << key << "(" << Helper::StringToHex(key) << "), range_end: " << range_end << "("
      << Helper::StringToHex(range_end) << "), limit: " << limit << ", keys_only: " << keys_only
      << ", count_only: " << count_only << ", kv size: " << kv.size() << ", total_count_in_range: " << return_count;

  return butil::Status::OK();
}

// kv functions for internal use
// KvRangeRawKeys is the get function for raw keys
// in:  key
// in:  range_end
// out: keys
// return: errno
butil::Status KvControl::KvRangeRawKeys(const std::string &key, const std::string &range_end,
                                        std::vector<std::string> &keys) {
  std::vector<pb::coordinator_internal::KvIndexInternal> kv_index_values;

  if (range_end.empty()) {
    pb::coordinator_internal::KvIndexInternal kv_index;
    auto ret = GetRawKvIndex(key, kv_index);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange GetRawKvIndex not found, key: " << key << "(" << Helper::StringToHex(key)
                       << "), error: " << ret.error_str();
      return butil::Status::OK();
    }
    keys.push_back(key);
  } else {
    // scan kv_index for legal keys
    auto ret = RangeRawKvIndex(key, range_end, kv_index_values);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange kv_index_map_.RangeRawKvIndex failed, key: " << key << "("
                       << Helper::StringToHex(key) << "), range_end: " << range_end << "("
                       << Helper::StringToHex(range_end) << "), error: " << ret.error_str();
      return ret;
    }

    for (const auto &kv_index_value : kv_index_values) {
      keys.push_back(kv_index_value.id());
    }
  }

  DINGO_LOG(DEBUG) << "KvRangeRawKeys finish, key: " << key << "(" << Helper::StringToHex(key)
                   << "), range_end: " << range_end << "(" << Helper::StringToHex(range_end)
                   << "), keys size: " << keys.size();

  return butil::Status::OK();
}

// KvPut is the put function
// in:  key_value
// in:  lease_id
// in:  need_prev_kv
// in:  igore_value
// in:  ignore_lease
// out:  prev_kv
// return: errno
butil::Status KvControl::KvPut(const pb::common::KeyValue &key_value_in, int64_t lease_id, bool need_prev_kv,
                               bool ignore_value, bool ignore_lease, pb::version::Kv &prev_kv, int64_t &lease_grant_id,
                               pb::coordinator_internal::MetaIncrement &meta_increment) {
  DINGO_LOG(INFO) << "KvPut, key: " << key_value_in.key() << "(" << Helper::StringToHex(key_value_in.key())
                  << "), value: " << key_value_in.value() << "(" << Helper::StringToHex(key_value_in.value())
                  << "), lease_id: " << lease_id << ", need_prev_kv: " << need_prev_kv
                  << ", igore_value: " << ignore_value << ", ignore_lease: " << ignore_lease;

  if (kv_index_map_.Size() > FLAGS_version_kv_max_count) {
    DINGO_LOG(ERROR) << "KvPut kv_index_map_ size: " << kv_index_map_.Size() << ", will do compaction";
    return butil::Status(pb::error::Errno::EKV_COUNT_EXCEEDS_LIMIT, "KvPut kv_index_map_ size is too large");
  }

  // check key
  if (key_value_in.key().empty()) {
    DINGO_LOG(ERROR) << "KvPut key is empty";
    return butil::Status(EINVAL, "KvPut key is empty");
  }

  // check key length
  if (key_value_in.key().size() > FLAGS_max_kv_key_size) {
    DINGO_LOG(ERROR) << "KvPut key is too long, max_kv_key_size: " << FLAGS_max_kv_key_size
                     << ", key: " << key_value_in.key() << "(" << Helper::StringToHex(key_value_in.key()) << ")";
    return butil::Status(EINVAL, "KvPut key is too long");
  }

  // check value
  if (!ignore_value && key_value_in.value().empty()) {
    DINGO_LOG(ERROR) << "KvPut value is empty";
    return butil::Status(EINVAL, "KvPut value is empty");
  }

  // check value length
  if (!ignore_value && key_value_in.value().size() > FLAGS_max_kv_value_size) {
    DINGO_LOG(ERROR) << "KvPut value is too long, max_kv_value_size: " << FLAGS_max_kv_value_size
                     << ", key: " << key_value_in.key() << "(" << Helper::StringToHex(key_value_in.key())
                     << "), value: " << key_value_in.value() << "(" << Helper::StringToHex(key_value_in.value()) << ")";
    return butil::Status(EINVAL, "KvPut value is too long");
  }

  // check lease is valid
  if (!ignore_lease && lease_id != 0) {
    std::set<std::string> keys;
    int64_t granted_ttl = 0;
    int64_t remaining_ttl = 0;

    auto ret = LeaseQuery(lease_id, false, granted_ttl, remaining_ttl, keys);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvPut LeaseQuery failed, lease_id: " << lease_id << ", error: " << ret.error_str();
      return ret;
    }

    lease_grant_id = lease_id;
  }

  // temp value for ignore_lease and need_prev_kv
  std::vector<pb::version::Kv> kvs_temp;

  int64_t total_count_in_range = 0;
  bool has_more = false;
  KvRange(key_value_in.key(), std::string(), 1, false, false, kvs_temp, total_count_in_range, has_more);
  if (ignore_lease) {
    if (!kvs_temp.empty()) {
      // if ignore_lease, get the lease of the key
      lease_grant_id = kvs_temp[0].lease();
    } else {
      DINGO_LOG(ERROR) << "KvPut ignore_lease, but not found key: " << key_value_in.key() << "("
                       << Helper::StringToHex(key_value_in.key()) << ")";
      return butil::Status(EINVAL, "KvPut ignore_lease, but not found key");
    }
  } else {
    if (!kvs_temp.empty()) {
      // if ignore_lease, get the lease of the key
      lease_grant_id = kvs_temp[0].lease();
      if (lease_grant_id != lease_id) {
        DINGO_LOG(ERROR) << "KvPut lease_id not match, key: " << key_value_in.key() << "("
                         << Helper::StringToHex(key_value_in.key()) << "), lease_id: " << lease_id
                         << ", lease_grant_id: " << lease_grant_id;
        return butil::Status(EINVAL, "KvPut lease_id not match");
      }
    }
  }

  // add key to lease if lease_id is not 0
  if (lease_grant_id != 0) {
    std::set<std::string> keys;
    keys.insert(key_value_in.key());
    auto ret = LeaseAddKeys(lease_grant_id, keys);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvPut LeaseAddKeys failed, lease_id: " << lease_grant_id << ", key: " << key_value_in.key()
                       << "(" << Helper::StringToHex(key_value_in.key()) << "), error: " << ret.error_str();
      return ret;
    }

    DINGO_LOG(INFO) << "KvPut LeaseAddKeys success, lease_id: " << lease_grant_id << ", key: " << key_value_in.key()
                    << "(" << Helper::StringToHex(key_value_in.key()) << ")";
  }

  // get prev_kvs
  if (need_prev_kv) {
    if (kvs_temp.empty()) {
      int64_t total_count_in_range = 0;
      bool has_more = false;
      KvRange(key_value_in.key(), std::string(), 1, false, false, kvs_temp, total_count_in_range, has_more);
    }
    if (!kvs_temp.empty()) {
      prev_kv = kvs_temp[0];
    } else {
      pb::version::Kv kv_temp;
      prev_kv = kv_temp;
    }
  }

  // update kv_index
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvPut will submit meta_increment, key: " << key_value_in.key() << "("
      << Helper::StringToHex(key_value_in.key()) << "), value: " << Helper::StringToHex(key_value_in.value()) << "("
      << Helper::StringToHex(key_value_in.value()) << ")";

  // add meta_increment
  auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
  kv_index_meta_increment->set_id(key_value_in.key());
  kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  kv_index_meta_increment->set_event_type(pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_PUT);
  kv_index_meta_increment->set_ignore_lease(ignore_lease);
  kv_index_meta_increment->set_lease_id(lease_grant_id);
  if (!ignore_value) {
    kv_index_meta_increment->set_ignore_value(ignore_value);
    kv_index_meta_increment->set_value(key_value_in.value());
  }
  if (!ignore_lease) {
    kv_index_meta_increment->set_ignore_lease(ignore_lease);
  }

  return butil::Status::OK();
}

// KvDeleteRange is the delete function
// in:  key
// in:  range_end
// in:  need_prev
// out: deleted_count
// out:  prev_kvs
// return: errno
butil::Status KvControl::KvDeleteRange(const std::string &key, const std::string &range_end, bool need_prev_kv,
                                       bool need_lease_remove_keys, int64_t &deleted_count,
                                       std::vector<pb::version::Kv> &prev_kvs,
                                       pb::coordinator_internal::MetaIncrement &meta_increment) {
  DINGO_LOG(INFO) << "KvDeleteRange, key: " << key << "(" << Helper::StringToHex(key) << "), range_end: " << range_end
                  << "(" << Helper::StringToHex(range_end) << "), need_prev_kv: " << need_prev_kv
                  << ", need_lease_remove_keys: " << need_lease_remove_keys;

  std::vector<pb::version::Kv> kvs_to_delete;
  int64_t total_count_in_range = 0;
  bool has_more = false;

  bool key_only = !need_prev_kv;

  auto ret = KvRange(key, range_end, INT64_MAX, key_only, false, kvs_to_delete, total_count_in_range, has_more);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteRange KvRange failed, key: " << key << "(" << Helper::StringToHex(key)
                     << "), range_end: " << range_end << "(" << Helper::StringToHex(range_end)
                     << "), error: " << ret.error_str();
    return ret;
  }

  std::map<int64_t, std::set<std::string>> keys_to_remove_lease;

  // do kv_delete
  for (const auto &kv_to_delete : kvs_to_delete) {
    // update kv_index
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvDelete will submit meta_increment, key: " << kv_to_delete.kv().key() << "("
        << Helper::StringToHex(kv_to_delete.kv().key()) << ")";

    // add meta_increment
    auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
    kv_index_meta_increment->set_id(kv_to_delete.kv().key());
    kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    kv_index_meta_increment->set_event_type(pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_DELETE);

    if (kv_to_delete.lease() == 0) {
      continue;
    }

    // prepare for lease remove
    std::set<std::string> keys;
    keys.insert(kv_to_delete.kv().key());
    keys_to_remove_lease.insert_or_assign(kv_to_delete.lease(), keys);

    if (need_lease_remove_keys) {
      DINGO_LOG(INFO) << "KvDelete will remove lease for key: " << kv_to_delete.kv().key() << "("
                      << Helper::StringToHex(kv_to_delete.kv().key()) << "), lease_id: " << kv_to_delete.lease();
    }
  }

  // do lease_remove_keys
  if (need_lease_remove_keys && (!keys_to_remove_lease.empty())) {
    ret = LeaseRemoveMultiLeaseKeys(keys_to_remove_lease);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvDeleteRange LeaseRemoveKeys failed, keys_to_remove_lease size: "
                       << keys_to_remove_lease.size() << ", error: " << ret.error_str();
      return ret;
    }
  }

  if (!kvs_to_delete.empty()) {
    deleted_count = kvs_to_delete.size();
  }

  if (need_prev_kv) {
    prev_kvs.swap(kvs_to_delete);
  }

  return butil::Status::OK();
}

butil::Status KvControl::KvPutApply(const std::string &key,
                                    const pb::coordinator_internal::RevisionInternal &op_revision, bool ignore_lease,
                                    int64_t lease_id, bool ignore_value, const std::string &value) {
  DINGO_LOG(INFO) << "KvPutApply, key: " << key << "(" << Helper::StringToHex(key)
                  << "), op_revision: " << op_revision.ShortDebugString() << ", ignore_lease: " << ignore_lease
                  << ", lease_id: " << lease_id << ", ignore_value: " << ignore_value << ", value: " << value << "("
                  << Helper::StringToHex(value) << ")";

  // get kv_index and generate new kv_index
  pb::coordinator_internal::KvIndexInternal kv_index;
  pb::coordinator_internal::RevisionInternal last_mod_revision;
  pb::coordinator_internal::RevisionInternal new_create_revision;
  new_create_revision.set_main(op_revision.main());
  new_create_revision.set_sub(op_revision.sub());
  int64_t new_version = 1;

  pb::version::Kv prev_kv;
  pb::version::Kv new_kv;

  auto ret = GetRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvPutApply GetRawKvIndex not found, will create key: " << key << "(" << Helper::StringToHex(key)
        << "), error: " << ret.error_str();
    kv_index.set_id(key);
    kv_index.mutable_mod_revision()->set_main(op_revision.main());
    kv_index.mutable_mod_revision()->set_sub(op_revision.sub());
    auto *generation = kv_index.add_generations();
    generation->mutable_create_revision()->set_main(op_revision.main());
    generation->mutable_create_revision()->set_sub(op_revision.sub());
    generation->set_verison(1);
    *(generation->add_revisions()) = op_revision;

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvPutApply GetRawKvIndex not found, will create new kv_index: key: " << key << "("
        << Helper::StringToHex(key) << "), kv_index generation: " << generation->ShortDebugString();
  } else {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvPutApply GetRawKvIndex found, will update key: " << key << "(" << Helper::StringToHex(key)
        << "), error: " << ret.error_str();

    last_mod_revision = kv_index.mod_revision();

    if (kv_index.generations_size() == 0) {
      auto *generation = kv_index.add_generations();
      generation->mutable_create_revision()->set_main(op_revision.main());
      generation->mutable_create_revision()->set_sub(op_revision.sub());
      generation->set_verison(1);
      *(generation->add_revisions()) = op_revision;

      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
          << "KvPutApply kv_index add generation: " << generation->ShortDebugString() << ", key: " << key << "("
          << Helper::StringToHex(key) << ")";
    } else {
      // auto &latest_generation = *kv_index.mutable_generations()->rbegin();
      auto *latest_generation = kv_index.mutable_generations(kv_index.generations_size() - 1);
      if (latest_generation->has_create_revision()) {
        *(latest_generation->add_revisions()) = op_revision;
        latest_generation->set_verison(latest_generation->verison() + 1);

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
            << "KvPutApply latest_generation add revsion: " << latest_generation->ShortDebugString() << ", key: " << key
            << "(" << Helper::StringToHex(key) << ")";

        // only in this situation, the prev_kv is meaningful
        prev_kv.set_create_revision(latest_generation->create_revision().main());
        prev_kv.set_mod_revision(kv_index.mod_revision().main());
        prev_kv.set_version(latest_generation->verison());
      } else {
        latest_generation->mutable_create_revision()->set_main(op_revision.main());
        latest_generation->mutable_create_revision()->set_sub(op_revision.sub());
        latest_generation->set_verison(1);
        *(latest_generation->add_revisions()) = op_revision;

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
            << "KvPutApply latest_generation create revsion: " << latest_generation->ShortDebugString()
            << ", key: " << key << "(" << Helper::StringToHex(key) << ")";
      }

      // setup new_create_revision to last create_revision
      new_create_revision.set_main(latest_generation->create_revision().main());
      new_create_revision.set_sub(latest_generation->create_revision().sub());

      // setup new_version
      new_version = latest_generation->verison();
    }
    *(kv_index.mutable_mod_revision()) = op_revision;
  }

  // generate new kv_rev
  pb::coordinator_internal::KvRevInternal kv_rev_last;
  pb::coordinator_internal::KvRevInternal kv_rev;
  GetRawKvRev(last_mod_revision, kv_rev_last);

  kv_rev.set_id(RevisionToString(op_revision));

  // kv is KvInternal
  auto *kv = kv_rev.mutable_kv();

  // id is key
  kv->set_id(key);
  // value
  if (!ignore_value) {
    kv->set_value(value);
  } else {
    kv->set_value(kv_rev_last.kv().value());
  }
  // create_revision
  kv->mutable_create_revision()->set_main(new_create_revision.main());
  kv->mutable_create_revision()->set_sub(new_create_revision.sub());
  // mod_revision
  kv->mutable_mod_revision()->set_main(op_revision.main());
  kv->mutable_mod_revision()->set_sub(op_revision.sub());
  // version
  kv->set_version(new_version);
  // lease
  if (ignore_lease) {
    kv->set_lease(kv_rev_last.kv().lease());
  } else {
    kv->set_lease(lease_id);
  }

  // check if lease is exists before apply to state machine
  if (kv->lease() > 0) {
    auto ret1 = kv_lease_map_.Exists(kv->lease());
    if (!ret1) {
      DINGO_LOG(WARNING) << "KvPutApply kv_lease_map_.Exists failed, lease_id: " << kv->lease() << ", key: " << key
                         << "(" << Helper::StringToHex(key) << ")";
      return butil::Status(EINVAL, "KvPutApply kv_lease_map_.Exists failed");
    }
  }

  // do real write to state machine
  // CAUTION: In doing put, we MUST do PutRawKvRev before put RawKvIndex
  ret = PutRawKvRev(op_revision, kv_rev);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvPutApply PutRawKvRev failed, revision: " << op_revision.ShortDebugString()
                     << ", kv_rev: " << kv_rev.ShortDebugString() << ", error: " << ret.error_str();
    return ret;
  }
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvPutApply PutRawKvRev success, revision: " << op_revision.ShortDebugString()
      << ", kv_rev: " << kv_rev.ShortDebugString();

  ret = PutRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvPutApply PutRawKvIndex failed, key: " << key << "(" << Helper::StringToHex(key)
                     << "), kv_index: " << kv_index.ShortDebugString() << ", error: " << ret.error_str();
  }
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvPutApply PutRawKvIndex success, key: " << key << "(" << Helper::StringToHex(key)
      << "), kv_index: " << kv_index.ShortDebugString();

  // trigger watch
  if (!one_time_watch_map_.empty()) {
    DINGO_LOG(INFO) << "KvPutApply one_time_watch_map_ is not empty, will trigger watch, key: " << key << "("
                    << Helper::StringToHex(key) << "), watch size: " << one_time_watch_map_.size();

    if (prev_kv.create_revision() > 0) {
      prev_kv.set_lease(kv_rev_last.kv().lease());
      prev_kv.mutable_kv()->set_key(key);
      prev_kv.mutable_kv()->set_value(kv_rev_last.kv().value());
    }
    new_kv.set_create_revision(new_create_revision.main());
    new_kv.set_mod_revision(op_revision.main());
    new_kv.set_version(new_version);
    new_kv.set_lease(kv_rev.kv().lease());
    new_kv.mutable_kv()->set_key(key);
    new_kv.mutable_kv()->set_value(kv_rev.kv().value());

    TriggerOneWatch(key, pb::version::Event::EventType::Event_EventType_PUT, new_kv, prev_kv);
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvPutApply success after trigger watch, key: " << key << "(" << Helper::StringToHex(key)
      << "), op_revision: " << op_revision.ShortDebugString() << ", ignore_lease: " << ignore_lease
      << ", lease_id: " << lease_id << ", ignore_value: " << ignore_value << ", value: " << value;

  return butil::Status::OK();
}

butil::Status KvControl::KvDeleteApply(const std::string &key,
                                       const pb::coordinator_internal::RevisionInternal &op_revision) {
  DINGO_LOG(INFO) << "KvDeleteApply, key: " << key << "(" << Helper::StringToHex(key)
                  << "), revision: " << op_revision.ShortDebugString();

  // get kv_index and generate new kv_index
  pb::coordinator_internal::KvIndexInternal kv_index;
  pb::coordinator_internal::RevisionInternal last_mod_revision;
  pb::coordinator_internal::RevisionInternal new_create_revision;
  new_create_revision.set_main(op_revision.main());
  new_create_revision.set_sub(op_revision.sub());
  int64_t new_version = 1;

  pb::version::Kv prev_kv;
  pb::version::Kv new_kv;

  auto ret = GetRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvDeleteApply GetRawKvIndex not found, no need to delete: " << key << "(" << Helper::StringToHex(key)
        << "), error: " << ret.error_str();
    return butil::Status::OK();
  } else {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvDeleteApply GetRawKvIndex found, will delete key: " << key << "(" << Helper::StringToHex(key)
        << "), error: " << ret.error_str();

    last_mod_revision = kv_index.mod_revision();

    if (kv_index.generations_size() == 0) {
      // create a null generator means delete
      auto *generation = kv_index.add_generations();
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
          << "KvDeleteApply kv_index add null generation[0]: " << generation->ShortDebugString() << ", key: " << key
          << "(" << Helper::StringToHex(key) << ")";
    } else {
      // auto &latest_generation = *kv_index.mutable_generations()->rbegin();
      auto *latest_generation = kv_index.mutable_generations(kv_index.generations_size() - 1);
      if (latest_generation->has_create_revision()) {
        // add a the delete revision to latest generation
        *(latest_generation->add_revisions()) = op_revision;
        latest_generation->set_verison(latest_generation->verison() + 1);

        // create a null generator means delete
        auto *generation = kv_index.add_generations();
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
            << "KvDeleteApply kv_index add null generation[1]: " << generation->ShortDebugString() << ", key: " << key
            << "(" << Helper::StringToHex(key) << ")";

        // only in this situation, the prev_kv is meaningful
        prev_kv.set_create_revision(latest_generation->create_revision().main());
        prev_kv.set_mod_revision(kv_index.mod_revision().main());
        prev_kv.set_version(latest_generation->verison());
      } else {
        // a null generation means delete
        // so we do not need to add a new generation
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
            << "KvDeleteApply kv_index exist null generation[1], nothing to do: "
            << latest_generation->ShortDebugString() << ", key: " << key << "(" << Helper::StringToHex(key) << ")";
      }

      // setup new_create_revision to last create_revision
      new_create_revision.set_main(latest_generation->create_revision().main());
      new_create_revision.set_sub(latest_generation->create_revision().sub());

      // setup new_version
      new_version = latest_generation->verison();
    }
    *(kv_index.mutable_mod_revision()) = op_revision;
  }

  // generate new kv_rev
  pb::coordinator_internal::KvRevInternal kv_rev_last;
  pb::coordinator_internal::KvRevInternal kv_rev;
  GetRawKvRev(last_mod_revision, kv_rev_last);

  kv_rev.set_id(RevisionToString(op_revision));

  // kv is KvInternal
  auto *kv = kv_rev.mutable_kv();

  // id is key
  kv->set_id(key);
  // create_revision
  kv->mutable_create_revision()->set_main(new_create_revision.main());
  kv->mutable_create_revision()->set_sub(new_create_revision.sub());
  // mod_revision
  kv->mutable_mod_revision()->set_main(op_revision.main());
  kv->mutable_mod_revision()->set_sub(op_revision.sub());
  // version
  kv->set_version(new_version);
  // is_deleted
  kv->set_is_deleted(true);

  // do real write to state machine
  // CAUTION: When deleting kv, we must do PutRawKvRev before put RawKvIndex
  ret = PutRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteApply PutRawKvIndex failed, key: " << key << "(" << Helper::StringToHex(key)
                     << "), kv_index: " << kv_index.ShortDebugString() << ", error: " << ret.error_str();
  }

  ret = PutRawKvRev(op_revision, kv_rev);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteApply PutRawKvRev failed, revision: " << op_revision.ShortDebugString()
                     << "key: " << key << "(" << Helper::StringToHex(key) << "), kv_rev: " << kv_rev.ShortDebugString()
                     << ", error: " << ret.error_str();
    return ret;
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvDeleteApply success, key: " << key << "(" << Helper::StringToHex(key)
      << "), revision: " << op_revision.ShortDebugString();

  // trigger watch
  if (!one_time_watch_map_.empty()) {
    DINGO_LOG(INFO) << "KvDeleteApply one_time_watch_map_ is not empty, will trigger watch, key: " << key << "("
                    << Helper::StringToHex(key) << "), watch size: " << one_time_watch_map_.size();

    if (prev_kv.create_revision() > 0) {
      prev_kv.set_lease(kv_rev_last.kv().lease());
      prev_kv.mutable_kv()->set_key(key);
      prev_kv.mutable_kv()->set_value(kv_rev_last.kv().value());
    }
    new_kv.set_create_revision(new_create_revision.main());
    new_kv.set_mod_revision(op_revision.main());
    new_kv.set_version(new_version);
    new_kv.set_lease(kv_rev.kv().lease());
    new_kv.mutable_kv()->set_key(key);
    new_kv.mutable_kv()->set_value(kv_rev.kv().value());

    TriggerOneWatch(key, pb::version::Event::EventType::Event_EventType_DELETE, new_kv, prev_kv);
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
      << "KvDeleteApply success after trigger watch, key: " << key << "(" << Helper::StringToHex(key)
      << "), revision: " << op_revision.ShortDebugString();

  return butil::Status::OK();
}

void KvControl::CompactionTask() {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv) << "compaction task start";

  if (!FLAGS_auto_compaction) {
    DINGO_LOG(INFO) << "compaction task skip, auto_compaction is false";
    return;
  }

  // get all keys in kv_index_map_
  std::vector<std::string> keys;
  auto ret = kv_index_map_.GetAllKeys(keys);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "kv_index_map_ GetAllKeys failed";
    return;
  }

  // build revision struct
  pb::coordinator_internal::RevisionInternal compact_revision;

  int64_t now_revision = GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION);
  if (now_revision < FLAGS_compaction_retention_rev_count) {
    DINGO_LOG(INFO) << "compaction task skip, now_revision: " << now_revision
                    << ", compaction_retention_rev_count: " << FLAGS_compaction_retention_rev_count;
    return;
  } else {
    compact_revision.set_main(now_revision - FLAGS_compaction_retention_rev_count);
  }

  compact_revision.set_sub(0);

  // do compaction for each key
  std::vector<std::string> keys_to_compact;
  for (const auto &key : keys) {
    if (keys_to_compact.size() < 50) {
      keys_to_compact.push_back(key);
    } else {
      auto ret = KvCompact(keys_to_compact, compact_revision);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "KvCompact failed, error: " << ret.error_str() << ", keys size: " << keys_to_compact.size();
        for (const auto &key : keys_to_compact) {
          DINGO_LOG(ERROR) << "KvCompact failed, key: " << key << "(" << Helper::StringToHex(key) << ")";
        }
      }
      keys_to_compact.clear();
    }
  }
  if (!keys_to_compact.empty()) {
    auto ret = KvCompact(keys_to_compact, compact_revision);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvCompact failed, error: " << ret.error_str() << ", keys size: " << keys_to_compact.size();
      for (const auto &key : keys_to_compact) {
        DINGO_LOG(ERROR) << "KvCompact failed, key: " << key << "(" << Helper::StringToHex(key) << ")";
      }
    }
  }

  DINGO_LOG(INFO) << "compaction task end, keys_count=" << keys.size();
}

static void Done(std::atomic<bool> *done) { done->store(true, std::memory_order_release); }

butil::Status KvControl::KvCompact(const std::vector<std::string> &keys,
                                   const pb::coordinator_internal::RevisionInternal &compact_revision) {
  DINGO_LOG(INFO) << "KvCompact, keys size: " << keys.size() << ", revision: " << compact_revision.ShortDebugString();

  if (keys.empty()) {
    return butil::Status::OK();
  }

  for (const auto &key : keys) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvCompact, will compact key: " << key << "(" << Helper::StringToHex(key) << ")";
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  for (auto key : keys) {
    auto *kv_index_increment = meta_increment.add_kv_indexes();
    kv_index_increment->set_id(key);
    kv_index_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    kv_index_increment->set_event_type(
        ::dingodb::pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_COMPACTION);
    *(kv_index_increment->mutable_op_revision()) = compact_revision;
  }

  if (meta_increment.ByteSizeLong() > 0) {
    auto ret = SubmitMetaIncrementSync(meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvCompact SubmitMetaIncrement failed, error: " << ret.error_str();
      return ret;
    }
  }

  return butil::Status::OK();
}

butil::Status KvControl::KvCompactApply(const std::string &key,
                                        const pb::coordinator_internal::RevisionInternal &compact_revision) {
  DINGO_LOG(INFO) << "KvCompactApply, key: " << key << "(" << Helper::StringToHex(key)
                  << "), revision: " << compact_revision.ShortDebugString();

  // get kv_index
  pb::coordinator_internal::KvIndexInternal kv_index;
  auto ret = GetRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvCompactApply GetRawKvIndex failed, key: " << key << "(" << Helper::StringToHex(key)
                     << "), key_hex: " << Helper::StringToHex(key) << ", error: " << ret.error_str();
    return ret;
  }

  // iterate kv_index generations, find revisions less than compact_revision
  if (kv_index.generations_size() == 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvCompactApply generations_size == 0, no need to compact, key: " << key << "(" << Helper::StringToHex(key)
        << ")";
    return butil::Status::OK();
  }

  pb::coordinator_internal::KvIndexInternal new_kv_index = kv_index;
  new_kv_index.clear_generations();
  std::vector<pb::coordinator_internal::RevisionInternal> revisions_to_delete;

  // for history generations, just filter compaction_revision to delete old revisions
  for (int i = 0; i < kv_index.generations_size() - 1; ++i) {
    pb::coordinator_internal::KvIndexInternal::Generation new_generation;

    const auto &old_generation = kv_index.generations(i);

    if (new_kv_index.generations_size() > 0) {
      new_generation = old_generation;
      *(new_kv_index.mutable_generations()->Add()) = new_generation;
      continue;
    }

    if (old_generation.has_create_revision()) {
      for (const auto &kv_revision : old_generation.revisions()) {
        if (kv_revision.main() < compact_revision.main()) {
          revisions_to_delete.push_back(kv_revision);
        } else {
          *(new_generation.mutable_revisions()->Add()) = kv_revision;
        }
      }

      if (new_generation.revisions_size() > 0) {
        *(new_generation.mutable_create_revision()) = old_generation.create_revision();
        new_generation.set_verison(old_generation.verison());
        *(new_kv_index.mutable_generations()->Add()) = new_generation;
      }
    }
  }

  // for latest generation, retent latest revision
  pb::coordinator_internal::KvIndexInternal::Generation new_generation;
  const auto &latest_generation = kv_index.generations(kv_index.generations_size() - 1);

  // if this is a delete generation, add it to new_kv_index if new_kv_index has more than 1 generation
  // else just delete this kv_index
  if (!latest_generation.has_create_revision()) {
    if (new_kv_index.generations_size() > 0) {
      *(new_kv_index.mutable_generations()->Add()) = latest_generation;
    }
  }

  // if this is a put generation, add it to new_kv_index if new_kv_index has more than 1 generation
  // else filter revisions of this generation
  else {
    if (new_kv_index.generations_size() > 0) {
      *(new_kv_index.mutable_generations()->Add()) = latest_generation;
    } else {
      for (int j = 0; j < latest_generation.revisions_size(); ++j) {
        const auto &kv_revision = latest_generation.revisions(j);

        // if this is the latest revision of the latest generation, just copy it
        if (j == latest_generation.revisions_size() - 1) {
          *(new_generation.add_revisions()) = kv_revision;
        } else {
          if (kv_revision.main() < compact_revision.main()) {
            revisions_to_delete.push_back(kv_revision);
          } else {
            *(new_generation.mutable_revisions()->Add()) = kv_revision;
          }
        }
      }

      if (new_generation.revisions_size() > 0) {
        *(new_generation.mutable_create_revision()) = latest_generation.create_revision();
        new_generation.set_verison(latest_generation.verison());
        *(new_kv_index.mutable_generations()->Add()) = new_generation;
      }
    }
  }

  // if new_kv_index has no generations, delete it
  // else put new_kv_index
  if (new_kv_index.generations_size() == 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvCompactApply new_kv_index has no generations, delete it, key: " << key << "(" << Helper::StringToHex(key)
        << ")";
    DeleteRawKvIndex(key);
  } else {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_kv)
        << "KvCompactApply new_kv_index has generations, put it, key: " << key << "(" << Helper::StringToHex(key)
        << "), new_kv_index: " << new_kv_index.ShortDebugString();
    PutRawKvIndex(key, new_kv_index);
  }

  // delete revisions in kv_rev
  for (const auto &kv_revision : revisions_to_delete) {
    pb::coordinator_internal::KvRevInternal kv_rev;
    kv_rev.set_id(RevisionToString(kv_revision));

    DINGO_LOG(INFO) << "KvCompactApply delete kv_rev, kv_revision: " << kv_revision.ShortDebugString()
                    << ", key: " << key << "(" << Helper::StringToHex(key)
                    << "), kv_rev: " << kv_rev.ShortDebugString();

    DeleteRawKvRev(kv_revision);
  }

  return butil::Status::OK();
}

}  // namespace dingodb
