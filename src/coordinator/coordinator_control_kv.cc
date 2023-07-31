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
#include "serial/buf.h"

namespace dingodb {

DEFINE_uint32(max_kv_put_count, 1000, "max kv put count");
DEFINE_uint32(max_kv_key_size, 4096, "max kv put count");
DEFINE_uint32(max_kv_value_size, 4096, "max kv put count");

std::string CoordinatorControl::RevisionToString(const pb::coordinator_internal::RevisionInternal &revision) {
  Buf buf(17);
  buf.WriteLong(revision.main());
  buf.Write('_');
  buf.WriteLong(revision.sub());

  std::string result;
  buf.GetBytes(result);

  return result;
}

pb::coordinator_internal::RevisionInternal CoordinatorControl::StringToRevision(const std::string &input_string) {
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

butil::Status CoordinatorControl::GetRawKvIndex(const std::string &key,
                                                pb::coordinator_internal::KvIndexInternal &kv_index) {
  auto ret = this->kv_index_map_.Get(key, kv_index);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRawKvIndex failed, key:[" << key << "]";
  }
  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                              pb::coordinator_internal::KvRevInternal &kv_rev) {
  auto ret = this->kv_rev_map_.Get(RevisionToString(revision), kv_rev);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRawKvRev failed, revision:[" << revision.ShortDebugString() << "]";
  }
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
// return: errno
butil::Status CoordinatorControl::KvRange(const std::string &key, const std::string &range_end, int64_t limit,
                                          bool keys_only, bool count_only, std::vector<pb::version::Kv> &kv,
                                          uint64_t &total_count_in_range) {
  DINGO_LOG(INFO) << "KvRange, key: " << key << ", range_end: " << range_end << ", limit: " << limit
                  << ", keys_only: " << keys_only << ", count_only: " << count_only;

  std::vector<pb::coordinator_internal::KvIndexInternal> kv_index_values;

  if (range_end.empty()) {
    pb::coordinator_internal::KvIndexInternal kv_index;
    auto ret = this->GetRawKvIndex(key, kv_index);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange GetRawKvIndex failed, key: " << key << ", error: " << ret.error_str();
      return ret;
    }
    kv_index_values.push_back(kv_index);
  } else {
    // scan kv_index for legal keys
    if (this->kv_index_map_.GetAllValues(
            kv_index_values, [&key, &range_end](pb::coordinator_internal::KvIndexInternal version_kv) -> bool {
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
              } else if (range_end == std::to_string('\0')) {
                return version_kv.id().compare(key) >= 0;
              } else {
                return version_kv.id().compare(key) >= 0 && version_kv.id().compare(range_end) < 0;
              }
            }) < 0) {
      DINGO_LOG(FATAL) << "OnLeaderStart kv_index_map_.GetAllValues failed";
    }
  }

  if (count_only) {
    total_count_in_range = kv_index_values.size();
    DINGO_LOG(INFO) << "KvRange count_only, total_count_in_range: " << total_count_in_range;
    return butil::Status::OK();
  }

  // query kv_rev for values
  uint32_t limit_count = 0;
  for (const auto &kv_index_value : kv_index_values) {
    auto generation_count = kv_index_value.generations_size();
    if (generation_count == 0) {
      continue;
    }
    const auto &latest_generation = kv_index_value.generations(generation_count - 1);
    if (!latest_generation.has_create_revision() || latest_generation.revisions_size() == 0) {
      continue;
    }

    pb::coordinator_internal::KvRevInternal kv_rev;
    std::string revision_string = RevisionToString(kv_index_value.mod_revision());
    auto ret = this->kv_rev_map_.Get(revision_string, kv_rev);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "kv_rev_map_.Get failed, revision_string: " << revision_string;
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

    // add to output
    kv.push_back(kv_temp);

    limit_count++;
    if (limit_count >= limit) {
      break;
    }
  }

  DINGO_LOG(INFO) << "KvRange finish, key: " << key << ", range_end: " << range_end << ", limit: " << limit
                  << ", keys_only: " << keys_only << ", count_only: " << count_only << ", kv size: " << kv.size()
                  << ", total_count_in_range: " << total_count_in_range;

  return butil::Status::OK();
}

// KvPut is the put function
// in:  key_values
// in:  lease_id
// in:  prev_kv
// in:  igore_value
// in:  ignore_lease
// out:  prev_kvs
// out:  revision
// return: errno
butil::Status CoordinatorControl::KvPut(const std::vector<pb::common::KeyValue> &key_values, uint64_t lease_id,
                                        bool prev_kv, bool igore_value, bool ignore_lease,
                                        std::vector<pb::version::Kv> &prev_kvs, uint64_t &revision,
                                        pb::coordinator_internal::MetaIncrement &meta_increment) {
  DINGO_LOG(INFO) << "KvPut, key_values size: " << key_values.size() << ", lease_id: " << lease_id
                  << ", prev_kv: " << prev_kv << ", igore_value: " << igore_value << ", ignore_lease: " << ignore_lease;

  // check lease
  if (!ignore_lease && lease_id != 0) {
    std::set<std::string> keys;
    int64_t granted_ttl = 0;
    int64_t remaining_ttl = 0;

    auto ret = this->LeaseQuery(lease_id, false, granted_ttl, remaining_ttl, keys);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvPut LeaseQuery failed, lease_id: " << lease_id << ", error: " << ret.error_str();
      return ret;
    }
  }

  if (key_values.size() > FLAGS_max_kv_put_count) {
    DINGO_LOG(ERROR) << "KvPut key_values size is too large, max_kv_put_count: " << FLAGS_max_kv_put_count
                     << ", key_values size: " << key_values.size();
    return butil::Status(EINVAL, "KvPut key_values size is too large");
  }

  for (const auto &key_value_in : key_values) {
    // check key
    if (key_value_in.key().empty()) {
      DINGO_LOG(ERROR) << "KvPut key is empty";
      return butil::Status(EINVAL, "KvPut key is empty");
    }

    // check value
    if (!igore_value && key_value_in.value().empty()) {
      DINGO_LOG(ERROR) << "KvPut value is empty";
      return butil::Status(EINVAL, "KvPut value is empty");
    }

    // check key length
    if (key_value_in.key().size() > FLAGS_max_kv_key_size) {
      DINGO_LOG(ERROR) << "KvPut key is too long, max_kv_key_size: " << FLAGS_max_kv_key_size
                       << ", key: " << key_value_in.key();
      return butil::Status(EINVAL, "KvPut key is too long");
    }

    // check value length
    if (!igore_value && key_value_in.value().size() > FLAGS_max_kv_value_size) {
      DINGO_LOG(ERROR) << "KvPut value is too long, max_kv_value_size: " << FLAGS_max_kv_value_size
                       << ", key: " << key_value_in.key();
      return butil::Status(EINVAL, "KvPut value is too long");
    }
  }

  // do kv_put
  uint64_t sub_revision = 0;
  revision = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment);
  for (const auto &key_value_in : key_values) {
    if (prev_kv) {
      std::vector<pb::version::Kv> kvs_temp;
      uint64_t total_count_in_range = 0;
      this->KvRange(key_value_in.key(), std::string(), 1, false, false, kvs_temp, total_count_in_range);
      if (!kvs_temp.empty()) {
        prev_kvs.push_back(kvs_temp[0]);
      } else {
        pb::version::Kv kv_temp;
        prev_kvs.push_back(kv_temp);
      }
    }

    // update kv_index
    pb::coordinator_internal::KvIndexInternal kv_index;
    auto ret = this->GetRawKvIndex(key_value_in.key(), kv_index);
    if (!ret.ok()) {
      DINGO_LOG(INFO) << "KvPut will create new key: " << key_value_in.key() << ", error_str: " << ret.error_str();

      kv_index.set_id(key_value_in.key());
      kv_index.mutable_mod_revision()->set_main(revision);
      kv_index.mutable_mod_revision()->set_sub(sub_revision);

      auto *new_generation = kv_index.add_generations();
      new_generation->mutable_create_revision()->set_main(revision);
      new_generation->mutable_create_revision()->set_sub(sub_revision);
      auto *new_revision = new_generation->add_revisions();
      new_revision->set_main(revision);
      new_revision->set_sub(sub_revision);

      ++sub_revision;

      // add meta_increment
      auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
      kv_index_meta_increment->set_id(key_value_in.key());
      kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
      kv_index_meta_increment->mutable_kv_index()->Swap(&kv_index);

    } else {
      DINGO_LOG(INFO) << "KvPut will update old key: " << key_value_in.key() << ", error_str: " << ret.error_str();

      auto generation_count = kv_index.generations_size();
      if (generation_count == 0) {
        DINGO_LOG(ERROR) << "KvPut generation_count is 0, key: " << key_value_in.key();
        return butil::Status(EINVAL, "KvPut generation_count is 0");
      }

      bool is_a_deleted_key = false;

      const auto &latest_generation = kv_index.generations(generation_count - 1);
      if (!latest_generation.has_create_revision() || latest_generation.revisions_size() == 0) {
        is_a_deleted_key = true;
      }

      kv_index.mutable_mod_revision()->set_main(revision);
      kv_index.mutable_mod_revision()->set_sub(sub_revision);

      auto *new_generation = kv_index.add_generations();

      if (is_a_deleted_key) {
        new_generation->mutable_create_revision()->set_main(revision);
        new_generation->mutable_create_revision()->set_sub(sub_revision);
        new_generation->set_verison(1L);
      } else {
        new_generation->mutable_create_revision()->set_main(latest_generation.create_revision().main());
        new_generation->mutable_create_revision()->set_sub(latest_generation.create_revision().sub());
        new_generation->set_verison(latest_generation.verison() + 1);
      }

      auto *new_revision = new_generation->add_revisions();
      new_revision->set_main(revision);
      new_revision->set_sub(sub_revision);

      ++sub_revision;

      // add meta_increment
      auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
      kv_index_meta_increment->set_id(key_value_in.key());
      kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
      kv_index_meta_increment->mutable_kv_index()->Swap(&kv_index);
      kv_index_meta_increment->set_event_type(
          ::dingodb::pb::coordinator_internal::MetaIncrementKvIndex_KvIndexEventType::
              MetaIncrementKvIndex_KvIndexEventType_KV_INDEX_EVENT_TYPE_PUT);
    }
  }
  return butil::Status::OK();
}

// KvDeleteRange is the delete function
// in:  key
// in:  range_end
// in:  prev_key
// out:  prev_kvs
// out:  revision
// return: errno
butil::Status CoordinatorControl::KvDeleteRange(const std::string &key, const std::string &range_end, bool prev_key,
                                                std::vector<pb::version::Kv> &prev_kvs, uint64_t &revision,
                                                pb::coordinator_internal::MetaIncrement &meta_increment) {
  return butil::Status::OK();
}

}  // namespace dingodb
