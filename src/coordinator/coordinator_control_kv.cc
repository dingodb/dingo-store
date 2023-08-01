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
    DINGO_LOG(WARNING) << "GetRawKvIndex not found, key:[" << key << "]";
    return butil::Status(EINVAL, "GetRawKvIndex not found");
  }
  return butil::Status::OK();
}

butil::Status CoordinatorControl::RangeRawKvIndex(
    const std::string &key, const std::string &range_end,
    std::vector<pb::coordinator_internal::KvIndexInternal> &kv_index_values) {
  // scan kv_index for legal keys
  auto ret = this->kv_index_map_.GetAllValues(
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
      });

  if (ret < 0) {
    DINGO_LOG(WARNING) << "RangeRawKvIndex failed, key:[" << key << "]";
    return butil::Status(EINVAL, "RangeRawKvIndex failed");
  } else {
    return butil::Status::OK();
  }
}

butil::Status CoordinatorControl::PutRawKvIndex(const std::string &key,
                                                const pb::coordinator_internal::KvIndexInternal &kv_index) {
  auto ret = this->kv_index_map_.Put(key, kv_index);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "PutRawKvIndex failed, key:[" << key << "]";
  }

  std::vector<pb::common::KeyValue> meta_write_to_kv;
  meta_write_to_kv.push_back(kv_index_meta_->TransformToKvValue(kv_index));
  meta_writer_->Put(meta_write_to_kv);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteRawKvIndex(const std::string &key,
                                                   const pb::coordinator_internal::KvIndexInternal &kv_index) {
  auto ret = this->kv_index_map_.Erase(key);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "DeleteRawKvIndex failed, key:[" << key << "]";
  }

  auto kv_to_delete = kv_index_meta_->TransformToKvValue(kv_index);
  meta_writer_->Delete(kv_to_delete.key());

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                              pb::coordinator_internal::KvRevInternal &kv_rev) {
  auto ret = this->kv_rev_map_.Get(RevisionToString(revision), kv_rev);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRawKvRev not found, revision:[" << revision.ShortDebugString() << "]";
    return butil::Status(EINVAL, "GetRawKvRev not found");
  }
  return butil::Status::OK();
}

butil::Status CoordinatorControl::PutRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                              const pb::coordinator_internal::KvRevInternal &kv_rev) {
  auto ret = this->kv_rev_map_.Put(RevisionToString(revision), kv_rev);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "PutRawKvRev failed, revision:[" << revision.ShortDebugString() << "]";
  }

  DINGO_LOG(INFO) << "PutRawKvRev success, revision:[" << revision.ShortDebugString() << "], kv_rev:["
                  << kv_rev.ShortDebugString() << "]"
                  << " kv_rev.id: " << Helper::StringToHex(kv_rev.id())
                  << ", revision_string: " << Helper::StringToHex(RevisionToString(revision));

  std::vector<pb::common::KeyValue> meta_write_to_kv;
  meta_write_to_kv.push_back(kv_rev_meta_->TransformToKvValue(kv_rev));
  meta_writer_->Put(meta_write_to_kv);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                                                 const pb::coordinator_internal::KvRevInternal &kv_rev) {
  auto ret = this->kv_rev_map_.Erase(RevisionToString(revision));
  if (ret < 0) {
    DINGO_LOG(WARNING) << "DeleteRawKvRev failed, revision:[" << revision.ShortDebugString() << "]";
  }
  auto kv_to_delete = kv_rev_meta_->TransformToKvValue(kv_rev);
  meta_writer_->Delete(kv_to_delete.key());

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

  if (limit == 0) {
    limit = INT64_MAX;
  }

  std::vector<pb::coordinator_internal::KvIndexInternal> kv_index_values;

  if (range_end.empty()) {
    pb::coordinator_internal::KvIndexInternal kv_index;
    auto ret = this->GetRawKvIndex(key, kv_index);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange GetRawKvIndex not found, key: " << key << ", error: " << ret.error_str();
      return butil::Status::OK();
    }
    kv_index_values.push_back(kv_index);
  } else {
    // scan kv_index for legal keys
    auto ret = RangeRawKvIndex(key, range_end, kv_index_values);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "KvRange kv_index_map_.RangeRawKvIndex failed";
      return ret;
    }
  }

  if (count_only) {
    DINGO_LOG(INFO) << "KvRange count_only, total_count_in_range: " << total_count_in_range;
    return butil::Status::OK();
  }

  // query kv_rev for values
  uint32_t limit_count = 0;
  for (const auto &kv_index_value : kv_index_values) {
    auto generation_count = kv_index_value.generations_size();
    if (generation_count == 0) {
      DINGO_LOG(INFO) << "KvRange generation_count is 0, key: " << key;
      continue;
    }
    const auto &latest_generation = kv_index_value.generations(generation_count - 1);
    if (!latest_generation.has_create_revision() || latest_generation.revisions_size() == 0) {
      DINGO_LOG(INFO) << "KvRange latest_generation is empty, key: " << key;
      continue;
    }

    limit_count++;
    if (limit_count > limit) {
      continue;
    }

    pb::coordinator_internal::KvRevInternal kv_rev;
    auto ret = GetRawKvRev(kv_index_value.mod_revision(), kv_rev);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "kv_rev_map_.Get failed, revision: " << kv_index_value.mod_revision().ShortDebugString()
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

    DINGO_LOG(INFO) << "KvRange will return kv: " << kv_temp.ShortDebugString();

    // add to output
    kv.push_back(kv_temp);
  }

  total_count_in_range = limit_count;

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
                                        uint64_t &lease_grant_id,
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

  lease_grant_id = lease_id;

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
  uint64_t sub_revision = 1;
  revision = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment);

  for (const auto &key_value_in : key_values) {
    if (prev_kv) {
      std::vector<pb::version::Kv> kvs_temp;
      uint64_t total_count_in_range = 0;
      this->KvRange(key_value_in.key(), std::string(), 1, false, false, kvs_temp, total_count_in_range);
      if (!kvs_temp.empty()) {
        prev_kvs.push_back(kvs_temp[0]);
        if (ignore_lease) {
          lease_grant_id = kvs_temp[0].lease();
        } else if (kvs_temp[0].lease() != lease_grant_id) {
          DINGO_LOG(ERROR) << "KvPut lease_grant_id is not equal, lease_grant_id: " << lease_grant_id
                           << ", kvs_temp[0].lease(): " << kvs_temp[0].lease();
          return butil::Status(EINVAL, "KvPut lease_grant_id is not equal");
        }
      } else {
        pb::version::Kv kv_temp;
        prev_kvs.push_back(kv_temp);
      }
    }

    // update kv_index
    DINGO_LOG(INFO) << "KvPut will put key: " << key_value_in.key();

    // add meta_increment
    auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
    kv_index_meta_increment->set_id(key_value_in.key());
    kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    kv_index_meta_increment->set_event_type(pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_PUT);
    kv_index_meta_increment->mutable_op_revision()->set_main(revision);
    kv_index_meta_increment->mutable_op_revision()->set_sub(sub_revision);
    kv_index_meta_increment->set_ignore_lease(ignore_lease);
    kv_index_meta_increment->set_lease_id(lease_grant_id);
    if (!ignore_lease) {
      kv_index_meta_increment->set_ignore_value(igore_value);
    }
    if (!igore_value) {
      kv_index_meta_increment->set_value(key_value_in.value());
    }

    ++sub_revision;
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
  DINGO_LOG(INFO) << "KvDeleteRange, key: " << key << ", range_end: " << range_end << ", prev_key: " << prev_key;

  std::vector<pb::version::Kv> kvs_to_delete;
  uint64_t total_count_in_range = 0;

  bool key_only = !prev_key;

  auto ret = KvRange(key, range_end, INT64_MAX, key_only, false, kvs_to_delete, total_count_in_range);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteRange KvRange failed, key: " << key << ", range_end: " << range_end
                     << ", error: " << ret.error_str();
    return ret;
  }

  // do kv_delete
  uint64_t sub_revision = 1;
  revision = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment);

  for (const auto &kv_to_delete : kvs_to_delete) {
    // update kv_index
    DINGO_LOG(INFO) << "KvDelete will delete key: " << kv_to_delete.kv().key();

    // add meta_increment
    auto *kv_index_meta_increment = meta_increment.add_kv_indexes();
    kv_index_meta_increment->set_id(kv_to_delete.kv().key());
    kv_index_meta_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    kv_index_meta_increment->set_event_type(pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_DELETE);
    kv_index_meta_increment->mutable_op_revision()->set_main(revision);
    kv_index_meta_increment->mutable_op_revision()->set_sub(sub_revision);

    ++sub_revision;
  }

  if (prev_key) {
    prev_kvs.swap(kvs_to_delete);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::KvPutApply(const std::string &key,
                                             const pb::coordinator_internal::RevisionInternal &op_revision,
                                             bool ignore_lease, uint64_t lease_id, bool ignore_value,
                                             const std::string &value) {
  DINGO_LOG(INFO) << "KvPutApply, key: " << key << ", op_revision: " << op_revision.ShortDebugString()
                  << ", ignore_lease: " << ignore_lease << ", lease_id: " << lease_id
                  << ", ignore_value: " << ignore_value << ", value: " << value;

  // get kv_index and generate new kv_index
  pb::coordinator_internal::KvIndexInternal kv_index;
  pb::coordinator_internal::RevisionInternal last_mod_revision;
  pb::coordinator_internal::RevisionInternal new_create_revision;
  new_create_revision.set_main(op_revision.main());
  new_create_revision.set_sub(op_revision.sub());
  uint64_t new_version = 1;

  auto ret = this->GetRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(INFO) << "KvPutApply GetRawKvIndex not found, will create key: " << key << ", error: " << ret.error_str();
    kv_index.set_id(key);
    kv_index.mutable_mod_revision()->set_main(op_revision.main());
    kv_index.mutable_mod_revision()->set_sub(op_revision.sub());
    auto *generation = kv_index.add_generations();
    generation->mutable_create_revision()->set_main(op_revision.main());
    generation->mutable_create_revision()->set_sub(op_revision.sub());
    generation->set_verison(1);
    generation->add_revisions()->CopyFrom(op_revision);

    DINGO_LOG(INFO) << "KvPutApply kv_index create new kv_index: " << generation->ShortDebugString();
  } else {
    DINGO_LOG(INFO) << "KvPutApply GetRawKvIndex found, will update key: " << key << ", error: " << ret.error_str();

    last_mod_revision = kv_index.mod_revision();

    if (kv_index.generations_size() == 0) {
      auto *generation = kv_index.add_generations();
      generation->mutable_create_revision()->set_main(op_revision.main());
      generation->mutable_create_revision()->set_sub(op_revision.sub());
      generation->set_verison(1);
      generation->add_revisions()->CopyFrom(op_revision);
      DINGO_LOG(INFO) << "KvPutApply kv_index add generation: " << generation->ShortDebugString();
    } else {
      // auto &latest_generation = *kv_index.mutable_generations()->rbegin();
      auto *latest_generation = kv_index.mutable_generations(kv_index.generations_size() - 1);
      if (latest_generation->has_create_revision()) {
        latest_generation->add_revisions()->CopyFrom(op_revision);
        latest_generation->set_verison(latest_generation->verison() + 1);
        DINGO_LOG(INFO) << "KvPutApply latest_generation add revsion: " << latest_generation->ShortDebugString();
      } else {
        latest_generation->mutable_create_revision()->set_main(op_revision.main());
        latest_generation->mutable_create_revision()->set_sub(op_revision.sub());
        latest_generation->set_verison(1);
        latest_generation->add_revisions()->CopyFrom(op_revision);
        DINGO_LOG(INFO) << "KvPutApply latest_generation create revsion: " << latest_generation->ShortDebugString();
      }

      // setup new_create_revision to last create_revision
      new_create_revision.set_main(latest_generation->create_revision().main());
      new_create_revision.set_sub(latest_generation->create_revision().sub());

      // setup new_version
      new_version = latest_generation->verison();
    }
    kv_index.mutable_mod_revision()->CopyFrom(op_revision);
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

  // do real write to state machine
  ret = this->PutRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvPutApply PutRawKvIndex failed, key: " << key << ", error: " << ret.error_str();
  }
  DINGO_LOG(INFO) << "KvPutApply PutRawKvIndex success, key: " << key << ", kv_index: " << kv_index.ShortDebugString();

  ret = this->PutRawKvRev(op_revision, kv_rev);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvPutApply PutRawKvRev failed, revision: " << op_revision.ShortDebugString()
                     << ", error: " << ret.error_str();
    return ret;
  }
  DINGO_LOG(INFO) << "KvPutApply PutRawKvRev success, revision: " << op_revision.ShortDebugString()
                  << ", kv_rev: " << kv_rev.ShortDebugString();

  DINGO_LOG(INFO) << "KvPutApply success, key: " << key << ", op_revision: " << op_revision.ShortDebugString()
                  << ", ignore_lease: " << ignore_lease << ", lease_id: " << lease_id
                  << ", ignore_value: " << ignore_value << ", value: " << value;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::KvDeleteApply(const std::string &key,
                                                const pb::coordinator_internal::RevisionInternal &op_revision) {
  DINGO_LOG(INFO) << "KvDeleteApply, key: " << key << ", revision: " << op_revision.ShortDebugString();

  // get kv_index and generate new kv_index
  pb::coordinator_internal::KvIndexInternal kv_index;
  pb::coordinator_internal::RevisionInternal last_mod_revision;
  pb::coordinator_internal::RevisionInternal new_create_revision;
  new_create_revision.set_main(op_revision.main());
  new_create_revision.set_sub(op_revision.sub());
  uint64_t new_version = 1;

  auto ret = this->GetRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(INFO) << "KvDeleteApply GetRawKvIndex not found, no need to delete: " << key
                    << ", error: " << ret.error_str();
    return butil::Status::OK();
  } else {
    DINGO_LOG(INFO) << "KvDeleteApply GetRawKvIndex found, will delete key: " << key << ", error: " << ret.error_str();

    last_mod_revision = kv_index.mod_revision();

    if (kv_index.generations_size() == 0) {
      // create a null generator means delete
      auto *generation = kv_index.add_generations();
      DINGO_LOG(INFO) << "KvDeleteApply kv_index add null generation[0]: " << generation->ShortDebugString();
    } else {
      // auto &latest_generation = *kv_index.mutable_generations()->rbegin();
      auto *latest_generation = kv_index.mutable_generations(kv_index.generations_size() - 1);
      if (latest_generation->has_create_revision()) {
        // add a the delete revision to latest generation
        latest_generation->add_revisions()->CopyFrom(op_revision);
        latest_generation->set_verison(latest_generation->verison() + 1);

        // create a null generator means delete
        auto *generation = kv_index.add_generations();
        DINGO_LOG(INFO) << "KvDeleteApply kv_index add null generation[1]: " << generation->ShortDebugString();
      } else {
        // a null generation means delete
        // so we do not need to add a new generation
        DINGO_LOG(INFO) << "KvDeleteApply kv_index exist null generation[1], nothing to do: "
                        << latest_generation->ShortDebugString();
      }

      // setup new_create_revision to last create_revision
      new_create_revision.set_main(latest_generation->create_revision().main());
      new_create_revision.set_sub(latest_generation->create_revision().sub());

      // setup new_version
      new_version = latest_generation->verison();
    }
    kv_index.mutable_mod_revision()->CopyFrom(op_revision);
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
  ret = this->PutRawKvIndex(key, kv_index);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteApply PutRawKvIndex failed, key: " << key << ", error: " << ret.error_str();
  }

  ret = this->PutRawKvRev(op_revision, kv_rev);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "KvDeleteApply PutRawKvRev failed, revision: " << op_revision.ShortDebugString()
                     << ", error: " << ret.error_str();
    return ret;
  }

  DINGO_LOG(INFO) << "KvDeleteApply success, key: " << key << ", revision: " << op_revision.ShortDebugString();

  return butil::Status::OK();
}

butil::Status CoordinatorControl::KvCompactApply(const std::string &key,  // NOLINT
                                                 const pb::coordinator_internal::RevisionInternal &op_revision) {
  DINGO_LOG(INFO) << "KvCompactApply, key: " << key << ", revision: " << op_revision.ShortDebugString();
  return butil::Status::OK();
}

}  // namespace dingodb
