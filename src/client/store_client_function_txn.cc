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

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bthread/bthread.h"
#include "client/client_helper.h"
#include "client/store_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "serial/buf.h"

const int kBatchSize = 1000;

DECLARE_string(key);
DECLARE_string(value);
DECLARE_int64(limit);
DECLARE_bool(is_reverse);
DECLARE_string(start_key);
DECLARE_string(end_key);
DECLARE_int64(vector_id);
DEFINE_int64(start_ts, 0, "start_ts");
DEFINE_int64(end_ts, 0, "end_ts");
DEFINE_int64(commit_ts, 0, "start_ts");
DEFINE_int64(lock_ts, 0, "start_ts");
DEFINE_int64(lock_ttl, 0, "lock_ttl");
DEFINE_int64(for_update_ts, 0, "for_update_ts");
DEFINE_int64(safe_point_ts, 0, "safe_point_ts");
DEFINE_int64(txn_size, 0, "txn_size");
DEFINE_string(primary_lock, "", "primary_lock");
DEFINE_string(primary_key, "", "primary_key");
DEFINE_int64(advise_lock_ttl, 0, "advise_lock_ttl");
DEFINE_int64(max_ts, 0, "max_ts");
DEFINE_int64(caller_start_ts, 0, "caller_start_ts");
DEFINE_int64(current_ts, 0, "current_ts");
DEFINE_bool(try_one_pc, false, "try_one_pc");
DEFINE_int64(max_commit_ts, 0, "max_commit_ts");
DEFINE_bool(key_only, false, "key_only");
DEFINE_bool(with_start, true, "with_start");
DEFINE_bool(with_end, false, "with_end");
DEFINE_string(mutation_op, "", "mutation_op, [put, delete, putifabsent, lock]");
DEFINE_string(key2, "", "key2");
DEFINE_string(value2, "value2", "value2");
DEFINE_bool(rc, false, "read commited");
DECLARE_int64(dimension);
DEFINE_string(extra_data, "", "extra_data");
DECLARE_bool(key_is_hex);
DECLARE_bool(value_is_hex);

namespace client {
std::string OctalToHex(const std::string& str) {
  std::string hex_str;
  for (std::size_t i = 0; i < str.size(); ++i) {
    if (str[i] == '\\' && i + 3 < str.size() && str[i + 1] >= '0' && str[i + 1] <= '7' && str[i + 2] >= '0' &&
        str[i + 2] <= '7' && str[i + 3] >= '0' && str[i + 3] <= '7') {
      // Convert octal escape sequence to hexadecimal
      int octal_value = (str[i + 1] - '0') * 64 + (str[i + 2] - '0') * 8 + (str[i + 3] - '0');
      hex_str += "\\x" + std::to_string(octal_value / 16) + std::to_string(octal_value % 16);
      //   hex_str += std::to_string(octal_value / 16) + std::to_string(octal_value % 16);
      i += 3;
    } else {
      // Copy non-escape-sequence characters to output
      hex_str += str[i];
    }
  }
  return hex_str;
}

std::string StringToHex(const std::string& key) { return dingodb::Helper::StringToHex(key); }

std::string HexToString(const std::string& hex) { return dingodb::Helper::HexToString(hex); }

std::string VectorPrefixToHex(char prefix, int64_t part_id) {
  std::string key = dingodb::Helper::EncodeVectorIndexRegionHeader(prefix, part_id);
  return dingodb::Helper::StringToHex(key);
}

std::string VectorPrefixToHex(char prefix, int64_t part_id, int64_t vector_id) {
  std::string key = dingodb::Helper::EncodeVectorIndexRegionHeader(prefix, part_id, vector_id);
  return dingodb::Helper::StringToHex(key);
}

std::string TablePrefixToHex(char prefix, const std::string& user_key) {
  std::string key = dingodb::Helper::EncodeTableRegionHeader(prefix, user_key);
  return dingodb::Helper::StringToHex(key);
}

std::string TablePrefixToHex(char prefix, int64_t part_id) {
  std::string key = dingodb::Helper::EncodeTableRegionHeader(prefix, part_id);
  return dingodb::Helper::StringToHex(key);
}

std::string TablePrefixToHex(char prefix, int64_t part_id, const std::string& user_key) {
  std::string key = dingodb::Helper::EncodeTableRegionHeader(prefix, part_id, user_key);
  return dingodb::Helper::StringToHex(key);
}

std::string HexToTablePrefix(const std::string& hex, bool has_part_id) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  char prefix = buf.Read();
  int64_t part_id = 0;
  if (has_part_id) {
    part_id = buf.ReadLong();
  }
  auto user_key_data_size = key.size() - 1;
  if (has_part_id) {
    user_key_data_size = user_key_data_size - 8;
  }
  std::vector<uint8_t> user_key_data;
  user_key_data.reserve(user_key_data_size);
  for (int i = 0; i < user_key_data_size; i++) {
    user_key_data.push_back(buf.Read());
  }
  std::string user_key(user_key_data.begin(), user_key_data.end());

  return std::string(1, prefix) + "_" + std::to_string(part_id) + "_(hex_" + StringToHex(user_key) + ")_(raw_" +
         user_key + ")";
}

std::string HexToVectorPrefix(const std::string& hex) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  char prefix = buf.Read();
  int64_t part_id = buf.ReadLong();
  int64_t vector_id = buf.ReadLong();

  return std::string(1, prefix) + "_" + std::to_string(part_id) + "_" + std::to_string(vector_id);
}

bool TxnGetRegion(int64_t region_id, dingodb::pb::common::Region& region) {
  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(region_id);

  auto status = InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "QueryRegion",
                                                                            query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << query_response.DebugString();

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return false;
  }

  region = query_response.region();
  return true;
}

std::string GetServiceName(const dingodb::pb::common::Region& region) {
  std::string service_name;
  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    exit(-1);
  }

  return service_name;
}

// store

void StoreSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  if (FLAGS_key_is_hex) {
    request.set_primary_lock(HexToString(FLAGS_primary_lock));
  } else {
    request.set_primary_lock(FLAGS_primary_lock);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(FLAGS_lock_ttl);

  if (FLAGS_txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(FLAGS_txn_size);

  request.set_try_one_pc(FLAGS_try_one_pc);
  request.set_max_commit_ts(FLAGS_max_commit_ts);

  if (FLAGS_mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }
  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  std::string key2 = FLAGS_key2;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
    key2 = HexToString(FLAGS_key2);
  }
  if (FLAGS_mutation_op == "put") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);
    mutation->set_key(key);
    if (FLAGS_value_is_hex) {
      mutation->set_value(HexToString(FLAGS_value));
    } else {
      mutation->set_value(FLAGS_value);
    }
    DINGO_LOG(INFO) << "key: " << FLAGS_key << ", value: " << FLAGS_value;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Put);
      mutation->set_key(key2);
      if (FLAGS_value_is_hex) {
        mutation->set_value(HexToString(FLAGS_value));
      } else {
        mutation->set_value(FLAGS_value);
      }
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2 << ", value: " << FLAGS_value;
    }
  } else if (FLAGS_mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(key);
    DINGO_LOG(INFO) << "key: " << FLAGS_key;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Delete);
      mutation->set_key(key2);
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2;
    }
  } else if (FLAGS_mutation_op == "insert") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(key);
    if (FLAGS_value_is_hex) {
      mutation->set_value(HexToString(FLAGS_value));
    } else {
      mutation->set_value(FLAGS_value);
    }
    DINGO_LOG(INFO) << "key: " << FLAGS_key << ", value: " << FLAGS_value;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
      mutation->set_key(key2);
      if (FLAGS_value_is_hex) {
        mutation->set_value(HexToString(FLAGS_value2));
      } else {
        mutation->set_value(FLAGS_value2);
      }
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2 << ", value2: " << FLAGS_value2;
    }
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!FLAGS_extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << FLAGS_extra_data;
  }

  if (FLAGS_for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << FLAGS_for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(FLAGS_for_update_ts);
      for_update_ts_check->set_index(i);

      if (!FLAGS_extra_data.empty()) {
        auto* extra_data = request.add_lock_extra_datas();
        extra_data->set_index(i);
        extra_data->set_extra_data(FLAGS_extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// index
void IndexSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(FLAGS_primary_lock);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(FLAGS_lock_ttl);

  if (FLAGS_txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(FLAGS_txn_size);

  request.set_try_one_pc(FLAGS_try_one_pc);
  request.set_max_commit_ts(FLAGS_max_commit_ts);

  if (FLAGS_mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (FLAGS_vector_id == 0) {
    DINGO_LOG(ERROR) << "vector_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
  int64_t vector_id = FLAGS_vector_id;
  int64_t dimension = 0;

  const auto& para = region.definition().index_parameter().vector_index_parameter();
  if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
    dimension = para.flat_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
    dimension = para.ivf_flat_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
    dimension = para.ivf_pq_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    dimension = para.hnsw_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    dimension = para.diskann_parameter().dimension();
  } else {
    DINGO_LOG(ERROR) << "vector_index_type is empty";
    return;
  }

  if (FLAGS_mutation_op == "put") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);

    mutation->set_key(
        dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0], part_id, vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    *mutation->mutable_vector() = vector_with_id;
  } else if (FLAGS_mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(
        dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0], part_id, vector_id));
  } else if (FLAGS_mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(
        dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0], part_id, vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    *mutation->mutable_vector() = vector_with_id;
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!FLAGS_extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << FLAGS_extra_data;
  }

  if (FLAGS_for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << FLAGS_for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(FLAGS_for_update_ts);
      for_update_ts_check->set_index(i);

      if (!FLAGS_extra_data.empty()) {
        auto* extra_data = request.add_lock_extra_datas();
        extra_data->set_index(i);
        extra_data->set_extra_data(FLAGS_extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// unified

void SendTxnGet(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnGetRequest request;
  dingodb::pb::store::TxnGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  request.set_key(key);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnScan(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  dingodb::pb::common::RangeWithOptions range;
  if (FLAGS_start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = FLAGS_start_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_start_key);
    }
    range.mutable_range()->set_start_key(key);
  }
  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = FLAGS_end_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_end_key);
    }
    range.mutable_range()->set_end_key(key);
  }
  range.set_with_start(FLAGS_with_start);
  range.set_with_end(FLAGS_with_end);
  *request.mutable_range() = range;

  if (FLAGS_limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    return;
  }
  request.set_limit(FLAGS_limit);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  request.set_is_reverse(FLAGS_is_reverse);
  request.set_key_only(FLAGS_key_only);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticLock(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnPessimisticLockRequest request;
  dingodb::pb::store::TxnPessimisticLockResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = FLAGS_primary_lock;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(FLAGS_lock_ttl);

  if (FLAGS_for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(FLAGS_for_update_ts);

  if (FLAGS_mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [lock]";
    return;
  }
  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  if (FLAGS_value.empty()) {
    DINGO_LOG(ERROR) << "value is empty";
    return;
  }
  std::string value = FLAGS_value;
  if (FLAGS_value_is_hex) {
    value = HexToString(FLAGS_value);
  }
  if (FLAGS_mutation_op == "lock") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Lock);
    mutation->set_key(key);
    mutation->set_value(value);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be [lock]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticRollback(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name;
  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    return;
  }

  dingodb::pb::store::TxnPessimisticRollbackRequest request;
  dingodb::pb::store::TxnPessimisticRollbackResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(FLAGS_for_update_ts);

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  *request.add_keys() = key;

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPrewrite(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    StoreSendTxnPrewrite(region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnPrewrite(region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnCommit(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnCommitRequest request;
  dingodb::pb::store::TxnCommitResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_commit_ts == 0) {
    DINGO_LOG(ERROR) << "commit_ts is empty";
    return;
  }
  request.set_commit_ts(FLAGS_commit_ts);

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  request.add_keys()->assign(key);
  DINGO_LOG(INFO) << "key: " << FLAGS_key;

  if (!FLAGS_key2.empty()) {
    std::string key2 = FLAGS_key2;
    if (FLAGS_key_is_hex) {
      key2 = HexToString(FLAGS_key2);
    }
    request.add_keys()->assign(key2);
    DINGO_LOG(INFO) << "key2: " << FLAGS_key2;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnCheckTxnStatus(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnCheckTxnStatusRequest request;
  dingodb::pb::store::TxnCheckTxnStatusResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_primary_key.empty()) {
    DINGO_LOG(ERROR) << "primary_key is empty";
    return;
  } else {
    std::string key = FLAGS_primary_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_primary_key);
    }
    request.set_primary_key(key);
  }

  if (FLAGS_lock_ts == 0) {
    DINGO_LOG(ERROR) << "lock_ts is 0";
    return;
  }
  request.set_lock_ts(FLAGS_lock_ts);

  if (FLAGS_caller_start_ts == 0) {
    DINGO_LOG(ERROR) << "caller_start_ts is 0";
    return;
  }
  request.set_caller_start_ts(FLAGS_caller_start_ts);

  if (FLAGS_current_ts == 0) {
    DINGO_LOG(ERROR) << "current_ts is 0";
    return;
  }
  request.set_current_ts(FLAGS_current_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCheckTxnStatus", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnResolveLock(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnResolveLockRequest request;
  dingodb::pb::store::TxnResolveLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is 0";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "commit_ts is: " << FLAGS_commit_ts;
  if (FLAGS_commit_ts != 0) {
    DINGO_LOG(WARNING) << "commit_ts is not 0, will do commit";
  } else {
    DINGO_LOG(WARNING) << "commit_ts is 0, will do rollback";
  }
  request.set_commit_ts(FLAGS_commit_ts);

  if (FLAGS_key.empty()) {
    DINGO_LOG(INFO) << "key is empty, will do resolve lock for all keys of this transaction";
  } else {
    std::string key = FLAGS_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_key);
    }
    request.add_keys()->assign(key);
    DINGO_LOG(INFO) << "key: " << FLAGS_key;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchGet(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnBatchGetRequest request;
  dingodb::pb::store::TxnBatchGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  if (FLAGS_key2.empty()) {
    DINGO_LOG(ERROR) << "key2 is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  std::string key2 = FLAGS_key2;
  if (FLAGS_key_is_hex) {
    key2 = HexToString(FLAGS_key2);
  }
  request.add_keys()->assign(key);
  request.add_keys()->assign(key2);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchRollback(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnBatchRollbackRequest request;
  dingodb::pb::store::TxnBatchRollbackResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string key = FLAGS_key;
  if (FLAGS_key_is_hex) {
    key = HexToString(FLAGS_key);
  }
  request.add_keys()->assign(key);

  if (!FLAGS_key2.empty()) {
    std::string key2 = FLAGS_key2;
    if (FLAGS_key_is_hex) {
      key2 = HexToString(FLAGS_key2);
    }
    request.add_keys()->assign(key2);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnScanLock(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnScanLockRequest request;
  dingodb::pb::store::TxnScanLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_max_ts == 0) {
    DINGO_LOG(ERROR) << "max_ts is empty";
    return;
  }
  request.set_max_ts(FLAGS_max_ts);

  if (FLAGS_start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = FLAGS_start_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_start_key);
    }
    request.set_start_key(key);
  }

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = FLAGS_end_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_end_key);
    }
    request.set_end_key(key);
  }

  if (FLAGS_limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    return;
  }
  request.set_limit(FLAGS_limit);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnHeartBeat(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnHeartBeatRequest request;
  dingodb::pb::store::TxnHeartBeatResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = FLAGS_primary_lock;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_advise_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "advise_lock_ttl is empty";
    return;
  }
  request.set_advise_lock_ttl(FLAGS_advise_lock_ttl);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnHeartBeat", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnGc(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnGcRequest request;
  dingodb::pb::store::TxnGcResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_safe_point_ts == 0) {
    DINGO_LOG(ERROR) << "safe_point_ts is empty";
    return;
  }
  request.set_safe_point_ts(FLAGS_safe_point_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGc", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnDeleteRange(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnDeleteRangeRequest request;
  dingodb::pb::store::TxnDeleteRangeResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = FLAGS_start_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_start_key);
    }
    request.set_start_key(key);
  }

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = FLAGS_end_key;
    if (FLAGS_key_is_hex) {
      key = HexToString(FLAGS_end_key);
    }
    request.set_end_key(key);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnDump(int64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnDumpRequest request;
  dingodb::pb::store::TxnDumpResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (FLAGS_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (FLAGS_start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  }
  std::string start_key = FLAGS_start_key;

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  std::string end_key = FLAGS_end_key;

  if (FLAGS_key_is_hex) {
    start_key = dingodb::Helper::HexToString(start_key);
    end_key = dingodb::Helper::HexToString(end_key);
  }

  request.set_start_key(start_key);
  request.set_end_key(end_key);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  if (FLAGS_end_ts == 0) {
    DINGO_LOG(ERROR) << "end_ts is empty";
    return;
  }
  request.set_end_ts(FLAGS_end_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDump", request, response);

  if (response.has_error()) {
    DINGO_LOG(ERROR) << "Response ERROR: " << response.error().ShortDebugString();
  }
  if (response.has_txn_result()) {
    DINGO_LOG(INFO) << "Response TxnResult: " << response.txn_result().ShortDebugString();
  }

  DINGO_LOG(INFO) << "data_size: " << response.data_keys_size();
  DINGO_LOG(INFO) << "lock_size: " << response.lock_keys_size();
  DINGO_LOG(INFO) << "write_size: " << response.write_keys_size();

  for (int i = 0; i < response.data_keys_size(); i++) {
    DINGO_LOG(INFO) << "data[" << i << "] hex_key: [" << StringToHex(response.data_keys(i).key()) << "] key: ["
                    << response.data_keys(i).ShortDebugString() << "], value: ["
                    << response.data_values(i).ShortDebugString() << "]";
  }

  for (int i = 0; i < response.lock_keys_size(); i++) {
    DINGO_LOG(INFO) << "lock[" << i << "] hex_key: [" << StringToHex(response.lock_keys(i).key()) << "] key: ["
                    << response.lock_keys(i).ShortDebugString() << "], value: ["
                    << response.lock_values(i).ShortDebugString() << "]";
  }

  for (int i = 0; i < response.write_keys_size(); i++) {
    DINGO_LOG(INFO) << "write[" << i << "] hex_key: [" << StringToHex(response.write_keys(i).key()) << "] key: ["
                    << response.write_keys(i).ShortDebugString() << "], value: ["
                    << response.write_values(i).ShortDebugString() << "]";
  }
}

}  // namespace client
