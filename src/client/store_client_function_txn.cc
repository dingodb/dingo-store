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
DECLARE_uint64(limit);
DECLARE_bool(is_reverse);
DECLARE_string(start_key);
DECLARE_string(end_key);
DECLARE_uint64(vector_id);
DEFINE_uint64(start_ts, 0, "start_ts");
DEFINE_uint64(end_ts, 0, "end_ts");
DEFINE_uint64(commit_ts, 0, "start_ts");
DEFINE_uint64(lock_ts, 0, "start_ts");
DEFINE_uint64(lock_ttl, 0, "lock_ttl");
DEFINE_uint64(for_update_ts, 0, "for_update_ts");
DEFINE_uint64(safe_point_ts, 0, "safe_point_ts");
DEFINE_uint64(txn_size, 0, "txn_size");
DEFINE_string(primary_lock, "", "primary_lock");
DEFINE_string(primary_key, "", "primary_key");
DEFINE_uint64(advise_lock_ttl, 0, "advise_lock_ttl");
DEFINE_uint64(max_ts, 0, "max_ts");
DEFINE_uint64(caller_start_ts, 0, "caller_start_ts");
DEFINE_uint64(current_ts, 0, "current_ts");
DEFINE_bool(try_one_pc, false, "try_one_pc");
DEFINE_uint64(max_commit_ts, 0, "max_commit_ts");
DEFINE_bool(key_only, false, "key_only");
DEFINE_bool(with_start, true, "with_start");
DEFINE_bool(with_end, false, "with_end");
DEFINE_string(mutation_op, "", "mutation_op, [put, delete, putifabsent, lock]");
DEFINE_string(key2, "", "key2");
DEFINE_bool(rc, false, "read commited");
DECLARE_int64(dimension);
DEFINE_string(extra_data, "", "extra_data");

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

std::string VectorPrefixToHex(uint64_t part_id, uint64_t vector_id) {
  std::string key = dingodb::Helper::EncodeVectorIndexRegionHeader(part_id, vector_id);
  return dingodb::Helper::StringToHex(key);
}

std::string HexToVectorPrefix(const std::string& hex) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  uint64_t part_id = buf.ReadLong();
  uint64_t vector_id = buf.ReadLong();

  return std::to_string(part_id) + "_" + std::to_string(vector_id);
}

bool TxnGetRegion(uint64_t region_id, dingodb::pb::common::Region& region) {
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

// store

void StoreSendTxnGet(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.set_key(FLAGS_key);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnScan(uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

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
  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  dingodb::pb::common::RangeWithOptions range;
  range.mutable_range()->set_start_key(FLAGS_start_key);
  range.mutable_range()->set_end_key(FLAGS_end_key);
  range.set_with_start(FLAGS_with_start);
  range.set_with_end(FLAGS_with_end);

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

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnPrewrite(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  if (FLAGS_mutation_op == "put") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);
    mutation->set_key(FLAGS_key);
    mutation->set_value(FLAGS_value);
    DINGO_LOG(INFO) << "key: " << FLAGS_key << ", value: " << FLAGS_value;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Put);
      mutation->set_key(FLAGS_key2);
      mutation->set_value(FLAGS_value);
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2 << ", value: " << FLAGS_value;
    }
  } else if (FLAGS_mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(FLAGS_key);
    DINGO_LOG(INFO) << "key: " << FLAGS_key;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Delete);
      mutation->set_key(FLAGS_key2);
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2;
    }
  } else if (FLAGS_mutation_op == "insert") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(FLAGS_key);
    mutation->set_value(FLAGS_value);
    DINGO_LOG(INFO) << "key: " << FLAGS_key << ", value: " << FLAGS_value;

    if (!FLAGS_key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
      mutation->set_key(FLAGS_key2);
      mutation->set_value(FLAGS_value);
      DINGO_LOG(INFO) << "key2: " << FLAGS_key2 << ", value: " << FLAGS_value;
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

void StoreSendTxnBatchGet(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.add_keys()->assign(FLAGS_key);
  request.add_keys()->assign(FLAGS_key2);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnDump(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.set_start_key(FLAGS_start_key);

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  request.set_end_key(FLAGS_end_key);

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

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnDump", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// index
void IndexSendTxnGet(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.set_key(FLAGS_key);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnScan(uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

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
  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  dingodb::pb::common::RangeWithOptions range;
  range.mutable_range()->set_start_key(FLAGS_start_key);
  range.mutable_range()->set_end_key(FLAGS_end_key);
  range.set_with_start(FLAGS_with_start);
  range.set_with_end(FLAGS_with_end);

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

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnPrewrite(uint64_t region_id, const dingodb::pb::common::Region& region) {
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

  uint64_t part_id = region.definition().part_id();
  uint64_t vector_id = FLAGS_vector_id;
  uint64_t dimension = 0;

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

    mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(part_id, vector_id));

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
    mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(part_id, vector_id));
  } else if (FLAGS_mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);

    mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(part_id, vector_id));

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

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnBatchGet(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.add_keys()->assign(FLAGS_key);
  request.add_keys()->assign(FLAGS_key2);

  if (FLAGS_start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(FLAGS_start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnDump(uint64_t region_id, const dingodb::pb::common::Region& region) {
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
  request.set_start_key(FLAGS_start_key);

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  request.set_end_key(FLAGS_end_key);

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

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnDump", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// unified

void SendTxnGet(uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    StoreSendTxnGet(region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnGet(region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnScan(uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    StoreSendTxnScan(region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnScan(region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnPessimisticLock(uint64_t region_id) {
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
  if (FLAGS_value.empty()) {
    DINGO_LOG(ERROR) << "value is empty";
    return;
  }
  if (FLAGS_mutation_op == "lock") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Lock);
    mutation->set_key(FLAGS_key);
    mutation->set_value(FLAGS_value);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be [lock]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticRollback(uint64_t region_id) {
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
  *request.add_keys() = FLAGS_key;

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPrewrite(uint64_t region_id) {
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

void SendTxnCommit(uint64_t region_id) {
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
  request.add_keys()->assign(FLAGS_key);
  DINGO_LOG(INFO) << "key: " << FLAGS_key;

  if (!FLAGS_key2.empty()) {
    request.add_keys()->assign(FLAGS_key2);
    DINGO_LOG(INFO) << "key2: " << FLAGS_key2;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnCheckTxnStatus(uint64_t region_id) {
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
  }
  request.set_primary_key(FLAGS_primary_key);

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

void SendTxnResolveLock(uint64_t region_id) {
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
    request.add_keys()->assign(FLAGS_key);
    DINGO_LOG(INFO) << "key: " << FLAGS_key;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchGet(uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    StoreSendTxnBatchGet(region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnBatchGet(region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnBatchRollback(uint64_t region_id) {
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
  request.add_keys()->assign(FLAGS_key);

  if (!FLAGS_key2.empty()) {
    request.add_keys()->assign(FLAGS_key2);
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

void SendTxnScanLock(uint64_t region_id) {
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
  }
  request.set_start_key(FLAGS_start_key);

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  request.set_end_key(FLAGS_end_key);

  if (FLAGS_limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    return;
  }
  request.set_limit(FLAGS_limit);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnHeartBeat(uint64_t region_id) {
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
  }
  request.set_primary_lock(FLAGS_primary_lock);

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

void SendTxnGc(uint64_t region_id) {
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

void SendTxnDeleteRange(uint64_t region_id) {
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
  }
  request.set_start_key(FLAGS_start_key);

  if (FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  request.set_end_key(FLAGS_end_key);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnDump(uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (!region.definition().index_parameter().has_vector_index_parameter()) {
    StoreSendTxnDump(region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnDump(region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

}  // namespace client
