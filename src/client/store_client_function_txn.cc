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
#include "serial/buf.h"

const int kBatchSize = 1000;

DECLARE_string(key);
DECLARE_string(value);
DECLARE_int32(limit);
DECLARE_bool(is_reverse);
DECLARE_string(start_key);
DECLARE_string(end_key);
DECLARE_uint64(vector_id);
DEFINE_uint64(start_ts, 0, "start_ts");
DEFINE_uint64(end_ts, 0, "end_ts");
DEFINE_uint64(commit_ts, 0, "start_ts");
DEFINE_uint64(lock_ts, 0, "start_ts");
DEFINE_uint64(lock_ttl, 0, "lock_ttl");
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
DEFINE_string(mutation_op, "", "mutation_op");
DEFINE_string(key2, "", "key2");
DEFINE_bool(rc, false, "read commited");
DECLARE_int64(dimension);

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
  std::string key = dingodb::Helper::EncodeIndexRegionHeader(part_id, vector_id);
  return dingodb::Helper::StringToHex(key);
}

std::string HexToVectorPrefix(const std::string& hex) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  uint64_t part_id = buf.ReadLong();
  uint64_t vector_id = buf.ReadLong();

  return std::to_string(part_id) + "_" + std::to_string(vector_id);
}

bool TxnGetRegion(ServerInteractionPtr interaction, uint64_t region_id, dingodb::pb::common::Region& region) {
  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(region_id);

  auto status = interaction->SendRequest("CoordinatorService", "QueryRegion", query_request, query_response);
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

void StoreSendTxnGet(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnGetRequest request;
  dingodb::pb::store::TxnGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnScan(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnPrewrite(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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
  } else if (FLAGS_mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(FLAGS_key);
  } else if (FLAGS_mutation_op == "insert") {
    if (FLAGS_value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(FLAGS_key);
    mutation->set_value(FLAGS_value);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("StoreService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnCommit(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnCommitRequest request;
  dingodb::pb::store::TxnCommitResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("StoreService", "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnCheckTxnStatus(std::shared_ptr<Context> ctx, uint64_t region_id,
                                const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnCheckTxnStatusRequest request;
  dingodb::pb::store::TxnCheckTxnStatusResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnCheckTxnStatus", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnResolveLock(std::shared_ptr<Context> ctx, uint64_t region_id,
                             const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnResolveLockRequest request;
  dingodb::pb::store::TxnResolveLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  if (FLAGS_commit_ts == 0) {
    DINGO_LOG(ERROR) << "commit_ts is 0";
    return;
  }
  request.set_commit_ts(FLAGS_commit_ts);

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  request.add_keys()->assign(FLAGS_key);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("StoreService", "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnBatchGet(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnBatchGetRequest request;
  dingodb::pb::store::TxnBatchGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnBatchRollback(std::shared_ptr<Context> ctx, uint64_t region_id,
                               const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnBatchRollbackRequest request;
  dingodb::pb::store::TxnBatchRollbackResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnBatchRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnScanLock(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnScanLockRequest request;
  dingodb::pb::store::TxnScanLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnHeartBeat(std::shared_ptr<Context> ctx, uint64_t region_id,
                           const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnHeartBeatRequest request;
  dingodb::pb::store::TxnHeartBeatResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnHeartBeat", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnGc(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnGcRequest request;
  dingodb::pb::store::TxnGcResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnGc", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnDeleteRange(std::shared_ptr<Context> ctx, uint64_t region_id,
                             const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnDeleteRangeRequest request;
  dingodb::pb::store::TxnDeleteRangeResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void StoreSendTxnDump(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnDumpRequest request;
  dingodb::pb::store::TxnDumpResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("StoreService", "TxnDump", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// index
void IndexSendTxnGet(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnGetRequest request;
  dingodb::pb::index::TxnGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnScan(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnScanRequest request;
  dingodb::pb::index::TxnScanResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnPrewrite(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnPrewriteRequest request;
  dingodb::pb::index::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

    mutation->set_key(dingodb::Helper::EncodeIndexRegionHeader(part_id, vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    mutation->mutable_vector()->CopyFrom(vector_with_id);
  } else if (FLAGS_mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(dingodb::Helper::EncodeIndexRegionHeader(part_id, vector_id));
  } else if (FLAGS_mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);

    mutation->set_key(dingodb::Helper::EncodeIndexRegionHeader(part_id, vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    mutation->mutable_vector()->CopyFrom(vector_with_id);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("IndexService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnCommit(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnCommitRequest request;
  dingodb::pb::index::TxnCommitResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("IndexService", "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnCheckTxnStatus(std::shared_ptr<Context> ctx, uint64_t region_id,
                                const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnCheckTxnStatusRequest request;
  dingodb::pb::index::TxnCheckTxnStatusResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnCheckTxnStatus", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnResolveLock(std::shared_ptr<Context> ctx, uint64_t region_id,
                             const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnResolveLockRequest request;
  dingodb::pb::index::TxnResolveLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  if (FLAGS_commit_ts == 0) {
    DINGO_LOG(ERROR) << "commit_ts is 0";
    return;
  }
  request.set_commit_ts(FLAGS_commit_ts);

  if (FLAGS_key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  request.add_keys()->assign(FLAGS_key);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  ctx->store_interaction->SendRequest("IndexService", "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnBatchGet(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnBatchGetRequest request;
  dingodb::pb::index::TxnBatchGetResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnBatchRollback(std::shared_ptr<Context> ctx, uint64_t region_id,
                               const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnBatchRollbackRequest request;
  dingodb::pb::index::TxnBatchRollbackResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnBatchRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnScanLock(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnScanLockRequest request;
  dingodb::pb::index::TxnScanLockResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnHeartBeat(std::shared_ptr<Context> ctx, uint64_t region_id,
                           const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnHeartBeatRequest request;
  dingodb::pb::index::TxnHeartBeatResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnHeartBeat", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnGc(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnGcRequest request;
  dingodb::pb::index::TxnGcResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnGc", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnDeleteRange(std::shared_ptr<Context> ctx, uint64_t region_id,
                             const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnDeleteRangeRequest request;
  dingodb::pb::index::TxnDeleteRangeResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnDump(std::shared_ptr<Context> ctx, uint64_t region_id, const dingodb::pb::common::Region& region) {
  dingodb::pb::index::TxnDumpRequest request;
  dingodb::pb::index::TxnDumpResponse response;

  request.mutable_context()->set_region_id(region_id);
  request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
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

  ctx->store_interaction->SendRequest("IndexService", "TxnDump", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// unified

void SendTxnGet(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnGet(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnGet(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnScan(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnScan(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnScan(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnPrewrite(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnPrewrite(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnPrewrite(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnCommit(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnCommit(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnCommit(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnCheckTxnStatus(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnCheckTxnStatus(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnCheckTxnStatus(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnResolveLock(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnResolveLock(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnResolveLock(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnBatchGet(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnBatchGet(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnBatchGet(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnBatchRollback(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnBatchRollback(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnBatchRollback(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnScanLock(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnScanLock(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnScanLock(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnHeartBeat(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnHeartBeat(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnHeartBeat(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnGc(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnGc(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnGc(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnDeleteRange(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnDeleteRange(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnDeleteRange(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

void SendTxnDump(std::shared_ptr<Context> ctx, uint64_t region_id) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(ctx->coordinator_interaction, region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::STORE_REGION) {
    StoreSendTxnDump(ctx, region_id, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION) {
    IndexSendTxnDump(ctx, region_id, region);
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}

}  // namespace client
