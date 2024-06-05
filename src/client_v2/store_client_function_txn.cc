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

#include <cstdint>
#include <string>

#include "client_v2/client_interation.h"
#include "client_v2/store_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "document/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"
#include "serial/buf.h"
#include "vector/codec.h"

const int kBatchSize = 1000;

namespace client_v2 {
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
  return dingodb::Helper::StringToHex(dingodb::VectorCodec::PackageVectorKey(prefix, part_id));
}

std::string VectorPrefixToHex(char prefix, int64_t part_id, int64_t vector_id) {
  return dingodb::Helper::StringToHex(dingodb::VectorCodec::PackageVectorKey(prefix, part_id, vector_id));
}

std::string TablePrefixToHex(char prefix, const std::string& user_key) {
  std::string buf;
  buf.push_back(prefix);
  buf.append(user_key);
  return dingodb::Helper::StringToHex(buf);
}

std::string TablePrefixToHex(char prefix, int64_t part_id) {
  std::string buf;
  buf.push_back(prefix);
  dingodb::SerialHelper::WriteLong(part_id, buf);

  return dingodb::Helper::StringToHex(buf);
}

std::string TablePrefixToHex(char prefix, int64_t part_id, const std::string& user_key) {
  std::string buf;
  buf.push_back(prefix);
  dingodb::SerialHelper::WriteLong(part_id, buf);
  buf.append(user_key);

  return dingodb::Helper::StringToHex(buf);
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
  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    service_name = "DocumentService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    exit(-1);
  }

  return service_name;
}

// store
void StoreSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                          std::string primary_lock, bool key_is_hex, int64_t start_ts, int64_t lock_ttl,
                          int64_t txn_size, bool try_one_pc, int64_t max_commit_ts, std::string mutation_op,
                          std::string key, std::string key2, std::string value, std::string value2, bool value_is_hex,
                          std::string extra_data, int64_t for_update_ts) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  if (key_is_hex) {
    request.set_primary_lock(HexToString(primary_lock));
  } else {
    request.set_primary_lock(primary_lock);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(lock_ttl);

  if (txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(txn_size);

  request.set_try_one_pc(try_one_pc);
  request.set_max_commit_ts(max_commit_ts);

  if (mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }
  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  std::string target_key2 = key2;
  if (key_is_hex) {
    target_key = HexToString(key);
    target_key2 = HexToString(key2);
  }
  if (mutation_op == "put") {
    if (value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);
    mutation->set_key(target_key);
    if (value_is_hex) {
      mutation->set_value(HexToString(value));
    } else {
      mutation->set_value(value);
    }
    DINGO_LOG(INFO) << "key: " << key << ", value: " << value;

    if (!key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Put);
      mutation->set_key(target_key2);
      if (value_is_hex) {
        mutation->set_value(HexToString(value));
      } else {
        mutation->set_value(value);
      }
      DINGO_LOG(INFO) << "key2: " << key2 << ", value: " << value;
    }
  } else if (mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(key);
    DINGO_LOG(INFO) << "key: " << key;

    if (!key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Delete);
      mutation->set_key(key2);
      DINGO_LOG(INFO) << "key2: " << key2;
    }
  } else if (mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(target_key);
    DINGO_LOG(INFO) << "key: " << target_key;

    if (!key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
      mutation->set_key(target_key2);
      DINGO_LOG(INFO) << "key2: " << target_key2;
    }
  } else if (mutation_op == "insert") {
    if (value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(target_key);
    if (value_is_hex) {
      mutation->set_value(HexToString(value));
    } else {
      mutation->set_value(value);
    }
    DINGO_LOG(INFO) << "key: " << key << ", value: " << value;

    if (!key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
      mutation->set_key(target_key2);
      if (value_is_hex) {
        mutation->set_value(HexToString(value2));
      } else {
        mutation->set_value(value2);
      }
      DINGO_LOG(INFO) << "key2: " << key2 << ", value2: " << value2;
    }
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << extra_data;
  }

  if (for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(for_update_ts);
      for_update_ts_check->set_index(i);

      if (!extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                          std::string primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                          bool try_one_pc, int64_t max_commit_ts, std::string mutation_op, std::string extra_data,
                          int64_t for_update_ts, int64_t vector_id) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(primary_lock);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(lock_ttl);

  if (txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(txn_size);

  request.set_try_one_pc(try_one_pc);
  request.set_max_commit_ts(max_commit_ts);

  if (mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (vector_id == 0) {
    DINGO_LOG(ERROR) << "vector_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
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

  char perfix = dingodb::Helper::GetKeyPrefix(region.definition().range());
  if (mutation_op == "put") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);

    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, vector_id));

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
  } else if (mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, vector_id));
  } else if (mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, vector_id));
  } else if (mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, vector_id));

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

  if (!extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << extra_data;
  }

  if (for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(for_update_ts);
      for_update_ts_check->set_index(i);

      if (!extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// void IndexSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
//                           std::string primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
//                           bool try_one_pc, int64_t max_commit_ts, std::string mutation_op, std::string extra_data,
//                           int64_t for_update_ts, int64_t vector_id) {
//   dingodb::pb::store::TxnPrewriteRequest request;
//   dingodb::pb::store::TxnPrewriteResponse response;

//   request.mutable_context()->set_region_id(region_id);
//   *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
//   if (rc) {
//     request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
//   } else {
//     request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
//   }

//   if (primary_lock.empty()) {
//     DINGO_LOG(ERROR) << "primary_lock is empty";
//     return;
//   }
//   request.set_primary_lock(primary_lock);

//   if (start_ts == 0) {
//     DINGO_LOG(ERROR) << "start_ts is empty";
//     return;
//   }
//   request.set_start_ts(start_ts);

//   if (lock_ttl == 0) {
//     DINGO_LOG(ERROR) << "lock_ttl is empty";
//     return;
//   }
//   request.set_lock_ttl(lock_ttl);

//   if (txn_size == 0) {
//     DINGO_LOG(ERROR) << "txn_size is empty";
//     return;
//   }
//   request.set_txn_size(txn_size);

//   request.set_try_one_pc(try_one_pc);
//   request.set_max_commit_ts(max_commit_ts);

//   if (mutation_op.empty()) {
//     DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
//     return;
//   }

//   if (vector_id == 0) {
//     DINGO_LOG(ERROR) << "vector_id is empty";
//     return;
//   }

//   int64_t part_id = region.definition().part_id();
//   int64_t target_vector_id = vector_id;
//   int64_t dimension = 0;

//   const auto& para = region.definition().index_parameter().vector_index_parameter();
//   if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
//     dimension = para.flat_parameter().dimension();
//   } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
//     dimension = para.ivf_flat_parameter().dimension();
//   } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
//     dimension = para.ivf_pq_parameter().dimension();
//   } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
//     dimension = para.hnsw_parameter().dimension();
//   } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
//     dimension = para.diskann_parameter().dimension();
//   } else {
//     DINGO_LOG(ERROR) << "vector_index_type is empty";
//     return;
//   }

//   if (mutation_op == "put") {
//     auto* mutation = request.add_mutations();
//     mutation->set_op(::dingodb::pb::store::Op::Put);

//     mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0],
//                                                                      part_id, target_vector_id));

//     dingodb::pb::common::VectorWithId vector_with_id;

//     std::mt19937 rng;
//     std::uniform_real_distribution<> distrib(0.0, 1.0);

//     vector_with_id.set_id(target_vector_id);
//     vector_with_id.mutable_vector()->set_dimension(dimension);
//     vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
//     for (int j = 0; j < dimension; j++) {
//       vector_with_id.mutable_vector()->add_float_values(distrib(rng));
//     }
//     *mutation->mutable_vector() = vector_with_id;
//   } else if (mutation_op == "delete") {
//     auto* mutation = request.add_mutations();
//     mutation->set_op(::dingodb::pb::store::Op::Delete);
//     mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0],
//                                                                      part_id, target_vector_id));
//     mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, vector_id));
//   } else if (mutation_op == "check_not_exists") {
//     auto* mutation = request.add_mutations();
//     mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
//     mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0],
//                                                                      part_id, target_vector_id));
//   } else if (mutation_op == "insert") {
//     auto* mutation = request.add_mutations();
//     mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
//     mutation->set_key(dingodb::Helper::EncodeVectorIndexRegionHeader(region.definition().range().start_key()[0],
//                                                                      part_id, target_vector_id));

//     dingodb::pb::common::VectorWithId vector_with_id;

//     std::mt19937 rng;
//     std::uniform_real_distribution<> distrib(0.0, 1.0);

//     vector_with_id.set_id(target_vector_id);
//     vector_with_id.mutable_vector()->set_dimension(dimension);
//     vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
//     for (int j = 0; j < dimension; j++) {
//       vector_with_id.mutable_vector()->add_float_values(distrib(rng));
//     }
//     *mutation->mutable_vector() = vector_with_id;
//   } else {
//     DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
//     return;
//   }

//   if (!extra_data.empty()) {
//     DINGO_LOG(INFO) << "extra_data is: " << extra_data;
//   }

//   if (for_update_ts > 0) {
//     DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << for_update_ts;
//     for (int i = 0; i < request.mutations_size(); i++) {
//       request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
//                                          TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
//       auto* for_update_ts_check = request.add_for_update_ts_checks();
//       for_update_ts_check->set_expected_for_update_ts(for_update_ts);
//       for_update_ts_check->set_index(i);

//       if (!extra_data.empty()) {
//         auto* extra_datas = request.add_lock_extra_datas();
//         extra_datas->set_index(i);
//         extra_datas->set_extra_data(extra_data);
//       }
//     }
//   }

//   DINGO_LOG(INFO) << "Request: " << request.DebugString();

//   InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnPrewrite", request, response);

//   DINGO_LOG(INFO) << "Response: " << response.DebugString();
// }

void DocumentSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                             std::string primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                             bool try_one_pc, int64_t max_commit_ts, std::string mutation_op, std::string extra_data,
                             int64_t for_update_ts, int64_t document_id, std::string document_text1,
                             std::string document_text2) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(primary_lock);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(lock_ttl);

  if (txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(txn_size);

  request.set_try_one_pc(try_one_pc);
  request.set_max_commit_ts(max_commit_ts);

  if (mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (document_id == 0) {
    DINGO_LOG(ERROR) << "document_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
  int64_t dimension = 0;

  if (region.region_type() != dingodb::pb::common::RegionType::DOCUMENT_REGION) {
    DINGO_LOG(ERROR) << "region_type is invalid, only document region can use this function";
    return;
  }

  char prefix = dingodb::Helper::GetKeyPrefix(region.definition().range());
  if (mutation_op == "put") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);

    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, document_id));

    dingodb::pb::common::DocumentWithId document_with_id;

    document_with_id.set_id(document_id);
    auto* document = document_with_id.mutable_document();

    document_with_id.set_id(document_id);
    auto* document_data = document_with_id.mutable_document()->mutable_document_data();

    if (document_text1.empty()) {
      DINGO_LOG(ERROR) << "document_text1 is empty";
      return;
    }

    if (document_text2.empty()) {
      DINGO_LOG(ERROR) << "document_text2 is empty";
      return;
    }

    // col1 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(document_text1);
      (*document_data)["col1"] = document_value1;
    }

    // col2 int64
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
      document_value1.mutable_field_value()->set_long_data(document_id);
      (*document_data)["col2"] = document_value1;
    }

    // col3 double
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
      document_value1.mutable_field_value()->set_double_data(document_id * 1.0);
      (*document_data)["col3"] = document_value1;
    }

    // col4 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(document_text2);
      (*document_data)["col4"] = document_value1;
    }

    *mutation->mutable_document() = document_with_id;
  } else if (mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, document_id));
  } else if (mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, document_id));
  } else if (mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, document_id));

    dingodb::pb::common::DocumentWithId document_with_id;

    document_with_id.set_id(document_id);
    auto* document = document_with_id.mutable_document();

    document_with_id.set_id(document_id);
    auto* document_data = document_with_id.mutable_document()->mutable_document_data();

    if (document_text1.empty()) {
      DINGO_LOG(ERROR) << "document_text1 is empty";
      return;
    }

    if (document_text2.empty()) {
      DINGO_LOG(ERROR) << "document_text2 is empty";
      return;
    }

    // col1 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(document_text1);
      (*document_data)["col1"] = document_value1;
    }

    // col2 int64
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
      document_value1.mutable_field_value()->set_long_data(document_id);
      (*document_data)["col2"] = document_value1;
    }

    // col3 double
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
      document_value1.mutable_field_value()->set_double_data(document_id * 1.0);
      (*document_data)["col3"] = document_value1;
    }

    // col4 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(document_text2);
      (*document_data)["col4"] = document_value1;
    }

    *mutation->mutable_document() = document_with_id;
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << extra_data;
  }

  if (for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(for_update_ts);
      for_update_ts_check->set_index(i);

      if (!extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

// unified
void SendTxnGet(int64_t region_id, bool rc, std::string key, bool key_is_hex, int64_t start_ts, int64_t resolve_locks) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  request.set_key(target_key);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnScan(int64_t region_id, bool rc, std::string start_key, std::string end_key, int64_t limit,
                 int64_t start_ts, bool is_reverse, bool key_only, int64_t resolve_locks, bool key_is_hex,
                 bool with_start, bool with_end) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    std::cout << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

  request.mutable_context()->set_region_id(region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  dingodb::pb::common::RangeWithOptions range;
  if (start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    std::cout << "start_key is empty";
    return;
  } else {
    std::string key = start_key;
    if (key_is_hex) {
      key = HexToString(start_key);
    }
    range.mutable_range()->set_start_key(key);
  }
  if (end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    std::cout << "end_key is empty";
    return;
  } else {
    std::string key = end_key;
    if (key_is_hex) {
      key = HexToString(end_key);
    }
    range.mutable_range()->set_end_key(key);
  }
  range.set_with_start(with_start);
  range.set_with_end(with_end);
  *request.mutable_range() = range;

  if (limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    std::cout << "limit is empty";
    return;
  }
  request.set_limit(limit);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  request.set_is_reverse(is_reverse);
  request.set_key_only(key_only);

  if (resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnPessimisticLock(int64_t region_id, bool rc, std::string primary_lock, bool key_is_hex, int64_t start_ts,
                            int64_t lock_ttl, int64_t for_update_ts, std::string mutation_op, std::string key,
                            std::string value, bool value_is_hex) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = primary_lock;
    if (key_is_hex) {
      key = HexToString(primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(lock_ttl);

  if (for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(for_update_ts);

  if (mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [lock]";
    return;
  }
  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  if (value.empty()) {
    DINGO_LOG(ERROR) << "value is empty";
    return;
  }
  std::string target_value = value;
  if (value_is_hex) {
    target_value = HexToString(value);
  }
  if (mutation_op == "lock") {
    if (value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Lock);
    mutation->set_key(target_key);
    mutation->set_value(target_value);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be [lock]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticRollback(int64_t region_id, bool rc, int64_t start_ts, int64_t for_update_ts, std::string key,
                                bool key_is_hex) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name;
  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    service_name = "DocumentService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    return;
  }

  dingodb::pb::store::TxnPessimisticRollbackRequest request;
  dingodb::pb::store::TxnPessimisticRollbackResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(for_update_ts);

  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  *request.add_keys() = target_key;

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnPrewrite(int64_t region_id, bool rc, std::string primary_lock, bool key_is_hex, int64_t start_ts,
                     int64_t lock_ttl, int64_t txn_size, bool try_one_pc, int64_t max_commit_ts,
                     std::string mutation_op, std::string key, std::string key2, std::string value, std::string value2,
                     bool value_is_hex, std::string extra_data, int64_t for_update_ts, int64_t vector_id,
                     int64_t document_id, std::string document_text1, std::string document_text2) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    StoreSendTxnPrewrite(region_id, region, rc, primary_lock, key_is_hex, start_ts, lock_ttl, txn_size, try_one_pc,
                         max_commit_ts, mutation_op, key, key2, value, value2, value_is_hex, extra_data, for_update_ts);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnPrewrite(region_id, region, rc, primary_lock, start_ts, lock_ttl, txn_size, try_one_pc, max_commit_ts,
                         mutation_op, extra_data, for_update_ts, vector_id);
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    DocumentSendTxnPrewrite(region_id, region, rc, primary_lock, start_ts, lock_ttl, txn_size, try_one_pc,
                            max_commit_ts, mutation_op, extra_data, for_update_ts, document_id, document_text1,
                            document_text2);

  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}
void SendTxnCommit(int64_t region_id, bool rc, int64_t start_ts, int64_t commit_ts, std::string key, std::string key2,
                   bool key_is_hex) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (commit_ts == 0) {
    DINGO_LOG(ERROR) << "commit_ts is empty";
    return;
  }
  request.set_commit_ts(commit_ts);

  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  request.add_keys()->assign(target_key);
  DINGO_LOG(INFO) << "key: " << key;

  if (!key2.empty()) {
    std::string target_key2 = key2;
    if (key_is_hex) {
      target_key2 = HexToString(key2);
    }
    request.add_keys()->assign(target_key);
    DINGO_LOG(INFO) << "key2: " << key2;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnCheckTxnStatus(int64_t region_id, bool rc, std::string primary_key, bool key_is_hex, int64_t lock_ts,
                           int64_t caller_start_ts, int64_t current_ts) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_key.empty()) {
    DINGO_LOG(ERROR) << "primary_key is empty";
    return;
  } else {
    std::string key = primary_key;
    if (key_is_hex) {
      key = HexToString(primary_key);
    }
    request.set_primary_key(key);
  }

  if (lock_ts == 0) {
    DINGO_LOG(ERROR) << "lock_ts is 0";
    return;
  }
  request.set_lock_ts(lock_ts);

  if (caller_start_ts == 0) {
    DINGO_LOG(ERROR) << "caller_start_ts is 0";
    return;
  }
  request.set_caller_start_ts(caller_start_ts);

  if (current_ts == 0) {
    DINGO_LOG(ERROR) << "current_ts is 0";
    return;
  }
  request.set_current_ts(current_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCheckTxnStatus", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnResolveLock(int64_t region_id, bool rc, int64_t start_ts, int64_t commit_ts, std::string key,
                        bool key_is_hex) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is 0";
    return;
  }
  request.set_start_ts(start_ts);

  DINGO_LOG(INFO) << "commit_ts is: " << commit_ts;
  if (commit_ts != 0) {
    DINGO_LOG(WARNING) << "commit_ts is not 0, will do commit";
  } else {
    DINGO_LOG(WARNING) << "commit_ts is 0, will do rollback";
  }
  request.set_commit_ts(commit_ts);

  if (key.empty()) {
    DINGO_LOG(INFO) << "key is empty, will do resolve lock for all keys of this transaction";
  } else {
    std::string target_key = key;
    if (key_is_hex) {
      target_key = HexToString(key);
    }
    request.add_keys()->assign(target_key);
    DINGO_LOG(INFO) << "key: " << key;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchGet(int64_t region_id, bool rc, std::string key, std::string key2, bool key_is_hex, int64_t start_ts,
                     int64_t resolve_locks) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  if (key2.empty()) {
    DINGO_LOG(ERROR) << "key2 is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  std::string target_key2 = key2;
  if (key_is_hex) {
    target_key2 = HexToString(key2);
  }
  request.add_keys()->assign(target_key);
  request.add_keys()->assign(target_key2);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchRollback(int64_t region_id, bool rc, std::string key, std::string key2, bool key_is_hex,
                          int64_t start_ts) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = key;
  if (key_is_hex) {
    target_key = HexToString(key);
  }
  request.add_keys()->assign(target_key);

  if (!key2.empty()) {
    std::string target_key2 = key2;
    if (key_is_hex) {
      target_key2 = HexToString(key2);
    }
    request.add_keys()->assign(target_key2);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnScanLock(int64_t region_id, bool rc, int64_t max_ts, std::string start_key, std::string end_key,
                     bool key_is_hex, int64_t limit) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (max_ts == 0) {
    DINGO_LOG(ERROR) << "max_ts is empty";
    return;
  }
  request.set_max_ts(max_ts);

  if (start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = start_key;
    if (key_is_hex) {
      key = HexToString(start_key);
    }
    request.set_start_key(key);
  }

  if (end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = end_key;
    if (key_is_hex) {
      key = HexToString(end_key);
    }
    request.set_end_key(key);
  }

  if (limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    return;
  }
  request.set_limit(limit);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnHeartBeat(int64_t region_id, bool rc, std::string primary_lock, int64_t start_ts, int64_t advise_lock_ttl,
                      bool key_is_hex) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = primary_lock;
    if (key_is_hex) {
      key = HexToString(primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (advise_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "advise_lock_ttl is empty";
    return;
  }
  request.set_advise_lock_ttl(advise_lock_ttl);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnHeartBeat", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnGc(int64_t region_id, bool rc, int64_t safe_point_ts) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (safe_point_ts == 0) {
    DINGO_LOG(ERROR) << "safe_point_ts is empty";
    return;
  }
  request.set_safe_point_ts(safe_point_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGc", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnDeleteRange(int64_t region_id, bool rc, std::string start_key, std::string end_key, bool key_is_hex) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = start_key;
    if (key_is_hex) {
      key = HexToString(start_key);
    }
    request.set_start_key(key);
  }

  if (end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = end_key;
    if (key_is_hex) {
      key = HexToString(end_key);
    }
    request.set_end_key(key);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnDump(int64_t region_id, bool rc, std::string start_key, std::string end_key, bool key_is_hex,
                 int64_t start_ts, int64_t end_ts) {
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
  if (rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  }
  std::string target_start_key = start_key;

  if (end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  }
  std::string target_end_key = end_key;

  if (key_is_hex) {
    target_start_key = dingodb::Helper::HexToString(start_key);
    target_end_key = dingodb::Helper::HexToString(end_key);
  }

  request.set_start_key(target_start_key);
  request.set_end_key(target_end_key);

  if (start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(start_ts);

  if (end_ts == 0) {
    DINGO_LOG(ERROR) << "end_ts is empty";
    return;
  }
  request.set_end_ts(end_ts);

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

  std::cout << "data_size: " << response.data_keys_size() << std::endl;
  std::cout << "lock_size: " << response.lock_keys_size() << std::endl;
  std::cout << "write_size: " << response.write_keys_size() << std::endl;

  for (int i = 0; i < response.data_keys_size(); i++) {
    DINGO_LOG(INFO) << "data[" << i << "] hex_key: [" << StringToHex(response.data_keys(i).key()) << "] key: ["
                    << response.data_keys(i).ShortDebugString() << "], value: ["
                    << response.data_values(i).ShortDebugString() << "]";
    std::cout << "data[" << i << "] hex_key: [" << StringToHex(response.data_keys(i).key()) << "] key: ["
              << response.data_keys(i).ShortDebugString() << "], value: [" << response.data_values(i).ShortDebugString()
              << "]" << std::endl;
  }

  for (int i = 0; i < response.lock_keys_size(); i++) {
    DINGO_LOG(INFO) << "lock[" << i << "] hex_key: [" << StringToHex(response.lock_keys(i).key()) << "] key: ["
                    << response.lock_keys(i).ShortDebugString() << "], value: ["
                    << response.lock_values(i).ShortDebugString() << "]";
    std::cout << "lock[" << i << "] hex_key: [" << StringToHex(response.lock_keys(i).key()) << "] key: ["
              << response.lock_keys(i).ShortDebugString() << "], value: [" << response.lock_values(i).ShortDebugString()
              << "]" << std::endl;
  }

  for (int i = 0; i < response.write_keys_size(); i++) {
    DINGO_LOG(INFO) << "write[" << i << "] hex_key: [" << StringToHex(response.write_keys(i).key()) << "] key: ["
                    << response.write_keys(i).ShortDebugString() << "], value: ["
                    << response.write_values(i).ShortDebugString() << "]";
    std::cout << "write[" << i << "] hex_key: [" << StringToHex(response.write_keys(i).key()) << "] key: ["
              << response.write_keys(i).ShortDebugString() << "], value: ["
              << response.write_values(i).ShortDebugString() << "]" << std::endl;
  }
}

}  // namespace client_v2
