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

#ifndef DINGODB_SDK_CLIENT_H_
#define DINGODB_SDK_CLIENT_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "sdk/status.h"
#include "sdk/vector.h"

namespace dingodb {
namespace sdk {

class RawKV;
class RegionCreator;
class TestBase;
class TransactionOptions;
class Transaction;
class VectorIndexCreator;
class VectorClient;

/// @brief Callers must keep client valid in it's lifetime in order to interact with the cluster,
class Client {
 public:
  Client(const Client&) = delete;
  const Client& operator=(const Client&) = delete;

  ~Client();

  // NOTE:: Caller must delete *client when it is no longer needed.
  static Status Build(std::string naming_service_url, Client** client);

  // NOTE:: Caller must delete *raw_kv when it is no longer needed.
  Status NewRawKV(RawKV** raw_kv);

  // NOTE:: Caller must delete *txn when it is no longer needed.
  Status NewTransaction(const TransactionOptions& options, Transaction** txn);

  // NOTE:: Caller must delete *raw_kv when it is no longer needed.
  Status NewRegionCreator(RegionCreator** creator);

  /// The out_create_in_progress is set only in case of success;
  /// it is true if the operation is in progress, else is false
  Status IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress);

  Status DropRegion(int64_t region_id);

  // NOTE:: Caller must delete *client when it is no longer needed.
  Status NewVectorClient(VectorClient** client);

  // NOTE:: Caller must delete *index_creator when it is no longer needed.
  Status NewVectorIndexCreator(VectorIndexCreator** index_creator);

  Status GetIndexId(int64_t schema_id, const std::string& index_name, int64_t& out_index_id);

  Status DropIndex(int64_t index_id);

  Status DropIndexByName(int64_t schema_id, const std::string& index_name);

  // TODO：list index/ GetIndexes

 private:
  friend class RawKV;
  friend class TestBase;

  Client();

  Status Init(std::string naming_service_url);

  // own
  class Data;
  Data* data_;
};

struct KVPair {
  std::string key;
  std::string value;
};

struct KeyOpState {
  std::string key;
  bool state;
};

class RawKV {
 public:
  RawKV(const RawKV&) = delete;
  const RawKV& operator=(const RawKV&) = delete;

  ~RawKV();

  Status Get(const std::string& key, std::string& out_value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& out_kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value, bool& out_state);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& out_states);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  // delete key in [start_key, end_key)
  // output_param: delete_count
  Status DeleteRangeNonContinuous(const std::string& start_key, const std::string& end_key, int64_t& out_delete_count);

  // delete key in [start_key, end_key), but region between [start_key, end_key) must continuous
  // output_param: delete_count
  Status DeleteRange(const std::string& start_key, const std::string& end_key, int64_t& out_delete_count);

  // expected_value: empty means key not exist
  Status CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                       bool& out_state);

  // expected_values size must equal kvs size
  Status BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                            std::vector<KeyOpState>& out_states);

  // limit: 0 means no limit, will scan all key in [start_key, end_key)
  Status Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& out_kvs);

 private:
  friend class Client;

  // own
  class Data;
  Data* data_;

  explicit RawKV(Data* data);
};

enum TransactionKind : uint8_t { kOptimistic, kPessimistic };

enum TransactionIsolation : uint8_t { kSnapshotIsolation, kReadCommitted };

struct TransactionOptions {
  TransactionKind kind;
  TransactionIsolation isolation;
  uint32_t keep_alive_ms;
};

class Transaction {
 public:
  Transaction(const Transaction&) = delete;
  const Transaction& operator=(const Transaction&) = delete;

  ~Transaction();

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  // limit: 0 means no limit, will scan all key in [start_key, end_key)
  Status Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& kvs);

  // If return status is ok, then call Commit
  // else try to precommit or rollback depends on status code
  Status PreCommit();

  // NOTE: Caller should first call PreCommit, when PreCommit success then call Commit
  // If return status is ok or rolledback, txn is end
  // other status, caller should retry
  Status Commit();

  Status Rollback();

 private:
  friend class Client;
  friend class TestBase;

  Status Begin();

  // own
  class TxnImpl;
  TxnImpl* impl_;

  explicit Transaction(TxnImpl* impl);
};

enum EngineType : uint8_t { kLSM, kBTree, kXDPROCKS };

class RegionCreator {
 public:
  ~RegionCreator();

  // required
  RegionCreator& SetRegionName(const std::string& name);

  // required
  RegionCreator& SetRange(const std::string& lower_bound, const std::string& upper_bound);

  /// optional, if not called, defaults is kLSM
  RegionCreator& SetEngineType(EngineType engine_type);

  /// optional, if not called, defaults is 3
  RegionCreator& SetReplicaNum(int64_t num);

  /// Wait for the region to be fully created before returning.
  /// If not called, defaults to true.
  RegionCreator& Wait(bool wait);

  // TODO: support resource_tag/schema_id/table_id/index_id/part_id/store_ids/region_type

  // TODO: support timeout
  /// when wait is false, the out_region_id will be set only in case of status ok
  /// when wait is true, the out_region_id will be set and status maybe ok or not,
  /// so caller should check out_region_id is set or not
  Status Create(int64_t& out_region_id);

 private:
  friend class Client;

  // own
  class Data;
  Data* data_;
  explicit RegionCreator(Data* data);
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_CLIENT_H_