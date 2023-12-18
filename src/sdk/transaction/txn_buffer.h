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

#ifndef DINGODB_SDK_TRANSACTION_BUFFER_H_
#define DINGODB_SDK_TRANSACTION_BUFFER_H_

#include <cstdint>
#include <map>
#include <string>

#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/store.pb.h"
#include "sdk/client.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

enum TxnMutationType : uint8_t { kNone, kPut, kDelete, kPutIfAbsent };

static const char* TxnMutationType2Str(TxnMutationType type) {
  switch (type) {
    case kNone:
      return "None";
    case kPut:
      return "Put";
    case kDelete:
      return "Delete";
    case kPutIfAbsent:
      return "PutIfAbsent";
    default:
      CHECK(false) << "unknow txn mutation type:" << type;
  }
}

struct TxnMutation {
  TxnMutationType type;
  std::string key;
  std::string value;

  explicit TxnMutation() : TxnMutation(kNone, "", "") {}

  std::string ToString() const {
    return fmt::format("(type:{}, [key:{} value:{}])", TxnMutationType2Str(type), key,
                       (value.empty() ? "NULL" : value));
  }

  static TxnMutation PutMutation(const std::string& key, const std::string& value) {
    return TxnMutation(kPut, key, value);
  }

  static TxnMutation DeleteMutation(const std::string& key) { return TxnMutation(kDelete, key, ""); }

  static TxnMutation PutIfAbsentMutation(const std::string& key, const std::string& value) {
    return TxnMutation(kPutIfAbsent, key, value);
  }

 private:
  explicit TxnMutation(TxnMutationType p_type, const std::string& p_key, std::string p_value)
      : type(p_type), key(p_key), value(p_value) {}
};

// NOTE: we need re think all method if we add lock or other entry type
class TxnBuffer {
 public:
  TxnBuffer();

  ~TxnBuffer();

  Status Get(const std::string& key, TxnMutation& mutation);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  Status Range(const std::string& start_key, const std::string& end_key, std::vector<TxnMutation>& mutations);

  bool IsEmpty() const { return mutation_map_.empty(); }

  int64_t MutationsSize() const { return mutation_map_.size(); }

  // NOTE: check IsEmpty before call this
  std::string GetPrimaryKey();

  const std::map<std::string, TxnMutation>& Mutations() { return mutation_map_; }

 private:
  void Erase(const std::string& key);

  void Emplace(const std::string& key, TxnMutation&& mutation);

  std::string primary_key_;
  std::map<std::string, TxnMutation> mutation_map_;
};

static void TxnMutation2MutationPB(const TxnMutation& mutation, pb::store::Mutation* mutation_pb) {
  switch (mutation.type) {
    case kPut:
      mutation_pb->set_op(pb::store::Op::Put);
      mutation_pb->set_key(mutation.key);
      mutation_pb->set_value(mutation.value);
      break;
    case kPutIfAbsent:
      mutation_pb->set_op(pb::store::Op::PutIfAbsent);
      mutation_pb->set_key(mutation.key);
      mutation_pb->set_value(mutation.value);
      break;
    case kDelete:
      mutation_pb->set_op(pb::store::Op::Delete);
      mutation_pb->set_key(mutation.key);
      break;
    default:
      CHECK(false) << "unknow txn mutation type:" << mutation.type;
  }
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TRANSACTION_BUFFER_H_