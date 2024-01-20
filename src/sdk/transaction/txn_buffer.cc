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

#include "sdk/transaction/txn_buffer.h"

#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

TxnBuffer::TxnBuffer() = default;

TxnBuffer::~TxnBuffer() {
  primary_key_.clear();
  mutation_map_.clear();
}

Status TxnBuffer::Get(const std::string& key, TxnMutation& mutation) {
  Status ret;
  auto iter = mutation_map_.find(key);
  if (iter != mutation_map_.cend()) {
    mutation = iter->second;
  } else {
    ret = Status::NotFound(fmt::format("key:{} not found", key));
  }
  return ret;
}

Status TxnBuffer::Put(const std::string& key, const std::string& value) {
  Erase(key);
  Emplace(key, TxnMutation::PutMutation(key, value));
  return Status::OK();
}

Status TxnBuffer::BatchPut(const std::vector<KVPair>& kvs) {
  for (const auto& kv : kvs) {
    Put(kv.key, kv.value);
  }
  return Status::OK();
}

Status TxnBuffer::PutIfAbsent(const std::string& key, const std::string& value) {
  TxnMutation op = TxnMutation::PutIfAbsentMutation(key, value);
  auto iter = mutation_map_.find(key);
  if (iter != mutation_map_.cend()) {
    const auto& mutation = iter->second;
    // NOTE: careful if we add more mutation type
    if (mutation.type == kDelete) {
      Erase(key);
      Emplace(key, std::move(op));
    }
  } else {
    Emplace(key, std::move(op));
  }

  return Status::OK();
}

Status TxnBuffer::BatchPutIfAbsent(const std::vector<KVPair>& kvs) {
  for (const auto& kv : kvs) {
    PutIfAbsent(kv.key, kv.value);
  }
  return Status::OK();
}

Status TxnBuffer::Delete(const std::string& key) {
  Erase(key);
  Emplace(key, TxnMutation::DeleteMutation(key));
  return Status::OK();
}

Status TxnBuffer::BatchDelete(const std::vector<std::string>& keys) {
  for (const auto& key : keys) {
    Delete(key);
  }
  return Status::OK();
}

Status TxnBuffer::Range(const std::string& start_key, const std::string& end_key, std::vector<TxnMutation>& mutations) {
  CHECK(start_key < end_key) << "start key must smaller than end_key";
  if (IsEmpty()) {
    return Status::OK();
  }

  auto start_iter = mutation_map_.lower_bound(start_key);
  if (start_iter == mutation_map_.end()) {
    return Status::OK();
  }

  const auto end_iter = mutation_map_.lower_bound(end_key);
  CHECK(end_iter != mutation_map_.begin());

  while (start_iter != end_iter) {
    mutations.push_back(start_iter->second);
    start_iter++;
  }

  return Status::OK();
}

std::string TxnBuffer::GetPrimaryKey() {
  CHECK(!primary_key_.empty()) << "call IsEmpty before this method";
  return primary_key_;
}

void TxnBuffer::Erase(const std::string& key) {
  if (key == primary_key_) {
    primary_key_.clear();
  }

  mutation_map_.erase(key);
}

void TxnBuffer::Emplace(const std::string& key, TxnMutation&& mutation) {
  if (primary_key_.empty()) {
    primary_key_ = key;
  }

  CHECK(mutation_map_.insert({key, std::move(mutation)}).second);
}

}  // namespace sdk
}  // namespace dingodb