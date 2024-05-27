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

#ifndef DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_
#define DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "sdk/port/meta.pb.h"
#include "sdk/status.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

class ClientStub;

class AutoInrementer {
 public:
  AutoInrementer(const ClientStub &stub) : stub_(stub) {}

  virtual ~AutoInrementer() = default;

  Status GetNextId(int64_t &next);

  Status GetNextIds(std::vector<int64_t> &to_fill, int64_t count);

 protected:
  virtual void PrepareRequest(pb::meta::GenerateAutoIncrementRequest &request) = 0;

 private:
  friend class AutoIncrementerManager;
  Status RefillCache();

  const ClientStub &stub_;

  std::mutex mutex_;
  struct Req;
  std::deque<Req *> queue_;
  std::vector<int64_t> id_cache_;
};

class IndexAutoInrementer : public AutoInrementer {
 public:
  IndexAutoInrementer(const ClientStub &stub, std::shared_ptr<VectorIndex> vector_index)
      : AutoInrementer(stub), vector_index_(std::move(vector_index)) {}

  ~IndexAutoInrementer() override = default;

 private:
  friend class AutoIncrementerManager;
  void PrepareRequest(pb::meta::GenerateAutoIncrementRequest &request) override;
  // NOTE: when delete index,  we should delete the auto incrementer
  const std::shared_ptr<VectorIndex> vector_index_;
};

class AutoIncrementerManager {
 public:
  AutoIncrementerManager(const ClientStub &stub) : stub_(stub) {}

  ~AutoIncrementerManager() = default;

  std::shared_ptr<AutoInrementer> GetOrCreateIndexIncrementer(std::shared_ptr<VectorIndex> &index);

  void RemoveIndexIncrementerById(int64_t index_id);

 private:
  const ClientStub &stub_;
  std::mutex mutex_;
  std::unordered_map<int64_t, std::shared_ptr<AutoInrementer>> auto_incrementer_map_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_