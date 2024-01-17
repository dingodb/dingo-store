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

#ifndef DINGODB_BENCHMARK_OPERATION_H_
#define DINGODB_BENCHMARK_OPERATION_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "sdk/client.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/status.h"

namespace dingodb {
namespace benchmark {

class Operation {
 public:
  Operation() = default;
  virtual ~Operation() = default;

  struct Result {
    sdk::Status status;
    size_t eplased_time{0};
    size_t write_bytes{0};
    size_t read_bytes{0};
  };

  virtual bool Arrange() = 0;

  virtual Result Execute() = 0;
};
using OperationPtr = std::shared_ptr<Operation>;

// All operation base class
// Support common method
class BaseOperation : public Operation {
 public:
  BaseOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix);
  ~BaseOperation() override = default;

  bool Arrange() override { return true; }

  Result Execute() override { return {}; }

 protected:
  Result KvPut(bool is_random);
  Result KvBatchPut(bool is_random);

  Result KvGet(std::string key);
  Result KvBatchGet(const std::vector<std::string>& keys);

  std::shared_ptr<sdk::Client> client;
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv;
  std::string prefix;

  std::atomic<size_t> counter{0};
};

// Sequence write operation
class FillSeqOperation : public BaseOperation {
 public:
  FillSeqOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : BaseOperation(client, prefix) {}
  ~FillSeqOperation() override = default;

  Result Execute() override;
};

// Random write operation
class FillRandomOperation : public BaseOperation {
 public:
  FillRandomOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : BaseOperation(client, prefix) {}
  ~FillRandomOperation() override = default;

  Result Execute() override;
};

// Read operation base class
class ReadOperation : public BaseOperation {
 public:
  ReadOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : BaseOperation(client, prefix) {}
  ~ReadOperation() override = default;

  bool Arrange() override;

 protected:
  std::vector<std::string> keys;
};

// Sequence read operation
class ReadSeqOperation : public ReadOperation {
 public:
  ReadSeqOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : ReadOperation(client, prefix) {}
  ~ReadSeqOperation() override = default;

  Result Execute() override;

 private:
  uint32_t index_{0};
};

// Random read operation
class ReadRandomOperation : public ReadOperation {
 public:
  ReadRandomOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : ReadOperation(client, prefix) {}
  ~ReadRandomOperation() override = default;

  Result Execute() override;
};

// Missing read operation
class ReadMissingOperation : public ReadOperation {
 public:
  ReadMissingOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix)
      : ReadOperation(client, prefix) {}
  ~ReadMissingOperation() override = default;

  Result Execute() override;
};

bool IsSupportBenchmarkType(const std::string& benchmark);
std::string GetSupportBenchmarkType();
OperationPtr NewOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix);

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_OPERATION_H_