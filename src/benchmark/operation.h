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

class BaseOperation : public Operation {
 public:
  BaseOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix);
  ~BaseOperation() override = default;

  bool Arrange() override { return true; }

  Result Execute() override { return {}; }

 protected:
  Result KvPut(bool is_random);
  Result BatchKvPut(bool is_random);

  std::shared_ptr<sdk::Client> client;
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv;
  std::string prefix;

  std::atomic<size_t> counter{0};
};

class FillSeqOperation : public BaseOperation {
 public:
  FillSeqOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : BaseOperation(client, prefix) {}
  ~FillSeqOperation() override = default;

  Result Execute() override;
};

class FillrandomOperation : public BaseOperation {
 public:
  FillrandomOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) : BaseOperation(client, prefix) {}
  ~FillrandomOperation() override = default;

  Result Execute() override;
};

bool IsSupportBenchmarkType(const std::string& benchmark);
std::string GetSupportBenchmarkType();
OperationPtr NewOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix);

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_OPERATION_H_