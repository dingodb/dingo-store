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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "benchmark/dataset.h"
#include "sdk/client.h"
#include "sdk/status.h"
#include "sdk/vector.h"

namespace dingodb {
namespace benchmark {

class RegionEntry;
using RegionEntryPtr = std::shared_ptr<RegionEntry>;

class VectorIndexEntry;
using VectorIndexEntryPtr = std::shared_ptr<VectorIndexEntry>;

// Abstract interface class
class Operation {
 public:
  Operation() = default;
  virtual ~Operation() = default;

  struct Result {
    sdk::Status status;
    size_t eplased_time{0};
    size_t write_bytes{0};
    size_t read_bytes{0};

    std::vector<uint32_t> recalls;
    std::vector<sdk::SearchResult> vector_search_results;
  };

  // Do some ready work at arrange stage
  virtual bool Arrange(RegionEntryPtr region_entry) = 0;
  virtual bool Arrange(VectorIndexEntryPtr entry, DatasetPtr dataset) = 0;

  // RPC invoke, return execute result
  virtual Result Execute(RegionEntryPtr region_entry) = 0;
  // RPC invoke, return execute result, for transaction access multiple region
  virtual Result Execute(std::vector<RegionEntryPtr>& region_entries) = 0;

  // RPC invoke, return execute result, for vector index
  virtual Result Execute(VectorIndexEntryPtr entry) = 0;
};
using OperationPtr = std::shared_ptr<Operation>;

// All operation base class
// Support common method
class BaseOperation : public Operation {
 public:
  BaseOperation(std::shared_ptr<sdk::Client> client);
  ~BaseOperation() override = default;

  bool Arrange(RegionEntryPtr) override { return true; }
  bool Arrange(VectorIndexEntryPtr, DatasetPtr) override { return true; }

  Result Execute(RegionEntryPtr) override { return {}; }

  Result Execute(std::vector<RegionEntryPtr>&) override { return {}; }

  Result Execute(VectorIndexEntryPtr) override { return {}; }

 protected:
  Result KvPut(RegionEntryPtr region_entry, bool is_random);
  Result KvBatchPut(RegionEntryPtr region_entry, bool is_random);

  Result KvGet(std::string key);
  Result KvBatchGet(const std::vector<std::string>& keys);

  Result KvTxnPut(std::vector<RegionEntryPtr>& region_entries, bool is_random);
  Result KvTxnPut(const std::vector<sdk::KVPair>& kvs);
  Result KvTxnBatchPut(std::vector<RegionEntryPtr>& region_entries, bool is_random);
  Result KvTxnBatchPut(const std::vector<sdk::KVPair>& kvs);

  Result KvTxnGet(const std::vector<std::string>& keys);
  Result KvTxnBatchGet(const std::vector<std::vector<std::string>>& keys);

  Result VectorPut(VectorIndexEntryPtr entry, const std::vector<sdk::VectorWithId>& vector_with_ids);
  Result VectorSearch(VectorIndexEntryPtr entry, const std::vector<sdk::VectorWithId>& vector_with_ids,
                      const sdk::SearchParam& search_param);

  std::shared_ptr<sdk::Client> client;
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv;
};

// Sequence write operation
class FillSeqOperation : public BaseOperation {
 public:
  FillSeqOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~FillSeqOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
};

// Random write operation
class FillRandomOperation : public BaseOperation {
 public:
  FillRandomOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~FillRandomOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
};

// Read operation base class
class ReadOperation : public BaseOperation {
 public:
  ReadOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~ReadOperation() override = default;

  bool Arrange(RegionEntryPtr region_entry) override;
};

// Sequence read operation
class ReadSeqOperation : public ReadOperation {
 public:
  ReadSeqOperation(std::shared_ptr<sdk::Client> client) : ReadOperation(client) {}
  ~ReadSeqOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
};

// Random read operation
class ReadRandomOperation : public ReadOperation {
 public:
  ReadRandomOperation(std::shared_ptr<sdk::Client> client) : ReadOperation(client) {}
  ~ReadRandomOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
};

// Missing read operation
class ReadMissingOperation : public ReadOperation {
 public:
  ReadMissingOperation(std::shared_ptr<sdk::Client> client) : ReadOperation(client) {}
  ~ReadMissingOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
};

// Transaction Sequence write operation
class FillTxnSeqOperation : public BaseOperation {
 public:
  FillTxnSeqOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~FillTxnSeqOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
  Result Execute(std::vector<RegionEntryPtr>& region_entries) override;
};

// Transaction random write operation
class FillTxnRandomOperation : public BaseOperation {
 public:
  FillTxnRandomOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~FillTxnRandomOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
  Result Execute(std::vector<RegionEntryPtr>& region_entries) override;
};

// Transaction read operation base class
class TxnReadOperation : public BaseOperation {
 public:
  TxnReadOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~TxnReadOperation() override = default;

  bool Arrange(RegionEntryPtr region_entry) override;
};

// Transaction sequence read operation
class TxnReadSeqOperation : public TxnReadOperation {
 public:
  TxnReadSeqOperation(std::shared_ptr<sdk::Client> client) : TxnReadOperation(client) {}
  ~TxnReadSeqOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
  Result Execute(std::vector<RegionEntryPtr>& region_entries) override;
};

// Transaction random read operation
class TxnReadRandomOperation : public TxnReadOperation {
 public:
  TxnReadRandomOperation(std::shared_ptr<sdk::Client> client) : TxnReadOperation(client) {}
  ~TxnReadRandomOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
  Result Execute(std::vector<RegionEntryPtr>& region_entries) override;
};

// Transaction missing read operation
class TxnReadMissingOperation : public TxnReadOperation {
 public:
  TxnReadMissingOperation(std::shared_ptr<sdk::Client> client) : TxnReadOperation(client) {}
  ~TxnReadMissingOperation() override = default;

  Result Execute(RegionEntryPtr region_entry) override;
  Result Execute(std::vector<RegionEntryPtr>& region_entries) override;
};

class VectorFillSeqOperation : public BaseOperation {
 public:
  VectorFillSeqOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~VectorFillSeqOperation() override = default;

  Result Execute(VectorIndexEntryPtr entry) override;
};

class VectorFillRandomOperation : public BaseOperation {
 public:
  VectorFillRandomOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~VectorFillRandomOperation() override = default;

  Result Execute(VectorIndexEntryPtr entry) override;
};

class VectorSearchOperation : public BaseOperation {
 public:
  VectorSearchOperation(std::shared_ptr<sdk::Client> client) : BaseOperation(client) {}
  ~VectorSearchOperation() override = default;

  bool Arrange(VectorIndexEntryPtr entry, DatasetPtr dataset) override;

  Result Execute(VectorIndexEntryPtr entry) override;

 private:
  bool ArrangeAutoData(VectorIndexEntryPtr entry);
  bool ArrangeManualData(VectorIndexEntryPtr entry, DatasetPtr dataset);

  Result ExecuteAutoData(VectorIndexEntryPtr entry);
  Result ExecuteManualData(VectorIndexEntryPtr entry);
};

bool IsSupportBenchmarkType(const std::string& benchmark);
std::string GetSupportBenchmarkType();
OperationPtr NewOperation(std::shared_ptr<sdk::Client> client);

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_OPERATION_H_