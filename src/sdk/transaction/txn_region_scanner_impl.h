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

#ifndef DINGODB_SDK_TXN_REGON_SCANNER_IMPL_H_
#define DINGODB_SDK_TXN_REGON_SCANNER_IMPL_H_

#include <cstdint>
#include <memory>

#include "sdk/client.h"
#include "sdk/region_scanner.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"

namespace dingodb {
namespace sdk {
class TxnRegionScannerFactoryImpl;
class TestBase;

class TxnRegionScannerImpl : public RegionScanner {
 public:
  explicit TxnRegionScannerImpl(const ClientStub& stub, std::shared_ptr<Region> region,
                                const TransactionOptions& txn_options, int64_t txn_start_ts, std::string start_key,
                                std::string end_key);

  ~TxnRegionScannerImpl() override;

  Status Open() override;

  void AsyncOpen(StatusCallback cb) override { cb(Status::NotSupported("AsyncOpen is not supported")); }

  void Close() override;

  void AsyncClose(StatusCallback cb) override { cb(Status::NotSupported("AsyncClose is not supported")); }

  Status NextBatch(std::vector<KVPair>& kvs) override;

  void AsyncNextBatch(std::vector<KVPair>& kvs, StatusCallback cb) override {
    (void)kvs;
    (void)cb;
    CHECK(false) << "AsyncNextBatch is not supported";
  }

  bool HasMore() const override;

  Status SetBatchSize(int64_t size) override;

  int64_t GetBatchSize() const override { return batch_size_; }

  bool TEST_IsOpen() {  // NOLINT
    return opened_;
  }

 private:
  std::unique_ptr<TxnScanRpc> PrepareTxnScanRpc();

  static bool NeedRetryAndInc(int& times);
  static void DelayRetry(int64_t delay_ms);

  const TransactionOptions txn_options_;
  int64_t txn_start_ts_;
  std::string start_key_;
  std::string end_key_;
  int64_t batch_size_;
  bool opened_;
  bool has_more_;
  std::string next_key_;
  bool include_next_key_;
};

class TxnRegionScannerFactoryImpl final : public RegionScannerFactory {
 public:
  TxnRegionScannerFactoryImpl();

  ~TxnRegionScannerFactoryImpl() override;

  Status NewRegionScanner(const ScannerOptions& options, std::shared_ptr<RegionScanner>& scanner) override;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TXN_REGON_SCANNER_IMPL_H_