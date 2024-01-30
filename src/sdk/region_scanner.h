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

#ifndef DINGODB_SDK_REGON_SCANNER_H_
#define DINGODB_SDK_REGON_SCANNER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "sdk/client.h"
#include "sdk/region.h"
#include "sdk/utils/callback.h"
namespace dingodb {
namespace sdk {

class ClientStub;
class RegionScanner {
 public:
  RegionScanner(const RegionScanner&) = delete;
  const RegionScanner& operator=(const RegionScanner&) = delete;

  RegionScanner(const ClientStub& stub, std::shared_ptr<Region> region) : stub(stub), region(std::move(region)) {}

  virtual ~RegionScanner() = default;

  virtual Status Open() = 0;

  virtual void AsyncOpen(StatusCallback cb) = 0;

  virtual void Close() = 0;

  virtual void AsyncClose(StatusCallback cb) = 0;

  virtual Status NextBatch(std::vector<KVPair>& kvs) = 0;

  virtual void AsyncNextBatch(std::vector<KVPair>& kvs, StatusCallback cb) = 0;

  virtual bool HasMore() const = 0;

  virtual Status SetBatchSize(int64_t size) = 0;

  virtual int64_t GetBatchSize() const = 0;

  std::shared_ptr<Region> GetRegion() { return region; }

 protected:
  const ClientStub& stub;
  std::shared_ptr<Region> region;
};

struct ScannerOptions {
  const ClientStub& stub;
  std::shared_ptr<Region> region;
  std::string start_key;
  std::string end_key;
  std::optional<const TransactionOptions> txn_options;
  std::optional<int64_t> start_ts;

  explicit ScannerOptions(const ClientStub& p_stub, std::shared_ptr<Region> p_region, std::string p_start_key,
                          std::string p_end_key)
      : stub(p_stub), region(std::move(p_region)), start_key(std::move(p_start_key)), end_key(std::move(p_end_key)) {}

  explicit ScannerOptions(const ClientStub& p_stub, std::shared_ptr<Region> p_region, std::string p_start_key,
                          std::string p_end_key, const TransactionOptions p_txn_options, int64_t p_start_ts)
      : stub(p_stub),
        region(std::move(p_region)),
        start_key(std::move(p_start_key)),
        end_key(std::move(p_end_key)),
        txn_options(p_txn_options),
        start_ts(p_start_ts) {}
};

class RegionScannerFactory {
 public:
  RegionScannerFactory(const RegionScannerFactory&) = delete;
  const RegionScannerFactory& operator=(const RegionScannerFactory&) = delete;

  RegionScannerFactory() = default;

  virtual ~RegionScannerFactory() = default;

  virtual Status NewRegionScanner(const ScannerOptions& options, std::shared_ptr<RegionScanner>& scanner) = 0;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_REGON_SCANNER_H_
