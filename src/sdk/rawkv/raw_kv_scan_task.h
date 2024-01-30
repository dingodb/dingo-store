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

#ifndef DINGODB_SDK_RAW_KV_SCAN_TASK_H_
#define DINGODB_SDK_RAW_KV_SCAN_TASK_H_

#include <cstdint>

#include "sdk/client_stub.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class RawKvScanTask : public RawKvTask {
 public:
  RawKvScanTask(const ClientStub& stub, const std::string& start_key, const std::string& end_key, uint64_t limit,
                std::vector<KVPair>& out_kvs);

  ~RawKvScanTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  void PostProcess() override;

  void ScanNext();
  void ScannerOpenCallback(Status status, std::shared_ptr<RegionScanner> scanner, std::shared_ptr<Region> region);
  void ScanNextWithScanner(std::shared_ptr<RegionScanner> scanner);
  void NextBatchCallback(const Status& status, std::shared_ptr<RegionScanner> scanner);

  bool ReachLimit();

  std::string Name() const override { return "RawKvScanTask"; }
  std::string ErrorMsg() const override {
    return fmt::format("start_key: {}, end_key:{}, limit:{}", start_key_, end_key_, limit_);
  }

  const std::string& start_key_;
  const std::string& end_key_;
  const uint64_t limit_;
  std::vector<KVPair>& out_kvs_;

  Status status_;
  std::string next_start_key_;
  std::vector<KVPair> tmp_out_kvs_;

  std::vector<KVPair> tmp_scanner_scan_kvs_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RAW_KV_SCAN_TASK_H_
