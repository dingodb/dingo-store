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

#ifndef DINGODB_ENGINE_SCAN_MANAGER_H_  // NOLINT
#define DINGODB_ENGINE_SCAN_MANAGER_H_

#include <map>
#include <memory>
#include <string>

#include "butil/memory/singleton.h"
#include "scan/scan.h"

namespace dingodb {

class ScanManager {
 public:
  static ScanManager* GetInstance();

  ScanManager(const ScanManager& rhs) = delete;
  ScanManager& operator=(const ScanManager& rhs) = delete;
  ScanManager(ScanManager&& rhs) = delete;
  ScanManager& operator=(ScanManager&& rhs) = delete;

  bool Init(std::shared_ptr<Config> config);

  std::shared_ptr<ScanContext> CreateScan(std::string* scan_id);
  std::shared_ptr<ScanContext> FindScan(const std::string& scan_id);
  void DeleteScan(const std::string& scan_id);
  void TryDeleteScan(const std::string& scan_id);

  uint64_t GetTimeoutMs() const { return timeout_ms_; }

  uint64_t GetMaxBytesRpc() const { return max_bytes_rpc_; }
  uint64_t GetMaxFetchCntByServer() const { return max_fetch_cnt_by_server_; }
  uint64_t GetScanIntervalMs() const { return scan_interval_ms_; }

  static void RegularCleaningHandler(void* arg);

 private:
  ScanManager();
  ~ScanManager();
  friend struct DefaultSingletonTraits<ScanManager>;

  std::map<std::string, std::shared_ptr<ScanContext>> alive_scans_;
  std::map<std::string, std::shared_ptr<ScanContext>> waiting_destroyed_scans_;
  uint64_t timeout_ms_;
  uint64_t max_bytes_rpc_;
  uint64_t max_fetch_cnt_by_server_;
  uint64_t scan_interval_ms_;
  bthread_mutex_t mutex_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_SCAN_MANAGER_H_   // NOLINT
