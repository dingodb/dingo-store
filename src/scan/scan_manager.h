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

#include "scan/scan.h"

namespace dingodb {

template <typename T>
class RawScanManagerSingleton {
 public:
  static T& GetInstance() noexcept(std::is_nothrow_constructible_v<T>) {
    static T instance{};
    return instance;
  }
  virtual ~RawScanManagerSingleton() = default;
  RawScanManagerSingleton(const RawScanManagerSingleton&) = delete;
  RawScanManagerSingleton& operator=(const RawScanManagerSingleton&) = delete;
  RawScanManagerSingleton(RawScanManagerSingleton&& rhs) = delete;
  RawScanManagerSingleton& operator=(RawScanManagerSingleton&& rhs) = delete;

 protected:
  RawScanManagerSingleton() noexcept = default;
};

class RawScanManager {
 public:
  RawScanManager(const RawScanManager& rhs) = delete;
  RawScanManager& operator=(const RawScanManager& rhs) = delete;
  RawScanManager(RawScanManager&& rhs) = delete;
  RawScanManager& operator=(RawScanManager&& rhs) = delete;

  virtual bool Init(std::shared_ptr<Config> config);

  virtual std::shared_ptr<ScanContext> CreateScan(std::string* scan_id);
  virtual std::shared_ptr<ScanContext> FindScan(const std::string& scan_id);
  virtual void DeleteScan(const std::string& scan_id);
  virtual void TryDeleteScan(const std::string& scan_id);

  virtual std::shared_ptr<ScanContext> CreateScan(int64_t scan_id);
  virtual std::shared_ptr<ScanContext> FindScan(int64_t scan_id);
  virtual void DeleteScan(int64_t scan_id);
  virtual void TryDeleteScan(int64_t scan_id);

  int64_t GetTimeoutMs() const { return timeout_ms; }
  int64_t GetMaxBytesRpc() const { return max_bytes_rpc; }
  int64_t GetMaxFetchCntByServer() const { return max_fetch_cnt_by_server; }
  int64_t GetScanIntervalMs() const { return scan_interval_ms; }

  RawScanManager();
  virtual ~RawScanManager();

 protected:
  int64_t timeout_ms;
  int64_t max_bytes_rpc;
  int64_t max_fetch_cnt_by_server;
  int64_t scan_interval_ms;
  bthread_mutex_t mutex;
};

class ScanManager : public RawScanManager, public RawScanManagerSingleton<ScanManager> {
 public:
  // ScanManager(const ScanManager& rhs) = delete;
  // ScanManager& operator=(const ScanManager& rhs) = delete;
  // ScanManager(ScanManager&& rhs) = delete;
  // ScanManager& operator=(ScanManager&& rhs) = delete;

  bool Init(std::shared_ptr<Config> config) override;

  std::shared_ptr<ScanContext> CreateScan(std::string* scan_id) override;
  std::shared_ptr<ScanContext> FindScan(const std::string& scan_id) override;
  void DeleteScan(const std::string& scan_id) override;
  void TryDeleteScan(const std::string& scan_id) override;

  static void RegularCleaningHandler(void* arg);

  ScanManager();
  ~ScanManager() override;

 private:
  std::map<std::string, std::shared_ptr<ScanContext>> alive_scans_;
  std::map<std::string, std::shared_ptr<ScanContext>> waiting_destroyed_scans_;
  bvar::Adder<uint64_t> bvar_scan_v1_object_running_num_;
  bvar::Adder<uint64_t> bvar_scan_v1_object_total_num_;
};

class ScanManagerV2 : public RawScanManager, public RawScanManagerSingleton<ScanManagerV2> {
 public:
  // ScanManagerV2(const ScanManagerV2& rhs) = delete;
  // ScanManagerV2& operator=(const ScanManagerV2& rhs) = delete;
  // ScanManagerV2(ScanManagerV2&& rhs) = delete;
  // ScanManagerV2& operator=(ScanManagerV2&& rhs) = delete;

  bool Init(std::shared_ptr<Config> config) override;

  std::shared_ptr<ScanContext> CreateScan(int64_t scan_id) override;
  std::shared_ptr<ScanContext> FindScan(int64_t scan_id) override;
  void DeleteScan(int64_t scan_id) override;
  void TryDeleteScan(int64_t scan_id) override;

  static void RegularCleaningHandler(void* arg);

  ScanManagerV2();
  ~ScanManagerV2() override;

 private:
  std::map<int64_t, std::shared_ptr<ScanContext>> alive_scans_;
  std::map<int64_t, std::shared_ptr<ScanContext>> waiting_destroyed_scans_;
  bvar::Adder<uint64_t> bvar_scan_v2_object_running_num_;
  bvar::Adder<uint64_t> bvar_scan_v2_object_total_num_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_SCAN_MANAGER_H_   // NOLINT
