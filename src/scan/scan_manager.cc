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
#include "scan/scan_manager.h"

#include <memory>

#include "butil/guid.h"
#include "common/constant.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

RawScanManager::RawScanManager()
    : timeout_ms(60 * 1000),
      max_bytes_rpc(4 * 1024 * 1024),
      max_fetch_cnt_by_server(1000),
      scan_interval_ms(60 * 1000) {
  bthread_mutex_init(&mutex, nullptr);
}

RawScanManager::~RawScanManager() {
  timeout_ms = 60 * 1000;
  max_bytes_rpc = 4 * 1024 * 1024;
  max_fetch_cnt_by_server = 1000;
  scan_interval_ms = 60 * 1000;
  // alive_scans_.clear();
  // waiting_destroyed_scans_.clear();
  bthread_mutex_destroy(&mutex);
}

bool RawScanManager::Init(std::shared_ptr<Config> /*config*/) { return false; }

std::shared_ptr<ScanContext> RawScanManager::CreateScan(std::string* /*scan_id*/) { return nullptr; }
std::shared_ptr<ScanContext> RawScanManager::FindScan(const std::string& /*scan_id*/) { return nullptr; }
void RawScanManager::DeleteScan(const std::string& scan_id) {}
void RawScanManager::TryDeleteScan(const std::string& scan_id) {}

std::shared_ptr<ScanContext> RawScanManager::CreateScan(int64_t /*scan_id*/) { return nullptr; }
std::shared_ptr<ScanContext> RawScanManager::FindScan(int64_t /*scan_id*/) { return nullptr; }
void RawScanManager::DeleteScan(int64_t scan_id) {}
void RawScanManager::TryDeleteScan(int64_t scan_id) {}

ScanManager::ScanManager()
    : bvar_scan_v1_object_running_num_("dingo_scan_v1_object_running_num"),
      bvar_scan_v1_object_total_num_("dingo_scan_v1_object_total_num") {}
ScanManager::~ScanManager() {
  alive_scans_.clear();
  waiting_destroyed_scans_.clear();
}

bool ScanManager::Init(std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex);
  std::map<std::string, int> conf = config->GetIntMap(Constant::kStoreScan);

  auto iter = conf.find(Constant::kStoreScanTimeoutS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      timeout_ms = iter->second * 1000;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxBytesRpc);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_bytes_rpc = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxFetchCntByServer);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_fetch_cnt_by_server = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanScanIntervalS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      scan_interval_ms = iter->second * 1000;
    }
  }

  return true;
}

std::shared_ptr<ScanContext> ScanManager::CreateScan(std::string* scan_id) {
  BAIDU_SCOPED_LOCK(mutex);

  while (true) {
    *scan_id = butil::GenerateGUID();
    // If GUID generation fails an empty string is returned.
    // retry
    if (!scan_id->empty()) {
      // carefully consider. whether the test is repeated
      if (auto iter = alive_scans_.find(*scan_id); iter == alive_scans_.end()) {
        break;
      }
    }
  }

  auto scan = std::make_shared<ScanContextV1>(ScanContextV1::GetScanLatency());
  scan->Init(timeout_ms, max_bytes_rpc, max_fetch_cnt_by_server);
  alive_scans_[*scan_id] = scan;
  bvar_scan_v1_object_running_num_ << 1;
  bvar_scan_v1_object_total_num_ << 1;

  return scan;
}

std::shared_ptr<ScanContext> ScanManager::FindScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    return iter->second;
  }
  return nullptr;
}

void ScanManager::DeleteScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    // free memory directly
    iter->second.reset();
    alive_scans_.erase(iter);
    bvar_scan_v1_object_running_num_ << -1;
    return;
  }
}

void ScanManager::TryDeleteScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    if (iter->second->IsRecyclable()) {
      // free memory directly
      iter->second.reset();
      alive_scans_.erase(iter);
      bvar_scan_v1_object_running_num_ << -1;
    }
  }
}

void ScanManager::RegularCleaningHandler(void*) {
  static std::atomic<bool> g_scan_manager_regular_cleaning_handler_running(false);

  if (g_scan_manager_regular_cleaning_handler_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO)
        << "RegularUpdateSafePointTsHandler... g_scan_manager_regular_cleaning_handler_running is true, return";
    return;
  }

  AtomicGuard guard(g_scan_manager_regular_cleaning_handler_running);

  ScanManager& manager = ScanManager::GetInstance();

  BAIDU_SCOPED_LOCK(manager.mutex);
  for (auto iter = manager.alive_scans_.begin(); iter != manager.alive_scans_.end();) {
    if (iter->second->IsRecyclable()) {
      manager.waiting_destroyed_scans_[iter->first] = iter->second;
      manager.alive_scans_.erase(iter++);
      manager.bvar_scan_v1_object_running_num_ << -1;
    } else {
      iter++;
    }
  }
  manager.waiting_destroyed_scans_.clear();
}

ScanManagerV2::ScanManagerV2()
    : bvar_scan_v2_object_running_num_("dingo_scan_v2_object_running_num"),
      bvar_scan_v2_object_total_num_("dingo_scan_v2_object_total_num") {}
ScanManagerV2::~ScanManagerV2() {
  alive_scans_.clear();
  waiting_destroyed_scans_.clear();
}

bool ScanManagerV2::Init(std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex);
  std::map<std::string, int> conf = config->GetIntMap(Constant::kStoreScanV2);

  auto iter = conf.find(Constant::kStoreScanTimeoutS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      timeout_ms = iter->second * 1000;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxBytesRpc);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_bytes_rpc = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxFetchCntByServer);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_fetch_cnt_by_server = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanScanIntervalS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      scan_interval_ms = iter->second * 1000;
    }
  }

  return true;
}

std::shared_ptr<ScanContext> ScanManagerV2::CreateScan(int64_t scan_id) {
  BAIDU_SCOPED_LOCK(mutex);

  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    DINGO_LOG(ERROR) << fmt::format("ScanManagerV2::CreateScan failed,  exist same scan_id : {}", scan_id);
    return nullptr;
  }

  auto scan = std::make_shared<ScanContextV2>(ScanContextV2::GetScanLatency());
  scan->Init(timeout_ms, max_bytes_rpc, max_fetch_cnt_by_server);
  alive_scans_[scan_id] = scan;
  bvar_scan_v2_object_running_num_ << 1;
  bvar_scan_v2_object_total_num_ << 1;

  return scan;
}

std::shared_ptr<ScanContext> ScanManagerV2::FindScan(int64_t scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    return iter->second;
  }
  return nullptr;
}

void ScanManagerV2::DeleteScan(int64_t scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    // free memory directly
    iter->second.reset();
    alive_scans_.erase(iter);
    bvar_scan_v2_object_running_num_ << -1;
    return;
  }
}

void ScanManagerV2::TryDeleteScan(int64_t scan_id) {
  BAIDU_SCOPED_LOCK(mutex);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    if (iter->second->IsRecyclable()) {
      // free memory directly
      iter->second.reset();
      alive_scans_.erase(iter);
      bvar_scan_v2_object_running_num_ << -1;
    }
  }
}

void ScanManagerV2::RegularCleaningHandler(void* /*arg*/) {
  static std::atomic<bool> g_scan_manager_v2_regular_cleaning_handler_running(false);

  if (g_scan_manager_v2_regular_cleaning_handler_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO)
        << "RegularUpdateSafePointTsHandler... g_scan_manager_v2_regular_cleaning_handler_running is true, return";
    return;
  }

  AtomicGuard guard(g_scan_manager_v2_regular_cleaning_handler_running);

  ScanManagerV2& manager = ScanManagerV2::GetInstance();

  BAIDU_SCOPED_LOCK(manager.mutex);
  for (auto iter = manager.alive_scans_.begin(); iter != manager.alive_scans_.end();) {
    if (iter->second->IsRecyclable()) {
      manager.waiting_destroyed_scans_[iter->first] = iter->second;
      manager.alive_scans_.erase(iter++);
      manager.bvar_scan_v2_object_running_num_ << -1;
    } else {
      iter++;
    }
  }
  manager.waiting_destroyed_scans_.clear();
}

}  // namespace dingodb
