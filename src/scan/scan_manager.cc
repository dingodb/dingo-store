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

namespace dingodb {

ScanManager::ScanManager()
    : timeout_ms_(60 * 1000),
      max_bytes_rpc_(4 * 1024 * 1024),
      max_fetch_cnt_by_server_(1000),
      scan_interval_ms_(60 * 1000) {
  bthread_mutex_init(&mutex_, nullptr);
}
ScanManager::~ScanManager() {
  timeout_ms_ = 60 * 1000;
  max_bytes_rpc_ = 4 * 1024 * 1024;
  max_fetch_cnt_by_server_ = 1000;
  scan_interval_ms_ = 60 * 1000;
  alive_scans_.clear();
  waiting_destroyed_scans_.clear();
  bthread_mutex_destroy(&mutex_);
}

ScanManager& ScanManager::GetInstance() {
  static ScanManager instance;
  return instance;
}

bool ScanManager::Init(std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::map<std::string, int> conf = config->GetIntMap(Constant::kStoreScan);

  auto iter = conf.find(Constant::kStoreScanTimeoutS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      timeout_ms_ = iter->second * 1000;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxBytesRpc);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_bytes_rpc_ = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanMaxFetchCntByServer);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      max_fetch_cnt_by_server_ = iter->second;
    }
  }

  iter = conf.find(Constant::kStoreScanScanIntervalS);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      scan_interval_ms_ = iter->second * 1000;
    }
  }

  ScanContext::Init(timeout_ms_, max_bytes_rpc_, max_fetch_cnt_by_server_);

  return true;
}

std::shared_ptr<ScanContext> ScanManager::CreateScan(std::string* scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);

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

  auto scan = std::make_shared<ScanContext>();
  alive_scans_[*scan_id] = scan;

  return scan;
}

std::shared_ptr<ScanContext> ScanManager::FindScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    return iter->second;
  }
  return nullptr;
}

void ScanManager::DeleteScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    // free memory directly
    iter->second.reset();
    alive_scans_.erase(iter);
    return;
  }
}

void ScanManager::TryDeleteScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    if (iter->second->IsRecyclable()) {
      // free memory directly
      iter->second.reset();
      alive_scans_.erase(iter);
    }
  }
}

void ScanManager::RegularCleaningHandler(void*) {
  ScanManager& manager = ScanManager::GetInstance();

  BAIDU_SCOPED_LOCK(manager.mutex_);
  for (auto iter = manager.alive_scans_.begin(); iter != manager.alive_scans_.end();) {
    if (iter->second->IsRecyclable()) {
      manager.waiting_destroyed_scans_[iter->first] = iter->second;
      manager.alive_scans_.erase(iter++);
    } else {
      iter++;
    }
  }
  manager.waiting_destroyed_scans_.clear();
}

}  // namespace dingodb
