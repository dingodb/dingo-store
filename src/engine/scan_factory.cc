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
#include "engine/scan_factory.h"

#include <memory>

#include "butil/guid.h"
#include "common/constant.h"
#include "common/logging.h"

namespace dingodb {

ScanContextFactory::ScanContextFactory()
    : timeout_ms_(60 * 1000),
      max_bytes_rpc_(4 * 1024 * 1024),
      max_fetch_cnt_by_server_(1000),
      scan_interval_ms_(60 * 1000) {
  bthread_mutex_init(&mutex_, nullptr);
}
ScanContextFactory::~ScanContextFactory() {
  timeout_ms_ = 60 * 1000;
  max_bytes_rpc_ = 4 * 1024 * 1024;
  max_fetch_cnt_by_server_ = 1000;
  scan_interval_ms_ = 60 * 1000;
  alive_scans_.clear();
  waiting_destroyed_scans_.clear();
  bthread_mutex_destroy(&mutex_);
}

ScanContextFactory* ScanContextFactory::GetInstance() { return Singleton<ScanContextFactory>::get(); }

bool ScanContextFactory::Init(std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::map<std::string, int> conf = config->GetIntMap(Constant::kStoreScan);

  auto iter = conf.find(Constant::kStoreScanTimeoutMs);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      timeout_ms_ = iter->second;
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

  iter = conf.find(Constant::kStoreScanScanIntervalMs);
  if (iter != conf.end()) {
    if (iter->second != 0) {
      scan_interval_ms_ = iter->second;
    }
  }

  return true;
}

std::shared_ptr<ScanContext> ScanContextFactory::CreateScan(std::string* scan_id) {
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

std::shared_ptr<ScanContext> ScanContextFactory::FindScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    return iter->second;
  }
  return nullptr;
}

void ScanContextFactory::DeleteScan(const std::string& scan_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto iter = alive_scans_.find(scan_id);
  if (iter != alive_scans_.end()) {
    // free memory directly
    iter->second.reset();
    alive_scans_.erase(iter);
    return;
  }
}

void ScanContextFactory::TryDeleteScan(const std::string& scan_id) {
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

void ScanContextFactory::RegularCleaningHandler(void* arg) {
  std::cout << "void ScanContextFactory::RegularCleaningHandler(void* arg)" << std::endl;
  ScanContextFactory* factory = static_cast<ScanContextFactory*>(arg);

  BAIDU_SCOPED_LOCK(factory->mutex_);
  for (auto iter = factory->alive_scans_.begin(); iter != factory->alive_scans_.end();) {
    if (iter->second->IsRecyclable()) {
      factory->waiting_destroyed_scans_[iter->first] = iter->second;
      factory->alive_scans_.erase(iter++);
    } else {
      iter++;
    }
  }
  factory->waiting_destroyed_scans_.clear();
}

}  // namespace dingodb
