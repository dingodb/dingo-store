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

#include "diskann/diskann_item_manager.h"

#include <omp.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "diskann/diskann_utils.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

DiskANNItemManager::DiskANNItemManager()
    : num_threads_(Constant::kDiskannNumThreadsDefaultValue),
      search_dram_budget_gb_(Constant::kDiskannSearchDramBudgetGbDefaultValue),
      build_dram_budget_gb_(Constant::kDiskannBuildDramBudgetGbDefaultValue),
      import_timeout_s_(Constant::kDiskannImportTimeoutSecondDefaultValue) {}

DiskANNItemManager::~DiskANNItemManager() {
  RWLockWriteGuard guard(&rw_lock_);
  items_.clear();
}

bool DiskANNItemManager::Init(std::shared_ptr<Config> config) {
  RWLockWriteGuard guard(&rw_lock_);
  std::map<std::string, std::string> conf = config->GetStringMap(Constant::kDiskannStore);

  if (auto iter = conf.find(Constant::kDiskannPathConfigName); iter != conf.end()) {
    path_ = iter->second;
    if (path_.empty()) {
      DINGO_LOG(ERROR) << "DiskANN path is empty";
      return false;
    }

    butil::Status status = DiskANNUtils::DirExists(path_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "DiskANN path " << path_ << " does not exist";
      return false;
    }

    if (path_.back() == '/') {
      path_.pop_back();
    }
  }

  if (auto iter = conf.find(Constant::kDiskannNumThreadsConfigName); iter != conf.end()) {
    try {
      decltype(num_threads_) num_threads;
      num_threads = std::stoi(iter->second);
      num_threads_ = num_threads;
    } catch (std::exception& e) {
    }
  }

  if (auto iter = conf.find(Constant::kDiskannSearchDramBudgetGbConfigName); iter != conf.end()) {
    try {
      decltype(search_dram_budget_gb_) search_dram_budget_gb;
      search_dram_budget_gb = std::stof(iter->second);
      if (search_dram_budget_gb > 1e-6) {
        search_dram_budget_gb_ = search_dram_budget_gb;
      }
    } catch (std::exception& e) {
    }
  }

  if (auto iter = conf.find(Constant::kDiskannBuildDramBudgetGbConfigName); iter != conf.end()) {
    try {
      decltype(build_dram_budget_gb_) build_dram_budget_gb;
      build_dram_budget_gb = std::stof(iter->second);
      if (build_dram_budget_gb > 1e-6) {
        build_dram_budget_gb_ = build_dram_budget_gb;
      }
    } catch (std::exception& e) {
    }
  }

  if (auto iter = conf.find(Constant::kDiskannImportTimeoutSecondConfigName); iter != conf.end()) {
    try {
      decltype(import_timeout_s_) import_timeout_s;
      import_timeout_s = std::stol(iter->second);
      if (import_timeout_s > 0) {
        import_timeout_s_ = import_timeout_s;
      }
    } catch (std::exception& e) {
    }
  }

  DiskANNItem::SetImportTimeout(import_timeout_s_);
  DiskANNItem::SetBaseDir(path_);

  return true;
}

std::shared_ptr<DiskANNItem> DiskANNItemManager::Create(
    std::shared_ptr<Context> ctx, int64_t vector_index_id,
    const pb::common::VectorIndexParameter& vector_index_parameter) {
  RWLockWriteGuard guard(&rw_lock_);
  auto iter = items_.find(vector_index_id);
  if (iter != items_.end()) {
    DINGO_LOG(ERROR) << "DiskANNItem id " << vector_index_id << " already exists";
    return nullptr;
  }

  std::shared_ptr<DiskANNItem> item = std::make_shared<DiskANNItem>(
      ctx, vector_index_id, vector_index_parameter, num_threads_, search_dram_budget_gb_, build_dram_budget_gb_);

  items_.insert(std::make_pair(vector_index_id, item));

  return item;
}

std::shared_ptr<DiskANNItem> DiskANNItemManager::Find(int64_t vector_index_id) {
  RWLockReadGuard guard(&rw_lock_);
  auto iter = items_.find(vector_index_id);
  if (iter == items_.end()) {
    return nullptr;
  }

  return iter->second;
}

void DiskANNItemManager::Delete(int64_t vector_index_id) {
  RWLockWriteGuard guard(&rw_lock_);
  auto iter = items_.find(vector_index_id);
  if (iter != items_.end()) {
    items_.erase(iter);
  }
}

std::vector<std::shared_ptr<DiskANNItem>> DiskANNItemManager::FindAll() {
  std::vector<std::shared_ptr<DiskANNItem>> items;
  RWLockReadGuard guard(&rw_lock_);
  items.reserve(items_.size());
  for (auto& item : items_) {
    items.push_back(item.second);
  }

  return items;
}

}  // namespace dingodb
