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

#include "config/config_manager.h"

#include <memory>

#include "bthread/mutex.h"
#include "butil/scoped_lock.h"
#include "common/logging.h"
#include "common/role.h"
#include "fmt/core.h"

namespace dingodb {

ConfigManager::ConfigManager() { bthread_mutex_init(&mutex_, nullptr); }
ConfigManager::~ConfigManager() { bthread_mutex_destroy(&mutex_); }

ConfigManager& ConfigManager::GetInstance() {
  static ConfigManager instance;
  return instance;
}

bool ConfigManager::IsExist(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);
  return configs_.find(name) != configs_.end();
}

void ConfigManager::Register(const std::string& name, std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (configs_.find(name) != configs_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[config] config {} already exist!", name);
    return;
  }

  configs_[name] = config;
}

std::shared_ptr<Config> ConfigManager::GetConfig(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = configs_.find(name);
  if (it == configs_.end()) {
    DINGO_LOG(ERROR) << fmt::format("[config] config {} not exist!", name);
    return nullptr;
  }

  return it->second;
}

std::shared_ptr<Config> ConfigManager::GetRoleConfig() {
  auto it = configs_.find(GetRoleName());
  if (it == configs_.end()) {
    DINGO_LOG(ERROR) << fmt::format("[config] config {} not exist!", GetRoleName());
    return nullptr;
  }

  return it->second;
}

}  // namespace dingodb
