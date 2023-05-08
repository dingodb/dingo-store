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

#include "butil/memory/singleton.h"
#include "butil/scoped_lock.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace dingodb {

ConfigManager::ConfigManager() { bthread_mutex_init(&mutex_, nullptr); }
ConfigManager::~ConfigManager() { bthread_mutex_destroy(&mutex_); }

ConfigManager *ConfigManager::GetInstance() { return Singleton<ConfigManager>::get(); }

bool ConfigManager::IsExist(pb::common::ClusterRole role) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto name = pb::common::ClusterRole_Name(role);
  return configs_.find(name) != configs_.end();
}

void ConfigManager::Register(pb::common::ClusterRole role, std::shared_ptr<Config> config) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto name = pb::common::ClusterRole_Name(role);
  if (configs_.find(name) != configs_.end()) {
    DINGO_LOG(WARNING) << fmt::format("config {} already exist!", name);
    return;
  }

  configs_[name] = config;
}

std::shared_ptr<Config> ConfigManager::GetConfig(pb::common::ClusterRole role) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto name = pb::common::ClusterRole_Name(role);
  auto it = configs_.find(name);
  if (it == configs_.end()) {
    DINGO_LOG(WARNING) << fmt::format("config {} not exist!", name);
    return nullptr;
  }

  return it->second;
}

}  // namespace dingodb
