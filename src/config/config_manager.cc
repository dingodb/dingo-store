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
#include "butil/strings/stringprintf.h"
#include "glog/logging.h"

namespace dingodb {

ConfigManager::ConfigManager() {}
ConfigManager::~ConfigManager() {}

ConfigManager *ConfigManager::GetInstance() {
  return Singleton<ConfigManager>::get();
}

bool ConfigManager::IsExist(const std::string &name) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return configs_.find(name) != configs_.end();
}

void ConfigManager::Register(const std::string &name,
                             std::shared_ptr<Config> config) {
  if (IsExist(name)) {
    LOG(WARNING) << butil::StringPrintf("config %s already exist!",
                                        name.c_str());
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  configs_[name] = config;
}

std::shared_ptr<Config> ConfigManager::GetConfig(const std::string &name) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = configs_.find(name);
  if (it == configs_.end()) {
    LOG(WARNING) << butil::StringPrintf("config %s not exist!", name.c_str());
    return nullptr;
  }

  return it->second;
}

}  // namespace dingodb