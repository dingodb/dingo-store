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

#ifndef DINGODB_CONFIG_MANAGER_H_
#define DINGODB_CONFIG_MANAGER_H_

#include <map>
#include <memory>
#include <string>

#include "bthread/types.h"
#include "config/config.h"

namespace dingodb {

// Manage all config
class ConfigManager {
 public:
  static ConfigManager &GetInstance();

  bool IsExist(const std::string &name);
  void Register(const std::string &name, std::shared_ptr<Config> config);
  std::shared_ptr<Config> GetConfig(const std::string &name);
  std::shared_ptr<Config> GetRoleConfig();

  ConfigManager(const ConfigManager &) = delete;
  const ConfigManager &operator=(const ConfigManager &) = delete;

 private:
  ConfigManager();
  ~ConfigManager();

  bthread_mutex_t mutex_;
  std::map<std::string, std::shared_ptr<Config> > configs_;
};

}  // namespace dingodb

#endif  // DINGODB_CONFIG_MANAGER_H
