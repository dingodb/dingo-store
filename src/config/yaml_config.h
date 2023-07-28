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

#ifndef DINGODB_YAML_CONFIG_H_
#define DINGODB_YAML_CONFIG_H_

#include <butil/strings/string_split.h>

#include <atomic>
#include <iostream>
#include <map>
#include <vector>

#include "config/config.h"
#include "yaml-cpp/yaml.h"

namespace dingodb {

// Handle yaml config use yaml-cpp
class YamlConfig : public Config {
 public:
  YamlConfig() : active_index_(0) {}
  ~YamlConfig() override = default;

  int Load(const std::string& data) override;
  int LoadFile(const std::string& filename) override;
  int ReloadFile(const std::string& filename) override;

  bool GetBool(const std::string& key) override;
  int GetInt(const std::string& key) override;
  int64_t GetInt64(const std::string& key) override;
  double GetDouble(const std::string& key) override;
  std::string GetString(const std::string& key) override;

  std::vector<int> GetIntList(const std::string& key) override;
  std::vector<std::string> GetStringList(const std::string& key) override;

  std::map<std::string, int> GetIntMap(const std::string& key) override;
  std::map<std::string, std::string> GetStringMap(const std::string& key) override;

  std::string ToString() override;

 private:
  template <typename T>
  T GetScalar(const std::string& key);
  template <typename T>
  std::vector<T> GetList(const std::string& key);
  template <typename T>
  std::map<std::string, T> GetMap(const std::string& key);

  std::atomic<int> active_index_;
  YAML::Node configs_[2];
};

}  // namespace dingodb

#endif  // DINGODB_YAML_CONFIG_H_
