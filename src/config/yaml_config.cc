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


#include "config/yaml_config.h"

namespace dingodb {


// Load config from string
int YamlConfig::Load(const std::string& data) {
  int new_active_index_ = active_index_.load() == 0? 1: 0;
  configs_[new_active_index_] = YAML::Load(data);
  active_index_.store(new_active_index_);

  return 0;
}

// Load config from file
int YamlConfig::LoadFile(const std::string& filename) {
  int new_active_index_ = active_index_.load() == 0? 1: 0;
  configs_[new_active_index_] = YAML::LoadFile(filename);
  active_index_.store(new_active_index_);

  return 0;
}

int YamlConfig::ReloadFile(const std::string& filename) {
  return Load(filename);
}

int YamlConfig::GetInt(const std::string& key) {
  return GetScalar<int>(key);
}

std::string YamlConfig::GetString(const std::string& key) {
  return GetScalar<std::string>(key);
}

std::vector<int> YamlConfig::GetIntList(const std::string& key) {
  return GetList<int>(key);
}

std::vector<std::string> YamlConfig::GetStringList(const std::string& key) {
  return GetList<std::string>(key);
}

std::map<std::string, int> YamlConfig::GetIntMap(const std::string& key) {
  return GetMap<int>(key);
}

std::map<std::string, std::string> YamlConfig::GetStringMap(const std::string& key) {
  return GetMap<std::string>(key);
}

// Get scalar value
template<typename T>
T YamlConfig::GetScalar(const std::string& key) {
  std::vector<std::string> tokens;
  butil::SplitString(key, '.', &tokens);

  YAML::Node node = YAML::Clone(configs_[active_index_]);

  for (auto& token : tokens) {
    node = node[token];
  }

  return node.as<T>();
}

// Get list value
template<typename T>
std::vector<T> YamlConfig::GetList(const std::string& key) {
  std::vector<std::string> tokens;
  butil::SplitString(key, '.', &tokens);

  YAML::Node node = YAML::Clone(configs_[active_index_]);
  for (auto& token : tokens) {
    node = node[token];
  }

  return node.as<std::vector<T> >();
}

// Get map value
template<typename T>
std::map<std::string, T> YamlConfig::GetMap(const std::string& key) {
  std::vector<std::string> tokens;
  butil::SplitString(key, '.', &tokens);

  YAML::Node node = YAML::Clone(configs_[active_index_]);
  for (auto& token : tokens) {
    node = node[token];
  }

  std::map<std::string, T> result;
  for (auto it = node.begin(); it != node.end(); ++it) {
    result[it->first.as<std::string>()] = it->second.as<T>();
  }

  return result;
}


} // namespace dingodb
