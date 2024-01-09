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

#include <cstdint>
#include <typeinfo>

#include "butil/strings/string_util.h"
#include "common/logging.h"

namespace dingodb {

// Load config from string
int YamlConfig::Load(const std::string& data) {
  int new_active_index = active_index_.load() == 0 ? 1 : 0;
  configs_[new_active_index] = YAML::Load(data);
  active_index_.store(new_active_index);
  return 0;
}

// Load config from file
int YamlConfig::LoadFile(const std::string& filename) {
  int new_active_index = active_index_.load() == 0 ? 1 : 0;
  configs_[new_active_index] = YAML::LoadFile(filename);
  active_index_.store(new_active_index);

  return 0;
}

int YamlConfig::ReloadFile(const std::string& filename) { return Load(filename); }

bool YamlConfig::GetBool(const std::string& key) {
  try {
    return GetScalar<bool>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetBool failed: " << key << " exception: " << e.what();
  }
  return false;
}

int YamlConfig::GetInt(const std::string& key) {
  try {
    return GetScalar<int>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetInt failed: " << key << " exception: " << e.what();
  }
  return -1;
}

int64_t YamlConfig::GetInt64(const std::string& key) {
  try {
    return GetScalar<int64_t>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetInt64 failed: " << key << " exception: " << e.what();
  }
  return -1;
}

double YamlConfig::GetDouble(const std::string& key) {
  try {
    return GetScalar<double>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetDouble failed: " << key << " exception: " << e.what();
  }
  return -1.0;
}

std::string YamlConfig::GetString(const std::string& key) {
  try {
    const std::string s = GetScalar<std::string>(key);
    std::string result;
    butil::TrimWhitespaceASCII(s, butil::TrimPositions::TRIM_ALL, &result);
    return result;
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetString failed: " << key << " exception: " << e.what();
  }
  return "";
}

std::vector<int> YamlConfig::GetIntList(const std::string& key) {
  try {
    return GetList<int>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetIntList failed: " << key << " exception: " << e.what();
  }

  return std::vector<int>{};
}

std::vector<std::string> YamlConfig::GetStringList(const std::string& key) {
  try {
    return GetList<std::string>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetStringList failed: " << key << " exception: " << e.what();
  }

  return std::vector<std::string>{};
}

std::map<std::string, int> YamlConfig::GetIntMap(const std::string& key) {
  try {
    return GetMap<int>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetIntMap failed: " << key << " exception: " << e.what();
  }

  return std::map<std::string, int>{};
}

std::map<std::string, std::string> YamlConfig::GetStringMap(const std::string& key) {
  try {
    return GetMap<std::string>(key);
  } catch (std::exception& e) {
    DINGO_LOG(FATAL) << "[config] Config GetStringMap failed: " << key << " exception: " << e.what();
  }

  return std::map<std::string, std::string>{};
}

// Get scalar value
template <typename T>
T YamlConfig::GetScalar(const std::string& key) {
  std::vector<std::string> tokens;
  butil::SplitString(key, '.', &tokens);

  YAML::Node node = YAML::Clone(configs_[active_index_]);

  for (auto& token : tokens) {
    node = node[token];
  }

  return node.IsDefined() ? node.as<T>() : T();
}

// Get list value
template <typename T>
std::vector<T> YamlConfig::GetList(const std::string& key) {
  std::vector<std::string> tokens;
  butil::SplitString(key, '.', &tokens);

  YAML::Node node = YAML::Clone(configs_[active_index_]);
  for (auto& token : tokens) {
    node = node[token];
    if (!node.IsDefined()) {
      return {};
    }
  }

  return node.as<std::vector<T> >();
}

// Get map value
template <typename T>
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

std::string YamlConfig::ToString() {
  // ToDo return String
  // Concat all configuration to String
  return "";
}

}  // namespace dingodb
