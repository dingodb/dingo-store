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

#ifndef DINGODB_CONFIG_H_
#define DINGODB_CONFIG_H_

#include <map>
#include <string>
#include <vector>

namespace dingodb {

class Config {
 public:
  Config() = default;
  virtual ~Config() = default;

  virtual int Load(const std::string& data) = 0;
  virtual int LoadFile(const std::string& filename) = 0;
  virtual int ReloadFile(const std::string& filename) = 0;

  virtual bool GetBool(const std::string& key) = 0;
  virtual int GetInt(const std::string& key) = 0;
  virtual int64_t GetInt64(const std::string& key) = 0;
  virtual double GetDouble(const std::string& key) = 0;
  virtual std::string GetString(const std::string& key) = 0;

  virtual std::vector<int> GetIntList(const std::string& key) = 0;
  virtual std::vector<std::string> GetStringList(const std::string& key) = 0;

  virtual std::map<std::string, int> GetIntMap(const std::string& key) = 0;
  virtual std::map<std::string, std::string> GetStringMap(const std::string& key) = 0;

  virtual std::string ToString() = 0;
};

}  // namespace dingodb

#endif  // DINGODB_CONFIG_H_
