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

#ifndef DINGODB_SDK_CLIENT_H_
#define DINGODB_SDK_CLIENT_H_

#include <memory>
#include <string>
#include <vector>

#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class RawKV;
class TestBase;

class Client : public std::enable_shared_from_this<Client> {
 public:
  Client(const Client&) = delete;
  const Client& operator=(const Client&) = delete;

  ~Client();

  static Status Build(std::string naming_service_url, std::shared_ptr<Client>& client);

  Status NewRawKV(std::shared_ptr<RawKV>& raw_kv);

 private:
  friend class RawKV;

  Client();

  Status Init(std::string naming_service_url);

  // own
  class ClientImpl;
  std::unique_ptr<ClientImpl> impl_;
};

struct KVPair {
  std::string key;
  std::string value;

  KVPair(std::string&& p_key, std::string&& p_value) : key(std::move(p_key)), value(std::move(p_value)) {}
};

class RawKV : public std::enable_shared_from_this<RawKV> {
 public:
  RawKV(const RawKV&) = delete;
  const RawKV& operator=(const RawKV&) = delete;

  ~RawKV();

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  // TODO scan

  Status Delete(const std::string& key);

  Status PutIfAbsent(const std::string& key, const std::string& value);

 private:
  friend class Client;
  friend class TestBase;

  // own
  class RawKVImpl;
  std::unique_ptr<RawKVImpl> impl_;

  explicit RawKV(RawKVImpl* impl);
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_CLIENT_H_