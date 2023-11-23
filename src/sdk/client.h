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
#include <unordered_map>
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
};

struct KeyOpState {
  std::string key;
  bool state;
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

  Status PutIfAbsent(const std::string& key, const std::string& value, bool& state);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& states);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  // NOTE: start must < end
  // output_param: delete_count
  Status DeleteRange(const std::string& start, const std::string& end, int64_t& delete_count, bool with_start = true,
                     bool with_end = false);

  // expected_value: empty means key not exist
  Status CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                       bool& state);

  // expected_values size must equal kvs size
  Status BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                            std::vector<KeyOpState>& states);

  // limit: 0 means no limit, will scan all key in [start_key, end_key)
  Status Scan(const std::string& start_key, const std::string& end_key,  uint64_t limit, std::vector<KVPair>& kvs);

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