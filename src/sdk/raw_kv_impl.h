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

#ifndef DINGODB_SDK_RAW_KV_IMPL_H_
#define DINGODB_SDK_RAW_KV_IMPL_H_

#include <memory>

#include "proto/store.pb.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/meta_cache.h"

namespace dingodb {
namespace sdk {
class RawKV::RawKVImpl {
 public:
  RawKVImpl(const RawKVImpl&) = delete;
  const RawKVImpl& operator=(const RawKVImpl&) = delete;

  explicit RawKVImpl(const ClientStub& stub);

  ~RawKVImpl() = default;

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status Delete(const std::string& key);

  Status PutIfAbsent(const std::string& key, const std::string& value);

 private:
  const ClientStub& stub_;
};
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RAW_KV_IMPL_H_