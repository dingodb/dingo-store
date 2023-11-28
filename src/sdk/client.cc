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

#include "sdk/client.h"

#include <charconv>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/error.pb.h"
#include "sdk/client_impl.h"
#include "sdk/raw_kv_impl.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

Status Client::Build(std::string naming_service_url, std::shared_ptr<Client>& client) {
  if (naming_service_url.empty()) {
    return Status::InvalidArgument("naming_service_url is empty");
  };

  std::shared_ptr<Client> tmp(new Client());

  Status s = tmp->Init(std::move(naming_service_url));
  if (s.IsOK()) {
    client = tmp;
  }

  return s;
}

Client::Client() : impl_(new Client::ClientImpl()) {}

Client::~Client() { impl_.reset(nullptr); }

Status Client::Init(std::string naming_service_url) {
  CHECK(!naming_service_url.empty());
  return impl_->Init(std::move(naming_service_url));
}

Status Client::NewRawKV(std::shared_ptr<RawKV>& raw_kv) {
  std::shared_ptr<RawKV> ret(new RawKV(new RawKV::RawKVImpl(impl_->GetStub())));
  raw_kv = ret;
  return Status::OK();
}

RawKV::RawKV(RawKVImpl* impl) : impl_(impl) {}

RawKV::~RawKV() { impl_.reset(nullptr); }

Status RawKV::Get(const std::string& key, std::string& value) { return impl_->Get(key, value); }

Status RawKV::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  return impl_->BatchGet(keys, kvs);
}

Status RawKV::Put(const std::string& key, const std::string& value) { return impl_->Put(key, value); }

Status RawKV::BatchPut(const std::vector<KVPair>& kvs) { return impl_->BatchPut(kvs); }

Status RawKV::PutIfAbsent(const std::string& key, const std::string& value, bool& state) {
  return impl_->PutIfAbsent(key, value, state);
}

Status RawKV::BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& states) {
  return impl_->BatchPutIfAbsent(kvs, states);
}

Status RawKV::Delete(const std::string& key) { return impl_->Delete(key); }

Status RawKV::BatchDelete(const std::vector<std::string>& keys) { return impl_->BatchDelete(keys); }

Status RawKV::DeleteRangeNonContinuous(const std::string& start_key, const std::string& end_key,
                                       int64_t& delete_count) {
  return impl_->DeleteRange(start_key, end_key, false, delete_count);
}

Status RawKV::DeleteRange(const std::string& start_key, const std::string& end_key, int64_t& delete_count) {
  return impl_->DeleteRange(start_key, end_key, true, delete_count);
}

Status RawKV::CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                            bool& state) {
  return impl_->CompareAndSet(key, value, expected_value, state);
}

Status RawKV::BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                                 std::vector<KeyOpState>& states) {
  return impl_->BatchCompareAndSet(kvs, expected_values, states);
}

Status RawKV::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& kvs) {
  return impl_->Scan(start_key, end_key, limit, kvs);
}

}  // namespace sdk
}  // namespace dingodb