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

#include <unistd.h>

#include <charconv>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "sdk/client_internal_data.h"
#include "sdk/client_stub.h"
#include "sdk/common/param_config.h"
#include "sdk/rawkv/raw_kv_impl.h"
#include "sdk/region_creator_internal_data.h"
#include "sdk/status.h"
#include "sdk/transaction/txn_impl.h"

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

Status Client::Build(std::string naming_service_url, Client** client) {
  std::shared_ptr<Client> tmp;
  Status s = Build(naming_service_url, tmp);
  if (s.ok()) {
    *client = tmp.get();
    tmp.reset();
  }
  return s;
}

Client::Client() : data_(new Client::Data()) {}

Client::~Client() { data_.reset(nullptr); }

Status Client::Init(std::string naming_service_url) {
  CHECK(!naming_service_url.empty());
  if (data_->init) {
    return Status::IllegalState("forbidden multiple init");
  }

  auto tmp = std::make_unique<ClientStub>();
  Status open = tmp->Open(naming_service_url);
  if (open.IsOK()) {
    data_->init = true;
    data_->stub = std::move(tmp);
  }
  return open;
}

Status Client::NewRawKV(std::shared_ptr<RawKV>& raw_kv) {
  std::shared_ptr<RawKV> tmp(new RawKV(new RawKV::RawKVImpl(*data_->stub)));
  raw_kv = std::move(tmp);
  return Status::OK();
}

Status Client::NewRawKV(RawKV** raw_kv) {
  std::shared_ptr<RawKV> tmp;
  Status s = NewRawKV(tmp);
  if (s.ok()) {
    *raw_kv = tmp.get();
    tmp.reset();
  }
  return s;
}

Status Client::NewTransaction(const TransactionOptions& options, std::shared_ptr<Transaction>& txn) {
  std::shared_ptr<Transaction> tmp(new Transaction(new Transaction::TxnImpl(*data_->stub, options)));
  Status s = tmp->Begin();
  if (s.IsOK()) {
    txn = std::move(tmp);
  }
  return s;
}

Status Client::NewTransaction(const TransactionOptions& options, Transaction** txn) {
  std::shared_ptr<Transaction> tmp;
  Status s = NewTransaction(options, tmp);
  if (s.ok()) {
    *txn = tmp.get();
    tmp.reset();
  }

  return s;
}

Status Client::NewRegionCreator(std::shared_ptr<RegionCreator>& creator) {
  std::shared_ptr<RegionCreator> tmp(new RegionCreator(new RegionCreator::Data(*data_->stub)));
  creator = std::move(tmp);
  return Status::OK();
}

// NOTE:: Caller must delete *raw_kv when it is no longer needed.
Status Client::NewRegionCreator(RegionCreator** creator) {
  std::shared_ptr<RegionCreator> tmp;
  Status s = NewRegionCreator(tmp);
  if (s.ok()) {
    *creator = tmp.get();
    tmp.reset();
  }
  return s;
}

Status Client::IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress) {
  return data_->stub->GetAdminTool()->IsCreateRegionInProgress(region_id, out_create_in_progress);
}

Status Client::DropRegion(int64_t region_id) { return data_->stub->GetAdminTool()->DropRegion(region_id); }

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

Transaction::Transaction(TxnImpl* impl) : impl_(impl) {}

Transaction::~Transaction() { impl_.reset(nullptr); }

Status Transaction::Begin() { return impl_->Begin(); }

Status Transaction::Get(const std::string& key, std::string& value) { return impl_->Get(key, value); }

Status Transaction::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  return impl_->BatchGet(keys, kvs);
}

Status Transaction::Put(const std::string& key, const std::string& value) { return impl_->Put(key, value); }

Status Transaction::BatchPut(const std::vector<KVPair>& kvs) { return impl_->BatchPut(kvs); }

Status Transaction::PutIfAbsent(const std::string& key, const std::string& value) {
  return impl_->PutIfAbsent(key, value);
}

Status Transaction::BatchPutIfAbsent(const std::vector<KVPair>& kvs) { return impl_->BatchPutIfAbsent(kvs); }

Status Transaction::Delete(const std::string& key) { return impl_->Delete(key); }

Status Transaction::BatchDelete(const std::vector<std::string>& keys) { return impl_->BatchDelete(keys); }

Status Transaction::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                         std::vector<KVPair>& kvs) {
  return impl_->Scan(start_key, end_key, limit, kvs);
}

Status Transaction::PreCommit() { return impl_->PreCommit(); }

Status Transaction::Commit() { return impl_->Commit(); }

Status Transaction::Rollback() { return impl_->Rollback(); }

RegionCreator::RegionCreator(Data* data) : data_(data) {}

RegionCreator::~RegionCreator() = default;

RegionCreator& RegionCreator::SetRegionName(const std::string& name) {
  data_->region_name = name;
  return *this;
}

RegionCreator& RegionCreator::SetRange(const std::string& lower_bound, const std::string& upper_bound) {
  data_->lower_bound = lower_bound;
  data_->upper_bound = upper_bound;
  return *this;
}

RegionCreator& RegionCreator::SetReplicaNum(int64_t num) {
  data_->replica_num = num;
  return *this;
}

RegionCreator& RegionCreator::Wait(bool wait) {
  data_->wait = wait;
  return *this;
}

Status RegionCreator::Create(int64_t& out_region_id) {
  if (data_->region_name.empty()) {
    return Status::InvalidArgument("Missing region name");
  }
  if (data_->lower_bound.empty() || data_->upper_bound.empty()) {
    return Status::InvalidArgument("lower_bound or upper_bound must not empty");
  }
  if (data_->replica_num <= 0) {
    return Status::InvalidArgument("replica num must greater 0");
  }

  pb::coordinator::CreateRegionRequest req;
  req.set_region_name(data_->region_name);
  req.set_replica_num(data_->replica_num);
  req.mutable_range()->set_start_key(data_->lower_bound);
  req.mutable_range()->set_end_key(data_->upper_bound);

  pb::coordinator::CreateRegionResponse resp;
  DINGO_RETURN_NOT_OK(data_->stub.GetCoordinatorProxy()->CreateRegion(req, resp));
  CHECK(resp.region_id() > 0) << "create region internal error, req:" << req.DebugString()
                              << ", resp:" << resp.DebugString();
  out_region_id = resp.region_id();

  if (data_->wait) {
    int retry = 0;
    while (retry < kCoordinatorInteractionMaxRetry) {
      bool creating = false;
      DINGO_RETURN_NOT_OK(data_->stub.GetAdminTool()->IsCreateRegionInProgress(out_region_id, creating));

      if (creating) {
        retry++;
        sleep(3);
      } else {
        return Status::OK();
      }
    }

    std::string msg = fmt::format("Fail query region:{} state retry:{} exceed limit:{}", out_region_id, retry,
                                  kCoordinatorInteractionMaxRetry);
    DINGO_LOG(INFO) << msg;
    return Status::Incomplete(msg);
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb