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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "sdk/client_internal_data.h"
#include "sdk/client_stub.h"
#include "sdk/common/param_config.h"
#include "sdk/rawkv/raw_kv_batch_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_batch_delete_task.h"
#include "sdk/rawkv/raw_kv_batch_get_task.h"
#include "sdk/rawkv/raw_kv_batch_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_batch_put_task.h"
#include "sdk/rawkv/raw_kv_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_delete_range_task.h"
#include "sdk/rawkv/raw_kv_delete_task.h"
#include "sdk/rawkv/raw_kv_get_task.h"
#include "sdk/rawkv/raw_kv_internal_data.h"
#include "sdk/rawkv/raw_kv_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_put_task.h"
#include "sdk/rawkv/raw_kv_scan_task.h"
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
  std::shared_ptr<RawKV> tmp(new RawKV(new RawKV::Data(*data_->stub)));
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

Status Client::DropRegion(int64_t region_id) {
  data_->stub->GetMetaCache()->RemoveRegion(region_id);
  return data_->stub->GetAdminTool()->DropRegion(region_id);
}

RawKV::RawKV(Data* data) : data_(data) {}

RawKV::~RawKV() { data_.reset(nullptr); }

Status RawKV::Get(const std::string& key, std::string& out_value) {
  RawKvGetTask task(data_->stub, key, out_value);
  return task.Run();
}

Status RawKV::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& out_kvs) {
  RawKvBatchGetTask task(data_->stub, keys, out_kvs);
  return task.Run();
}

Status RawKV::Put(const std::string& key, const std::string& value) {
  RawKvPutTask task(data_->stub, key, value);
  return task.Run();
}

Status RawKV::BatchPut(const std::vector<KVPair>& kvs) {
  RawKvBatchPutTask task(data_->stub, kvs);
  return task.Run();
}

Status RawKV::PutIfAbsent(const std::string& key, const std::string& value, bool& out_state) {
  RawKvPutIfAbsentTask task(data_->stub, key, value, out_state);
  return task.Run();
}

Status RawKV::BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& out_states) {
  RawKvBatchPutIfAbsentTask task(data_->stub, kvs, out_states);
  return task.Run();
}

Status RawKV::Delete(const std::string& key) {
  RawKvDeleteTask task(data_->stub, key);
  return task.Run();
}

Status RawKV::BatchDelete(const std::vector<std::string>& keys) {
  RawKvBatchDeleteTask task(data_->stub, keys);
  return task.Run();
}

Status RawKV::DeleteRangeNonContinuous(const std::string& start_key, const std::string& end_key,
                                       int64_t& out_delete_count) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvDeleteRangeTask task(data_->stub, start_key, end_key, false, out_delete_count);
  return task.Run();
}

Status RawKV::DeleteRange(const std::string& start_key, const std::string& end_key, int64_t& out_delete_count) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvDeleteRangeTask task(data_->stub, start_key, end_key, true, out_delete_count);
  return task.Run();
}

Status RawKV::CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                            bool& out_state) {
  RawKvCompareAndSetTask task(data_->stub, key, value, expected_value, out_state);
  return task.Run();
}

Status RawKV::BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                                 std::vector<KeyOpState>& out_states) {
  if (kvs.size() != expected_values.size()) {
    return Status::InvalidArgument(
        fmt::format("kvs size:{} must equal expected_values size:{}", kvs.size(), expected_values.size()));
  }

  RawKvBatchCompareAndSetTask task(data_->stub, kvs, expected_values, out_states);
  return task.Run();
}

Status RawKV::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& kvs) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvScanTask task(data_->stub, start_key, end_key, limit, kvs);
  return task.Run();
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

RegionCreator& RegionCreator::SetEngineType(EngineType engine_type) {
  data_->engine_type = engine_type;
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

static pb::common::RawEngine EngineType2RawEngine(EngineType engine_type) {
  switch (engine_type) {
    case kLSM:
      return pb::common::RawEngine::RAW_ENG_ROCKSDB;
    case kBTree:
      return pb::common::RawEngine::RAW_ENG_BDB;
    case kXDPROCKS:
      return pb::common::RawEngine::RAW_ENG_XDPROCKS;
    default:
      CHECK(false) << "unknow engine_type:" << engine_type;
  }
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

  req.set_raw_engine(EngineType2RawEngine(data_->engine_type));

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
        usleep(kCoordinatorInteractionDelayMs * 1000);
      } else {
        return Status::OK();
      }
    }

    std::string msg = fmt::format("Fail query region:{} state retry:{} exceed limit:{}, delay ms:{}", out_region_id,
                                  retry, kCoordinatorInteractionMaxRetry, kCoordinatorInteractionDelayMs);
    DINGO_LOG(INFO) << msg;
    return Status::Incomplete(msg);
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb