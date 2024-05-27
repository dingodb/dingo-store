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

#include "sdk/auto_increment_manager.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/status.h"

namespace dingodb {

namespace sdk {

struct AutoInrementer::Req {
  explicit Req() = default;
  std::condition_variable cv;
};

Status AutoInrementer::GetNextId(int64_t& next) {
  std::vector<int64_t> ids;
  DINGO_RETURN_NOT_OK(GetNextIds(ids, 1));
  CHECK(!ids.empty());
  next = ids.front();
  return Status::OK();
}

Status AutoInrementer::GetNextIds(std::vector<int64_t>& to_fill, int64_t count) {
  CHECK_GT(count, 0);
  Req req;

  {
    std::unique_lock<std::mutex> lk(mutex_);
    queue_.push_back(&req);
    while (&req != queue_.front()) {
      req.cv.wait(lk);
    }
  }

  Status s;
  while (s.ok() && count > 0) {
    if (id_cache_.size() < count) {
      s = RefillCache();
    } else {
      to_fill.insert(to_fill.end(), id_cache_.begin(), id_cache_.begin() + count);
      id_cache_.erase(id_cache_.begin(), id_cache_.begin() + count);
      count = 0;
    }
  }

  {
    std::unique_lock<std::mutex> lk(mutex_);
    queue_.pop_front();
    if (!queue_.empty()) {
      queue_.front()->cv.notify_one();
    }
  }

  return s;
}

Status AutoInrementer::RefillCache() {
  GenerateAutoIncrementRpc rpc;
  PrepareRequest(*rpc.MutableRequest());

  VLOG(kSdkVlogLevel) << "GenerateAutoIncrement request:" << rpc.Request()->DebugString()
                      << " response:" << rpc.Response()->DebugString();

  DINGO_RETURN_NOT_OK(stub_.GetMetaRpcController()->SyncCall(rpc));
  // TODO: maybe not crash just return error msg
  const auto* response = rpc.Response();
  const auto* request = rpc.Request();
  CHECK_GT(response->end_id(), response->start_id())
      << " request:" << request->DebugString() << " response: " << response->DebugString();
  for (int64_t i = response->start_id(); i < response->end_id(); i++) {
    id_cache_.push_back(i);
  }
  return Status::OK();
}

void IndexAutoInrementer::PrepareRequest(pb::meta::GenerateAutoIncrementRequest& request) {
  *request.mutable_table_id() = vector_index_->GetIndexDefWithId().index_id();
  request.set_count(FLAGS_auto_incre_req_count);
  request.set_auto_increment_increment(1);
  request.set_auto_increment_offset(vector_index_->GetIncrementStartId());
}

std::shared_ptr<AutoInrementer> AutoIncrementerManager::GetOrCreateIndexIncrementer(
    std::shared_ptr<VectorIndex>& index) {
  std::unique_lock<std::mutex> lk(mutex_);
  int64_t index_id = index->GetId();
  auto iter = auto_incrementer_map_.find(index_id);
  if (iter != auto_incrementer_map_.end()) {
    return iter->second;
  } else {
    auto incrementer = std::make_shared<IndexAutoInrementer>(stub_, index);
    CHECK(auto_incrementer_map_.emplace(std::make_pair(index_id, incrementer)).second);
    return incrementer;
  }
}

void AutoIncrementerManager::RemoveIndexIncrementerById(int64_t index_id) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto iter = auto_incrementer_map_.find(index_id);
  if (iter != auto_incrementer_map_.end()) {
    auto_incrementer_map_.erase(iter);
  }
}

}  // namespace sdk
}  // namespace dingodb