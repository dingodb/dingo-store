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

#include "sdk/vector/vector_add_task.h"

#include <unordered_map>

#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/status.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_helper.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

Status VectorAddTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  next_vector_ids_.clear();

  for (const auto& vector : vectors_) {
    CHECK(next_vector_ids_.insert(vector.id).second) << "duplicate vector id: " << vector.id;
  }

  return Status::OK();
}

void VectorAddTask::DoAsync() {
  std::set<int64_t> next_batch;
  Status tmp;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    next_batch = next_vector_ids_;
    tmp = status_;
  }

  if (next_batch.empty()) {
    DoAsyncDone(tmp);
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_vectors_to_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& id : next_batch) {
    auto vector_iter = std::find_if(vectors_.begin(), vectors_.end(),
                                    [&](const VectorWithId& vector_with_id) { return vector_with_id.id == id; });
    CHECK(vector_iter != vectors_.end()) << "can't find vector id:" << id;

    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(vector_helper::VectorIdToRangeKey(*vector_index_, vector_iter->id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_vectors_to_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_vectors_to_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<VectorAddRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    rpc->MutableRequest()->set_is_update(is_update_);
    rpc->MutableRequest()->set_replace_deleted(replace_deleted_);

    // TODO : refact
    for (const auto& id : entry.second) {
      auto vector_iter = std::find_if(vectors_.begin(), vectors_.end(),
                                      [&](const VectorWithId& vector_with_id) { return vector_with_id.id == id; });
      CHECK(vector_iter != vectors_.end()) << "can't find vector id:" << id;

      FillVectorWithIdPB(rpc->MutableRequest()->add_vectors(), *vector_iter);
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_vectors_to_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_vectors_to_ids.size());

  for (auto i = 0; i < region_vectors_to_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorAddRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorAddTask::VectorAddRpcCallback(const Status& status, VectorAddRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    for (const auto& vector : rpc->Request()->vectors()) {
      next_vector_ids_.erase(vector.id());
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb