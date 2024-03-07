
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

#include "sdk/vector/vector_batch_query_task.h"

#include "sdk/common/common.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_helper.h"

namespace dingodb {
namespace sdk {

Status VectorBatchQueryTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  for (long id : query_param_.vector_ids) {
    CHECK(vector_ids_.insert(id).second) << "duplicate vector id: " << id;
  }

  return Status::OK();
}

void VectorBatchQueryTask::DoAsync() {
  std::set<int64_t> next_batch;
  Status tmp;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    next_batch = vector_ids_;
    tmp = status_;
  }

  if (next_batch.empty()) {
    DoAsyncDone(tmp);
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_id_to_vector_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& id : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(vector_helper::VectorIdToRangeKey(*vector_index_, id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_id_to_vector_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_id_to_vector_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<VectorBatchQueryRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    // TODO: extract fuction
    rpc->MutableRequest()->set_without_vector_data(!query_param_.with_vector_data);
    rpc->MutableRequest()->set_without_scalar_data(!query_param_.with_scalar_data);
    rpc->MutableRequest()->set_without_table_data(!query_param_.with_table_data);

    if (query_param_.with_scalar_data) {
      for (const auto& select : query_param_.selected_keys) {
        rpc->MutableRequest()->add_selected_keys(select);
      }
    }

    for (const auto& id : entry.second) {
      rpc->MutableRequest()->add_vector_ids(id);
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_id_to_vector_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_id_to_vector_ids.size());

  for (auto i = 0; i < region_id_to_vector_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorBatchQueryRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorBatchQueryTask::VectorBatchQueryRpcCallback(const Status& status, VectorBatchQueryRpc* rpc) {
  VLOG(kSdkVlogLevel) << "rpc: " << rpc->Method() << " request: " << rpc->Request()->DebugString()
                      << " response: " << rpc->Response()->DebugString();

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    CHECK_EQ(rpc->Response()->vectors_size(), rpc->Request()->vector_ids_size())
        << Name() << ", rpc: " << rpc->Method() << " request vector_ids_size: " << rpc->Request()->vector_ids_size()
        << " response vectors_size: " << rpc->Response()->vectors_size()
        << " request: " << rpc->Request()->DebugString() << " response: " << rpc->Response()->DebugString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    for (const auto& vectorid_pb : rpc->Response()->vectors()) {
      out_result_.vectors.emplace_back(InternalVectorIdPB2VectorWithId(vectorid_pb));
      vector_ids_.erase(vectorid_pb.id());
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