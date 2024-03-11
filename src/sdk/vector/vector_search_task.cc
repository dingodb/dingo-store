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

#include "sdk/vector/vector_search_task.h"

#include <cstdint>
#include <iterator>
#include <memory>

#include "common/logging.h"
#include "common/synchronization.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/vector.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

Status VectorSearchTask::Init() {
  if (target_vectors_.empty()) {
    return Status::InvalidArgument("target_vectors is empty");
  }

  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  std::unique_lock<std::shared_mutex> w(rw_lock_);

  auto part_ids = vector_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  return Status::OK();
}

void VectorSearchTask::DoAsync() {
  std::set<int64_t> next_part_ids;
  Status tmp;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    next_part_ids = next_part_ids_;
    tmp = status_;
  }

  if (next_part_ids.empty()) {
    DoAsyncDone(tmp);
    return;
  }

  sub_tasks_count_.store(next_part_ids.size());

  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new VectorSearchPartTask(stub, index_id_, part_id, search_param_, target_vectors_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorSearchTask::SubTaskCallback(Status status, VectorSearchPartTask* sub_task) {
  DEFER(delete sub_task);

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    std::unordered_map<int64_t, std::vector<VectorWithDistance>>& sub_results = sub_task->GetSearchResult();
    // merge
    for (auto& result : sub_results) {
      auto iter = tmp_out_result_.find(result.first);
      if (iter != tmp_out_result_.cend()) {
        auto& origin = iter->second;
        auto& to_put = result.second;
        origin.reserve(origin.size() + to_put.size());
        std::move(to_put.begin(), to_put.end(), std::back_inserter(origin));
      } else {
        CHECK(tmp_out_result_.insert({result.first, std::move(result.second)}).second);
      }
    }

    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      ConstructResultUnlocked();
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

void VectorSearchTask::ConstructResultUnlocked() {
  for (const auto& vector_with_id : target_vectors_) {
    VectorWithId tmp;
    {
      //  NOTE: use copy
      const Vector& to_copy = vector_with_id.vector;
      tmp.vector.dimension = to_copy.dimension;
      tmp.vector.value_type = to_copy.value_type;
      tmp.vector.float_values = to_copy.float_values;
      tmp.vector.binary_values = to_copy.binary_values;
    }

    SearchResult search(std::move(tmp));

    out_result_.push_back(std::move(search));
  }

  for (auto& iter : tmp_out_result_) {
    auto& vec = iter.second;
    std::sort(vec.begin(), vec.end(),
              [](const VectorWithDistance& a, const VectorWithDistance& b) { return a.distance < b.distance; });
  }

  for (auto& iter : tmp_out_result_) {
    int64_t idx = iter.first;
    auto& vec_distance = iter.second;
    if (!search_param_.enable_range_search && search_param_.topk > 0 && search_param_.topk < vec_distance.size()) {
      vec_distance.resize(search_param_.topk);
    }

    out_result_[idx].vector_datas = std::move(vec_distance);
  }
}

Status VectorSearchPartTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  return Status::OK();
}

void VectorSearchPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    search_result_.clear();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorSearchRpc>();
    FillVectorSearchRpcRequest(rpc->MutableRequest(), region);

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), regions.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions.size());

  for (auto i = 0; i < regions.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorSearchRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorSearchPartTask::FillVectorSearchRpcRequest(pb::index::VectorSearchRequest* request,
                                                      const std::shared_ptr<Region>& region) {
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  FillInternalSearchParams(request->mutable_parameter(), vector_index_->GetVectorIndexType(), search_param_);
  for (const auto& vector_id : target_vectors_) {
    // NOTE* vector_id is useless
    FillVectorWithIdPB(request->add_vector_with_ids(), vector_id, false);
  }
}

void VectorSearchPartTask::VectorSearchRpcCallback(const Status& status, VectorSearchRpc* rpc) {
  // TODO : to remove
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
    if (rpc->Response()->batch_results_size() != rpc->Request()->vector_with_ids_size()) {
      DINGO_LOG(INFO) << Name() << " rpc: " << rpc->Method()
                      << " request vector_with_ids_size: " << rpc->Request()->vector_with_ids_size()
                      << " response batch_results_size: " << rpc->Response()->batch_results_size();
    }

    {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      for (auto i = 0; i < rpc->Response()->batch_results_size(); i++) {
        for (const auto& distancepb : rpc->Response()->batch_results(i).vector_with_distances()) {
          VectorWithDistance distance = InternalVectorWithDistance2VectorWithDistance(distancepb);
          search_result_[i].push_back(std::move(distance));
        }
      }
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