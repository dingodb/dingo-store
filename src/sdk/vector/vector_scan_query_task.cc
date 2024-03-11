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

#include "sdk/vector/vector_scan_query_task.h"

#include "common/synchronization.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/vector/index_service_rpc.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

Status VectorScanQueryTask::Init() {
  if (scan_query_param_.max_scan_count < 0) {
    return Status::InvalidArgument("max_scan_count must be greater than or equal to 0");
  }

  if (scan_query_param_.is_reverse) {
    if (!(scan_query_param_.vector_id_end < scan_query_param_.vector_id_start)) {
      return Status::InvalidArgument("vector_id_end must be less than vector_id_start in reverse scan");
    }
  } else {
    if (scan_query_param_.vector_id_end != 0 &&
        !(scan_query_param_.vector_id_start < scan_query_param_.vector_id_end)) {
      return Status::InvalidArgument("vector_id_end must be greater than vector_id_start in forward scan");
    }
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

void VectorScanQueryTask::DoAsync() {
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
    auto* sub_task = new VectorScanQueryPartTask(stub, vector_index_, part_id, scan_query_param_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorScanQueryTask::SubTaskCallback(Status status, VectorScanQueryPartTask* sub_task) {
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
    std::vector<VectorWithId> vectors = sub_task->GetResult();
    for (auto& result : vectors) {
      CHECK(vector_ids_.find(result.id) == vector_ids_.end()) << "scan query find duplicate vector id: " << result.id;
      result_vectors_.push_back(std::move(result));
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

void VectorScanQueryTask::ConstructResultUnlocked() {
  if (scan_query_param_.is_reverse) {
    std::sort(result_vectors_.begin(), result_vectors_.end(),
              [](const VectorWithId& a, const VectorWithId& b) { return a.id > b.id; });
  } else {
    std::sort(result_vectors_.begin(), result_vectors_.end(),
              [](const VectorWithId& a, const VectorWithId& b) { return a.id < b.id; });
  }

  if (result_vectors_.size() > scan_query_param_.max_scan_count) {
    result_vectors_.resize(scan_query_param_.max_scan_count);
  }

  out_result_.vectors = std::move(result_vectors_);
}

void VectorScanQueryPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    result_vectors_.clear();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorScanQueryRpc>();
    FillVectorScanQueryRpcRequest(rpc->MutableRequest(), region);

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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorScanQueryRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorScanQueryPartTask::FillVectorScanQueryRpcRequest(pb::index::VectorScanQueryRequest* request,
                                                            const std::shared_ptr<Region>& region) {
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());

  request->set_vector_id_start(scan_query_param_.vector_id_start);
  request->set_is_reverse_scan(scan_query_param_.is_reverse);
  request->set_max_scan_count(scan_query_param_.max_scan_count);
  request->set_vector_id_end(scan_query_param_.vector_id_end);
  request->set_without_vector_data(!scan_query_param_.with_vector_data);
  request->set_without_scalar_data(!scan_query_param_.with_scalar_data);
  if (scan_query_param_.with_scalar_data) {
    for (const auto& key : scan_query_param_.selected_keys) {
      request->add_selected_keys(key);
    }
  }

  request->set_without_table_data(!scan_query_param_.with_table_data);
  request->set_use_scalar_filter(scan_query_param_.use_scalar_filter);
  CHECK(!scan_query_param_.use_scalar_filter) << "not support scalar filter now";
}

void VectorScanQueryPartTask::VectorScanQueryRpcCallback(Status status, VectorScanQueryRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      for (const auto& vectorid_pb : rpc->Response()->vectors()) {
        result_vectors_.emplace_back(InternalVectorIdPB2VectorWithId(vectorid_pb));
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