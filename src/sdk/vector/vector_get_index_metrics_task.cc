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

#include "sdk/vector/vector_get_index_metrics_task.h"

#include <cstdint>
#include <memory>

#include "common/synchronization.h"
#include "sdk/common/common.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

Status VectorGetIndexMetricsTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  tmp_result_.index_type = vector_index_->GetVectorIndexType();

  auto part_ids = vector_index_->GetPartitionIds();
  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  return Status::OK();
}

void VectorGetIndexMetricsTask::DoAsync() {
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
    auto* sub_task = new VectorGetIndexMetricsPartTask(stub, vector_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorGetIndexMetricsTask::SubTaskCallback(Status status, VectorGetIndexMetricsPartTask* sub_task) {
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
    IndexMetricsResult result = sub_task->GetResult();
    MergeIndexMetricsResult(result, tmp_result_);
    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
      if (tmp.ok()) {
        if (tmp_result_.min_vector_id == INT64_MAX) {
          tmp_result_.min_vector_id = 0;
        }
        out_result_ = tmp_result_;
      }
    }

    DoAsyncDone(tmp);
  }
}

void VectorGetIndexMetricsPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    region_id_to_metrics_.clear();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorGetRegionMetricsRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), regions.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions.size());

  for (auto i = 0; i < regions.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall([this, rpc = rpcs_[i].get()](auto&& s) {
      VectorGetRegionMetricsRpcCallback(std::forward<decltype(s)>(s), rpc);
    });
  }
}

void VectorGetIndexMetricsPartTask::VectorGetRegionMetricsRpcCallback(const Status& status,
                                                                      VectorGetRegionMetricsRpc* rpc) {
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
    VLOG(kSdkVlogLevel) << "request:" << rpc->Request()->DebugString() << "\n"
                        << "response :" << rpc->Response()->DebugString() << "\n"
                        << "has_metris:" << (rpc->Response()->has_metrics() ? "true" : "false");
    IndexMetricsResult result = InternalVectorIndexMetrics2IndexMetricsResult(rpc->Response()->metrics());
    result.index_type = vector_index_->GetVectorIndexType();
    CHECK(region_id_to_metrics_.emplace(rpc->Request()->context().region_id(), result).second);
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