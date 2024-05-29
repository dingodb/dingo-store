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

#include "sdk/vector/vector_get_border_task.h"

#include <cstdint>

#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/status.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status VectorGetBorderTask::Init() {
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

void VectorGetBorderTask::DoAsync() {
  std::set<int64_t> next_part_ids;
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (next_part_ids_.empty()) {
      DoAsyncDone(Status::OK());
      return;
    }
    next_part_ids = next_part_ids_;
    status_ = Status::OK();
  }

  sub_tasks_count_.store(next_part_ids.size());

  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new VectorGetBorderPartTask(stub, vector_index_, part_id, is_max_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorGetBorderTask::SubTaskCallback(Status status, VectorGetBorderPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    int64_t result_vecotr_id = sub_task->GetResult();
    target_vector_id_ =
        is_max_ ? std::max(target_vector_id_, result_vecotr_id) : std::min(target_vector_id_, result_vecotr_id);

    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
      if (tmp.ok()) {
        out_vector_id_ = target_vector_id_;
      }
    }

    DoAsyncDone(tmp);
  }
}

void VectorGetBorderPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    result_vector_id_ = is_max_ ? -1 : INT64_MAX;
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorGetBorderIdRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    rpc->MutableRequest()->set_get_min(!is_max_);

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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorGetBorderIdRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorGetBorderPartTask::VectorGetBorderIdRpcCallback(const Status& status, VectorGetBorderIdRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    int64_t vector_id = rpc->Response()->id();
    if (vector_id > 0) {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      result_vector_id_ = is_max_ ? std::max(result_vector_id_, vector_id) : std::min(result_vector_id_, vector_id);
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