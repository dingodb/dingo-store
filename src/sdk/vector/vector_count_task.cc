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

#include "sdk/vector/vector_count_task.h"

#include <cstdint>

#include "common/logging.h"
#include "common/synchronization.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/vector/index_service_rpc.h"
#include "vector/codec.h"

namespace dingodb {
namespace sdk {

Status VectorCountTask::Init() {
  if (start_vector_id_ >= end_vector_id_) {
    return Status::InvalidArgument("start_vector_id_ must be less than end_vector_id_");
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

void VectorCountTask::DoAsync() {
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
    auto* sub_task = new VectorCountPartTask(stub, vector_index_, part_id, start_vector_id_, end_vector_id_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorCountTask::SubTaskCallback(Status status, VectorCountPartTask* sub_task) {
  DEFER(delete sub_task);

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    tmp_count_.fetch_add(sub_task->GetResult());
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }

    if (tmp.ok()) {
      out_count_ = tmp_count_.load();
    }

    DoAsyncDone(tmp);
  }
}

void VectorCountPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> partition_regions;
  Status s =
      stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), partition_regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  ret_count_.store(0);

  controllers_.clear();
  rpcs_.clear();

  std::vector<std::shared_ptr<Region>> regions;

  for (const auto& region : partition_regions) {
    int64_t region_start_vector_id;
    int64_t region_end_vector_id;
    VectorCodec::DecodeRangeToVectorId(region->Range(), region_start_vector_id, region_end_vector_id);

    auto start = std::max(region_start_vector_id, start_vector_id_);
    auto end = std::min(region_end_vector_id, end_vector_id_);

    if (start < end) {
      auto rpc = std::make_unique<VectorCountRpc>();
      FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());

      rpc->MutableRequest()->set_vector_id_start(start);
      rpc->MutableRequest()->set_vector_id_end(end);

      StoreRpcController controller(stub, *rpc, region);
      controllers_.push_back(controller);

      rpcs_.push_back(std::move(rpc));
      regions.push_back(region);
    } else {
      DINGO_LOG(INFO) << fmt::format(
          "region: {} decode vecotor_id: [{}, {}] has no overlap with request vector range: [{}, {}]",
          region->RegionId(), region_start_vector_id, region_end_vector_id, start_vector_id_, end_vector_id_);
    }
  }

  DCHECK_EQ(rpcs_.size(), regions.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  if (regions.empty()) {
    DINGO_LOG(WARNING) << fmt::format("index:{} part_id:{} has no overlap with request vector range: [{}, {}]",
                                      vector_index_->ToString(), part_id_, start_vector_id_, end_vector_id_);
    DoAsyncDone(Status::OK());
  } else {
    sub_tasks_count_.store(regions.size());

    for (auto i = 0; i < regions.size(); i++) {
      auto& controller = controllers_[i];

      controller.AsyncCall(
          [this, rpc = rpcs_[i].get()](auto&& s) { VectorCountRpcCallback(std::forward<decltype(s)>(s), rpc); });
    }
  }
}

void VectorCountPartTask::VectorCountRpcCallback(Status status, VectorCountRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    ret_count_.fetch_add(rpc->Response()->count());
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