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

#include "sdk/rawkv/raw_kv_delete_range_task.h"

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/store/store_rpc.h"

namespace dingodb {
namespace sdk {

RawKvDeleteRangeTask::RawKvDeleteRangeTask(const ClientStub& stub, const std::string& start_key,
                                           const std::string& end_key, bool continuous, int64_t& out_delete_count)
    : RawKvTask(stub),
      start_key_(start_key),
      end_key_(end_key),
      continuous_(continuous),
      out_delete_count_(out_delete_count),
      tmp_out_delete_count_(0) {}

Status RawKvDeleteRangeTask::Init() {
  auto meta_cache = stub.GetMetaCache();

  std::vector<std::shared_ptr<Region>> regions;
  Status ret = meta_cache->ScanRegionsBetweenRange(start_key_, end_key_, 0, regions);
  if (!ret.ok()) {
    if (ret.IsNotFound()) {
      DINGO_LOG(WARNING) << fmt::format("region not found between [{},{}), no need retry, status:{}", start_key_,
                                        end_key_, ret.ToString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("lookup region fail between [{},{}), need retry, status:{}", start_key_,
                                        end_key_, ret.ToString());
    }

    return ret;
  }

  CHECK(!regions.empty()) << "regions must not empty";

  if (continuous_) {
    for (int i = 0; i < regions.size() - 1; i++) {
      auto cur = regions[i];
      auto next = regions[i + 1];
      if (cur->Range().end_key() != next->Range().start_key()) {
        std::string msg = fmt::format("regions bewteen [{}, {}) not continuous", start_key_, end_key_);
        DINGO_LOG(WARNING) << msg
                           << fmt::format(", cur region:{} ({}-{}), next region:{} ({}-{})", cur->RegionId(),
                                          cur->Range().start_key(), cur->Range().end_key(), next->RegionId(),
                                          next->Range().start_key(), next->Range().end_key());

        Status ret = Status::Aborted(msg);
        return ret;
      }
    }
  }

  next_start_key_ = start_key_;
  return Status::OK();
}

void RawKvDeleteRangeTask::DoAsync() { DeleteNextRange(); }

void RawKvDeleteRangeTask::DeleteNextRange() {
  CHECK(!next_start_key_.empty()) << "next_start_key_ should not empty";
  CHECK(next_start_key_ <= end_key_) << fmt::format(
      "next_start_key_:{} should less than or equal to end_key_:", next_start_key_, end_key_);

  auto meta_cache = stub.GetMetaCache();

  std::shared_ptr<Region> region;
  status_ = meta_cache->LookupRegionBetweenRange(next_start_key_, end_key_, region);
  if (status_.IsNotFound()) {
    DINGO_LOG(INFO) << fmt::format("region not found  between [{},{}), start_key:{} status:{}", next_start_key_,
                                   end_key_, start_key_, status_.ToString());
    DoAsyncDone(Status::OK());
    return;
  }

  if (!status_.ok()) {
    DINGO_LOG(WARNING) << fmt::format("region look fail between [{},{}), start_key:{} status:{}", next_start_key_,
                                      end_key_, start_key_, status_.ToString());
    DoAsyncDone(status_);
    return;
  }

  CHECK_NOTNULL(region.get());
  const auto& range = region->Range();
  auto start = (range.start_key() <= next_start_key_ ? next_start_key_ : range.start_key());
  auto end = (range.end_key() <= end_key_) ? range.end_key() : end_key_;

  //  fill rpc
  auto rpc = std::make_unique<KvDeleteRangeRpc>();
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  auto* range_with_option = rpc->MutableRequest()->mutable_range();
  auto* to_fill_range = range_with_option->mutable_range();
  to_fill_range->set_start_key(start);
  to_fill_range->set_end_key(end);
  range_with_option->set_with_start(true);
  range_with_option->set_with_end(false);

  auto controller = std::make_unique<StoreRpcController>(stub, *rpc.get(), region);

  controller->AsyncCall([this, r = rpc.release(), c = controller.release()](auto&& s) {
    KvDeleteRangeRpcCallback(std::forward<decltype(s)>(s), r, c);
  });
}

void RawKvDeleteRangeTask::KvDeleteRangeRpcCallback(Status status, KvDeleteRangeRpc* rpc,
                                                    StoreRpcController* controller) {
  status_ = status;

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString() << ", rpc req:" << rpc->Request()->DebugString()
                       << " rpc resp:" << rpc->Response()->DebugString();
  } else {
    const auto& end_key = rpc->Request()->range().range().end_key();
    CHECK(!end_key.empty()) << "illegal request:" << rpc->Request()->DebugString()
                            << ", resp:" << rpc->Response()->DebugString();

    tmp_out_delete_count_.fetch_add(rpc->Response()->delete_count());
    next_start_key_ = end_key;
  }

  delete controller;
  delete rpc;

  if (next_start_key_ >= end_key_) {
    DoAsyncDone(Status::OK());
    return;
  } else {
    stub.GetActuator()->Execute([this] { DeleteNextRange(); });
  }
}

void RawKvDeleteRangeTask::PostProcess() { out_delete_count_ = tmp_out_delete_count_.load(); }

}  // namespace sdk
}  // namespace dingodb
