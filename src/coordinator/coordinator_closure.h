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

#ifndef DINGODB_COORDINATOR_CLOSURE_H__
#define DINGODB_COORDINATOR_CLOSURE_H__

#include <cstdint>
#include <memory>

#include "braft/util.h"
#include "brpc/closure_guard.h"
#include "coordinator/coordinator_control.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

template <typename REQ, typename RESP>
class CoordinatorClosure : public braft::Closure {
 public:
  CoordinatorClosure(const REQ* request, RESP* response, google::protobuf::Closure* done)
      : request_(request), response_(response), done_(done) {}
  ~CoordinatorClosure() override = default;

  const REQ* request() const { return request_; }  // NOLINT
  RESP* response() const { return response_; }     // NOLINT
  void Run() override { brpc::ClosureGuard done_guard(done_); }

 private:
  const REQ* request_;
  RESP* response_;
  google::protobuf::Closure* done_;
};

template <>
class CoordinatorClosure<pb::coordinator::StoreHeartbeatRequest, pb::coordinator::StoreHeartbeatResponse>
    : public braft::Closure {
 public:
  CoordinatorClosure(const pb::coordinator::StoreHeartbeatRequest* request,
                     pb::coordinator::StoreHeartbeatResponse* response, google::protobuf::Closure* done,
                     uint64_t new_regionmap_epoch, uint64_t new_storemap_epoch,
                     std::shared_ptr<CoordinatorControl> coordinator_control)
      : request_(request),
        response_(response),
        done_(done),
        coordinator_control_(coordinator_control),
        new_regionmap_epoch_(new_regionmap_epoch),
        new_storemap_epoch_(new_storemap_epoch) {}
  ~CoordinatorClosure() override = default;

  const pb::coordinator::StoreHeartbeatRequest* request() const { return request_; }  // NOLINT
  pb::coordinator::StoreHeartbeatResponse* response() const { return response_; }     // NOLINT

  void Run() override {
    auto* new_regionmap = response()->mutable_regionmap();
    coordinator_control_->GetRegionMap(*new_regionmap);

    auto* new_storemap = response()->mutable_storemap();
    coordinator_control_->GetStoreMap(*new_storemap);

    response()->set_storemap_epoch(new_storemap_epoch_);
    response()->set_regionmap_epoch(new_regionmap_epoch_);

    brpc::ClosureGuard done_guard(done_);
  }

 private:
  const pb::coordinator::StoreHeartbeatRequest* request_;
  pb::coordinator::StoreHeartbeatResponse* response_;
  google::protobuf::Closure* done_;
  uint64_t new_regionmap_epoch_;
  uint64_t new_storemap_epoch_;
  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_COMMON_H_
