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

#include "server/coordinator_service.h"

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

void CoordinatorServiceImpl::Hello(google::protobuf::RpcController * /*controller*/,
                                   const pb::coordinator::HelloRequest *request,
                                   pb::coordinator::HelloResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  response->set_state(static_cast<pb::common::CoordinatorState>(0));
  response->set_status_detail("OK");

  if (request->get_memory_info()) {
    auto *memory_info = response->mutable_memory_info();
    this->coordinator_control_->GetMemoryInfo(*memory_info);
  }
}

void CoordinatorServiceImpl::CreateExecutor(google::protobuf::RpcController *controller,
                                            const pb::coordinator::CreateExecutorRequest *request,
                                            pb::coordinator::CreateExecutorResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor Request: IsLeader:" << is_leader << ", Request: " << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor
  uint64_t executor_id = 0;
  std::string keyring;
  auto local_ctl = this->coordinator_control_;
  int const ret = local_ctl->CreateExecutor(request->cluster_id(), executor_id, keyring, meta_increment);
  if (ret == 0) {
    response->set_executor_id(executor_id);
    response->set_keyring(keyring);
  } else {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Need legal cluster_id");

    auto *error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CreateExecutorRequest, pb::coordinator::CreateExecutorResponse>
      *meta_put_closure =
          new CoordinatorClosure<pb::coordinator::CreateExecutorRequest, pb::coordinator::CreateExecutorResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::DeleteExecutor(google::protobuf::RpcController *controller,
                                            const pb::coordinator::DeleteExecutorRequest *request,
                                            pb::coordinator::DeleteExecutorResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Executor Request: IsLeader:" << is_leader << ", Request: " << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete executor
  uint64_t const executor_id = request->executor_id();
  std::string const keyring = request->keyring();
  auto local_ctl = this->coordinator_control_;
  int const ret = local_ctl->DeleteExecutor(request->cluster_id(), executor_id, keyring, meta_increment);
  if (ret != 0) {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::ESTORE_NOTEXIST_RAFTENGINE, "DeleteExecutor failed");

    auto *error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

    DINGO_LOG(ERROR) << "Deleteexecutor failed:  executor_id=" << executor_id << ", keyring=" << keyring;
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::DeleteExecutorRequest, pb::coordinator::DeleteExecutorResponse>
      *meta_delete_store_closure =
          new CoordinatorClosure<pb::coordinator::DeleteExecutorRequest, pb::coordinator::DeleteExecutorResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_delete_store_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::CreateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::CreateStoreRequest *request,
                                         pb::coordinator::CreateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Store Request: IsLeader:" << is_leader << ", Request: " << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create store
  uint64_t store_id = 0;
  std::string keyring;
  auto local_ctl = this->coordinator_control_;
  int const ret = local_ctl->CreateStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (ret == 0) {
    response->set_store_id(store_id);
    response->set_keyring(keyring);
  } else {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Need legal cluster_id");
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CreateStoreRequest, pb::coordinator::CreateStoreResponse> *meta_put_closure =
      new CoordinatorClosure<pb::coordinator::CreateStoreRequest, pb::coordinator::CreateStoreResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::DeleteStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::DeleteStoreRequest *request,
                                         pb::coordinator::DeleteStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Store Request: IsLeader:" << is_leader << ", Request: " << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete store
  uint64_t const store_id = request->store_id();
  std::string const keyring = request->keyring();
  auto local_ctl = this->coordinator_control_;
  int const ret = local_ctl->DeleteStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (ret != 0) {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::ESTORE_NOTEXIST_RAFTENGINE, "DeleteStore failed");

    auto *error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

    DINGO_LOG(ERROR) << "DeleteStore failed:  store_id=" << store_id << ", keyring=" << keyring;
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::DeleteStoreRequest, pb::coordinator::DeleteStoreResponse>
      *meta_delete_store_closure =
          new CoordinatorClosure<pb::coordinator::DeleteStoreRequest, pb::coordinator::DeleteStoreResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_delete_store_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::ExecutorHeartbeat(google::protobuf::RpcController *controller,
                                               const pb::coordinator::ExecutorHeartbeatRequest *request,
                                               pb::coordinator::ExecutorHeartbeatResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Executor Heartbeat Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // validate executor
  if (!request->has_executor()) {
    auto *error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "ExecutorHeartBeat has_executor() is false, reject heartbeat";
    return;
  }

  int const ret = this->coordinator_control_->ValidateExecutor(request->executor().id(), request->executor().keyring());
  if (ret) {
    DINGO_LOG(ERROR) << "ExecutorHeartBeat ValidateExecutor failed, reject heardbeat, executor_id="
                     << request->executor().id() << " keyring=" << request->executor().keyring();
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update executor map
  int const new_executormap_epoch = this->coordinator_control_->UpdateExecutorMap(request->executor(), meta_increment);

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::ExecutorHeartbeatRequest, pb::coordinator::ExecutorHeartbeatResponse>
      *meta_create_executor_closure =
          new CoordinatorClosure<pb::coordinator::ExecutorHeartbeatRequest, pb::coordinator::ExecutorHeartbeatResponse>(
              request, response, done_guard.release(), new_executormap_epoch, this->coordinator_control_);

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_create_executor_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  // // setup response
  // DINGO_LOG(INFO) << "set epoch id to response " << new_executormap_epoch << " " << new_regionmap_epoch;
  // response->set_executormap_epoch(new_executormap_epoch);
  // response->set_regionmap_epoch(new_regionmap_epoch);

  // auto *new_regionmap = response->mutable_regionmap();
  // this->coordinator_control_->GetRegionMap(*new_regionmap);

  // auto *new_executormap = response->mutable_executormap();
  // this->coordinator_control_->GetExecutorMap(*new_executormap);
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController *controller,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Store Heartbeat Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // validate store
  if (!request->has_store()) {
    auto *error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "StoreHeartBeat has_store() is false, reject heartbeat";
    return;
  }

  int const ret = this->coordinator_control_->ValidateStore(request->store().id(), request->store().keyring());
  if (ret) {
    DINGO_LOG(ERROR) << "StoreHeartBeat ValidateStore failed, reject heardbeat, store_id=" << request->store().id()
                     << " keyring=" << request->store().keyring();
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update store map
  int const new_storemap_epoch = this->coordinator_control_->UpdateStoreMap(request->store(), meta_increment);

  // update region map
  std::vector<pb::common::Region> regions;
  for (const auto &x : request->regions()) {
    regions.push_back(x);
  }

  // call UpdateRegionMap
  uint64_t const new_regionmap_epoch = this->coordinator_control_->UpdateRegionMap(regions, meta_increment);

  // update store metrics
  if (request->has_store_metrics()) {
    this->coordinator_control_->UpdateStoreMetrics(request->store_metrics(), meta_increment);
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::StoreHeartbeatRequest, pb::coordinator::StoreHeartbeatResponse>
      *meta_put_closure =
          new CoordinatorClosure<pb::coordinator::StoreHeartbeatRequest, pb::coordinator::StoreHeartbeatResponse>(
              request, response, done_guard.release(), new_regionmap_epoch, new_storemap_epoch,
              this->coordinator_control_);

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  // // setup response
  // DINGO_LOG(INFO) << "set epoch id to response " << new_storemap_epoch << " " << new_regionmap_epoch;
  // response->set_storemap_epoch(new_storemap_epoch);
  // response->set_regionmap_epoch(new_regionmap_epoch);

  // auto *new_regionmap = response->mutable_regionmap();
  // this->coordinator_control_->GetRegionMap(*new_regionmap);

  // auto *new_storemap = response->mutable_storemap();
  // this->coordinator_control_->GetStoreMap(*new_storemap);
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);

  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMap Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  pb::common::StoreMap storemap;
  this->coordinator_control_->GetStoreMap(storemap);
  response->mutable_storemap()->CopyFrom(storemap);
  response->set_epoch(storemap.epoch());
}

void CoordinatorServiceImpl::GetStoreMetrics(google::protobuf::RpcController * /*controller*/,
                                             const pb::coordinator::GetStoreMetricsRequest *request,
                                             pb::coordinator::GetStoreMetricsResponse *response,
                                             google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMetrics Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // get store metrics
  pb::common::StoreMetrics store_metrics;
  std::vector<pb::common::StoreMetrics> store_metrics_list;
  this->coordinator_control_->GetStoreMetrics(store_metrics_list);

  for (auto &store_metrics : store_metrics_list) {
    auto *new_store_metrics = response->add_store_metrics();
    new_store_metrics->CopyFrom(store_metrics);
  }
}

void CoordinatorServiceImpl::GetExecutorMap(google::protobuf::RpcController * /*controller*/,
                                            const pb::coordinator::GetExecutorMapRequest *request,
                                            pb::coordinator::GetExecutorMapResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);

  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get ExecutorMap Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  pb::common::ExecutorMap executormap;
  this->coordinator_control_->GetExecutorMap(executormap);
  response->mutable_executormap()->CopyFrom(executormap);
  response->set_epoch(executormap.epoch());
}

void CoordinatorServiceImpl::GetRegionMap(google::protobuf::RpcController * /*controller*/,
                                          const pb::coordinator::GetRegionMapRequest *request,
                                          pb::coordinator::GetRegionMapResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);

  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  pb::common::RegionMap regionmap;
  this->coordinator_control_->GetRegionMap(regionmap);

  response->mutable_regionmap()->CopyFrom(regionmap);
  response->set_epoch(regionmap.epoch());
}

void CoordinatorServiceImpl::GetCoordinatorMap(google::protobuf::RpcController * /*controller*/,
                                               const pb::coordinator::GetCoordinatorMapRequest *request,
                                               pb::coordinator::GetCoordinatorMapResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  DINGO_LOG(DEBUG) << "Receive Get CoordinatorMap Request:" << format_request;

  uint64_t epoch;
  pb::common::Location leader_location;
  std::vector<pb::common::Location> locations;
  this->coordinator_control_->GetCoordinatorMap(request->cluster_id(), epoch, leader_location, locations);

  response->set_epoch(epoch);

  auto *leader_location_resp = response->mutable_leader_location();
  leader_location_resp->CopyFrom(leader_location);

  for (const auto &member_location : locations) {
    auto *location = response->add_coordinator_locations();
    location->CopyFrom(member_location);
  }
}

}  // namespace dingodb
