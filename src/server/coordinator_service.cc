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
#include "butil/containers/flat_map.h"
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
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor Request: IsLeader:" << is_leader
                  << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor
  pb::common::Executor executor_to_create;
  executor_to_create.CopyFrom(request->executor());
  auto ret = coordinator_control_->CreateExecutor(request->cluster_id(), executor_to_create, meta_increment);
  if (ret == pb::error::Errno::OK) {
    response->mutable_executor()->CopyFrom(executor_to_create);
  } else {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
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
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Executor Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->executor().id().length() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete executor
  auto ret = coordinator_control_->DeleteExecutor(request->cluster_id(), request->executor(), meta_increment);
  if (ret != pb::error::Errno::OK) {
    response->mutable_error()->set_errcode(ret);
    DINGO_LOG(ERROR) << "DeleteExecutor failed:  executor_id=" << request->executor().id();
    return;
  }
  DINGO_LOG(INFO) << "DeleteExecutor success:  executor_id=" << request->executor().id();

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

void CoordinatorServiceImpl::CreateExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::CreateExecutorUserRequest *request,
                                                pb::coordinator::CreateExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor user
  pb::common::ExecutorUser executor_user;
  executor_user.CopyFrom(request->executor_user());
  auto ret = this->coordinator_control_->CreateExecutorUser(request->cluster_id(), executor_user, meta_increment);
  if (ret == pb::error::Errno::OK) {
    response->mutable_executor_user()->CopyFrom(executor_user);
  } else {
    auto *error = response->mutable_error();
    error->set_errcode(ret);
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "CreateExecutorUser: meta_increment is empty";
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CreateExecutorUserRequest,
                     pb::coordinator::CreateExecutorUserResponse> *meta_put_closure =
      new CoordinatorClosure<pb::coordinator::CreateExecutorUserRequest, pb::coordinator::CreateExecutorUserResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::UpdateExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::UpdateExecutorUserRequest *request,
                                                pb::coordinator::UpdateExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Update Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = this->coordinator_control_->UpdateExecutorUser(request->cluster_id(), request->executor_user(),
                                                            request->executor_user_update(), meta_increment);
  if (ret == pb::error::Errno::OK) {
    response->mutable_executor_user()->CopyFrom(request->executor_user_update());
    response->mutable_executor_user()->set_user(request->executor_user().user());
  } else {
    response->mutable_error()->set_errcode(ret);
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "UpdateExecutorUser: meta_increment is empty";
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::UpdateExecutorUserRequest,
                     pb::coordinator::UpdateExecutorUserResponse> *meta_put_closure =
      new CoordinatorClosure<pb::coordinator::UpdateExecutorUserRequest, pb::coordinator::UpdateExecutorUserResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::DeleteExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::DeleteExecutorUserRequest *request,
                                                pb::coordinator::DeleteExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Delete Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor user
  pb::common::ExecutorUser executor_user;
  executor_user.CopyFrom(request->executor_user());
  auto local_ctl = this->coordinator_control_;
  auto ret = local_ctl->DeleteExecutorUser(request->cluster_id(), executor_user, meta_increment);
  if (ret != pb::error::Errno::OK) {
    response->mutable_error()->set_errcode(ret);
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "DeleteExecutorUser: meta_increment is empty";
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::DeleteExecutorUserRequest,
                     pb::coordinator::DeleteExecutorUserResponse> *meta_put_closure =
      new CoordinatorClosure<pb::coordinator::DeleteExecutorUserRequest, pb::coordinator::DeleteExecutorUserResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::GetExecutorUserMap(google::protobuf::RpcController * /*controller*/,
                                                const pb::coordinator::GetExecutorUserMapRequest *request,
                                                pb::coordinator::GetExecutorUserMapResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Get Executor User Map Request: IsLeader:" << is_leader
                  << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::common::ExecutorUserMap executor_user_map;
  auto ret = this->coordinator_control_->GetExecutorUserMap(request->cluster_id(), executor_user_map);
  if (ret != pb::error::Errno::OK) {
    response->mutable_error()->set_errcode(ret);
    return;
  }

  response->mutable_executor_user_map()->CopyFrom(executor_user_map);
}

void CoordinatorServiceImpl::CreateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::CreateStoreRequest *request,
                                         pb::coordinator::CreateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Store Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create store
  uint64_t store_id = 0;
  std::string keyring;
  auto local_ctl = this->coordinator_control_;
  auto ret = local_ctl->CreateStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (ret == pb::error::Errno::OK) {
    response->set_store_id(store_id);
    response->set_keyring(keyring);
  } else {
    response->mutable_error()->set_errcode(ret);
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(ERROR) << "CreateStore meta_incremnt=0:  store_id=" << store_id << ", keyring=" << keyring;
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
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Store Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->store_id() == 0) {
    auto *error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete store
  uint64_t const store_id = request->store_id();
  std::string const keyring = request->keyring();
  auto local_ctl = this->coordinator_control_;
  auto ret = local_ctl->DeleteStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (ret != pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "DeleteStore failed:  store_id=" << store_id << ", keyring=" << keyring;
    response->mutable_error()->set_errcode(ret);
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(INFO) << "DeleteStore meta_incremnt=0:  store_id=" << store_id << ", keyring=" << keyring;
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
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Executor Heartbeat Request, IsLeader:" << is_leader
                   << ", Request:" << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (!request->has_executor()) {
    auto *error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "ExecutorHeartBeat has_executor() is false, reject heartbeat";
    return;
  }

  pb::common::Executor executor = request->executor();

  if (executor.id().length() <= 0) {
    DINGO_LOG(DEBUG) << "ExecutorHeartBeat generate executor_id, executor_id=" << executor.server_location().host()
                     << ":" << executor.server_location().port();
    executor.set_id(executor.server_location().host() + ":" + std::to_string(executor.server_location().port()));
  }

  auto ret = coordinator_control_->ValidateExecutorUser(executor.executor_user());
  if (!ret) {
    DINGO_LOG(ERROR) << "ExecutorHeartBeat ValidateExecutor failed, reject heardbeat, executor_id="
                     << request->executor().id() << " keyring=" << request->executor().executor_user().keyring();
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update executor map
  int const new_executormap_epoch = this->coordinator_control_->UpdateExecutorMap(executor, meta_increment);

  // if no need to update meta, just return
  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(DEBUG) << "ExecutorHeartbeat no need to update meta, store_id=" << request->executor().id();
    return;
  }

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
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController *controller,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Store Heartbeat Request, IsLeader:" << is_leader
                   << ", Request:" << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // validate store
  if (!request->has_store()) {
    auto *error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
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

  // if no need to update meta, just return
  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(DEBUG) << "StoreHeartbeat no need to update meta, store_id=" << request->store().id();
    return;
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
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMap Request, IsLeader:" << is_leader << ", Request:" << request->DebugString();

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
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->DebugString();

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

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get ExecutorMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->DebugString();

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

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader << ", Request:" << request->DebugString();

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
  DINGO_LOG(DEBUG) << "Receive Get CoordinatorMap Request:" << request->DebugString();

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

// Region services
void CoordinatorServiceImpl::QueryRegion(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::QueryRegionRequest *request,
                                         pb::coordinator::QueryRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Query Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  auto region_id = request->region_id();

  pb::common::Region region;
  auto ret = this->coordinator_control_->QueryRegion(region_id, region);
  response->mutable_error()->set_errcode(ret);

  if (ret == pb::error::Errno::OK) {
    response->mutable_region()->CopyFrom(region);
  } else {
    response->mutable_error()->set_errcode(ret);
  }
}

void CoordinatorServiceImpl::CreateRegion(google::protobuf::RpcController *controller,
                                          const pb::coordinator::CreateRegionRequest *request,
                                          pb::coordinator::CreateRegionResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Create Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  std::string region_name = request->region_name();
  std::string resource_tag = request->resource_tag();
  uint64_t replica_num = request->replica_num();
  pb::common::Range range = request->range();
  uint64_t schema_id = request->schema_id();
  uint64_t table_id = request->table_id();
  uint64_t new_region_id = 0;

  auto ret = coordinator_control_->CreateRegion(region_name, resource_tag, replica_num, range, schema_id, table_id,
                                                new_region_id, meta_increment);
  response->mutable_error()->set_errcode(ret);
  response->set_region_id(new_region_id);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CreateRegionRequest, pb::coordinator::CreateRegionResponse>
      *meta_create_region_closure =
          new CoordinatorClosure<pb::coordinator::CreateRegionRequest, pb::coordinator::CreateRegionResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_create_region_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::DropRegion(google::protobuf::RpcController *controller,
                                        const pb::coordinator::DropRegionRequest *request,
                                        pb::coordinator::DropRegionResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Drop Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto region_id = request->region_id();

  auto ret = this->coordinator_control_->DropRegion(region_id, meta_increment);
  response->mutable_error()->set_errcode(ret);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::DropRegionRequest, pb::coordinator::DropRegionResponse>
      *meta_drop_region_closure =
          new CoordinatorClosure<pb::coordinator::DropRegionRequest, pb::coordinator::DropRegionResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_drop_region_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::DropRegionPermanently(google::protobuf::RpcController *controller,
                                                   const pb::coordinator::DropRegionPermanentlyRequest *request,
                                                   pb::coordinator::DropRegionPermanentlyResponse *response,
                                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Drop Region Permanently Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto region_id = request->region_id();
  auto cluster_id = request->cluster_id();

  auto ret = this->coordinator_control_->DropRegionPermanently(region_id, meta_increment);
  response->mutable_error()->set_errcode(ret);

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::DropRegionPermanentlyRequest, pb::coordinator::DropRegionPermanentlyResponse>
      *meta_drop_region_permanently_closure = new CoordinatorClosure<pb::coordinator::DropRegionPermanentlyRequest,
                                                                     pb::coordinator::DropRegionPermanentlyResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_drop_region_permanently_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::SplitRegion(google::protobuf::RpcController *controller,
                                         const pb::coordinator::SplitRegionRequest *request,
                                         pb::coordinator::SplitRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Split Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_split_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto split_request = request->split_request();

  auto ret =
      this->coordinator_control_->SplitRegion(split_request.split_from_region_id(), split_request.split_to_region_id(),
                                              split_request.split_watershed_key(), meta_increment);
  response->mutable_error()->set_errcode(ret);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::SplitRegionRequest, pb::coordinator::SplitRegionResponse>
      *meta_split_region_closure =
          new CoordinatorClosure<pb::coordinator::SplitRegionRequest, pb::coordinator::SplitRegionResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_split_region_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::MergeRegion(google::protobuf::RpcController *controller,
                                         const pb::coordinator::MergeRegionRequest *request,
                                         pb::coordinator::MergeRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Merge Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_merge_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto merge_request = request->merge_request();

  auto ret = this->coordinator_control_->MergeRegion(merge_request.merge_from_region_id(),
                                                     merge_request.merge_to_region_id(), meta_increment);
  response->mutable_error()->set_errcode(ret);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::MergeRegionRequest, pb::coordinator::MergeRegionResponse>
      *meta_merge_region_closure =
          new CoordinatorClosure<pb::coordinator::MergeRegionRequest, pb::coordinator::MergeRegionResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_merge_region_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::ChangePeerRegion(google::protobuf::RpcController *controller,
                                              const pb::coordinator::ChangePeerRegionRequest *request,
                                              pb::coordinator::ChangePeerRegionResponse *response,
                                              google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Change Peer Region Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_change_peer_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto change_peer_request = request->change_peer_request();
  if (!change_peer_request.has_region_definition()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  const auto &region_definition = change_peer_request.region_definition();
  if (region_definition.peers_size() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<uint64_t> new_store_ids;
  for (const auto &it : region_definition.peers()) {
    new_store_ids.push_back(it.store_id());
  }

  auto ret = this->coordinator_control_->ChangePeerRegion(region_definition.id(), new_store_ids, meta_increment);
  response->mutable_error()->set_errcode(ret);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::ChangePeerRegionRequest, pb::coordinator::ChangePeerRegionResponse>
      *meta_change_peer_region_closure =
          new CoordinatorClosure<pb::coordinator::ChangePeerRegionRequest, pb::coordinator::ChangePeerRegionResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_change_peer_region_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

// StoreOperation service
void CoordinatorServiceImpl::GetStoreOperation(google::protobuf::RpcController * /*controller*/,
                                               const pb::coordinator::GetStoreOperationRequest *request,
                                               pb::coordinator::GetStoreOperationResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreOperation Request, IsLeader:" << is_leader
                   << ", Request:" << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // if store_id = 0, get all store operation
  if (request->store_id() == 0) {
    butil::FlatMap<uint64_t, pb::coordinator::StoreOperation> store_operations;
    store_operations.init(100);
    coordinator_control_->GetStoreOperations(store_operations);

    for (const auto &it : store_operations) {
      auto *new_store_operation = response->add_store_operations();
      new_store_operation->CopyFrom(it.second);
    }
    return;
  }

  // get store_operation for id
  auto *store_operation = response->add_store_operations();
  coordinator_control_->GetStoreOperation(request->store_id(), *store_operation);
}

void CoordinatorServiceImpl::CleanStoreOperation(google::protobuf::RpcController *controller,
                                                 const pb::coordinator::CleanStoreOperationRequest *request,
                                                 pb::coordinator::CleanStoreOperationResponse *response,
                                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive CleanStoreOperation Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto store_id = request->store_id();

  auto ret = this->coordinator_control_->CleanStoreOperation(store_id, meta_increment);
  response->mutable_error()->set_errcode(ret);

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CleanStoreOperationRequest,
                     pb::coordinator::CleanStoreOperationResponse> *meta_clean_store_operation_closure =
      new CoordinatorClosure<pb::coordinator::CleanStoreOperationRequest, pb::coordinator::CleanStoreOperationResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_clean_store_operation_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::AddStoreOperation(google::protobuf::RpcController *controller,
                                               const pb::coordinator::AddStoreOperationRequest *request,
                                               pb::coordinator::AddStoreOperationResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive AddStoreOperation Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto store_operation = request->store_operation();

  auto ret = this->coordinator_control_->AddStoreOperation(store_operation, meta_increment);
  response->mutable_error()->set_errcode(ret);

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::AddStoreOperationRequest, pb::coordinator::AddStoreOperationResponse>
      *meta_add_store_operation_closure =
          new CoordinatorClosure<pb::coordinator::AddStoreOperationRequest, pb::coordinator::AddStoreOperationResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_add_store_operation_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::RemoveStoreOperation(google::protobuf::RpcController *controller,
                                                  const pb::coordinator::RemoveStoreOperationRequest *request,
                                                  pb::coordinator::RemoveStoreOperationResponse *response,
                                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive RemoveStoreOperation Request:" << request->DebugString();

  auto is_leader = this->coordinator_control_->IsLeader();
  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto store_id = request->store_id();
  auto region_cmd_id = request->region_cmd_id();

  auto ret = this->coordinator_control_->RemoveStoreOperation(store_id, region_cmd_id, meta_increment);
  response->mutable_error()->set_errcode(ret);

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::RemoveStoreOperationRequest, pb::coordinator::RemoveStoreOperationResponse>
      *meta_remove_store_operation_closure = new CoordinatorClosure<pb::coordinator::RemoveStoreOperationRequest,
                                                                    pb::coordinator::RemoveStoreOperationResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_remove_store_operation_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

}  // namespace dingodb
