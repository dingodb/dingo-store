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

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_int32(max_create_id_count, 2048, "max create id count");
DEFINE_bool(async_hello, true, "async hello");
DEFINE_int32(hello_latency_ms, 0, "hello latency seconds");

void DoCoordinatorHello(google::protobuf::RpcController * /*controller*/, const pb::coordinator::HelloRequest *request,
                        pb::coordinator::HelloResponse *response, TrackClosure *done,
                        std::shared_ptr<CoordinatorControl> coordinator_control,
                        std::shared_ptr<Engine> /*raft_engine*/, bool get_memory_info) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  *response->mutable_version_info() = GetVersionInfo();
  if (request->is_just_version_info()) {
    return;
  }

  if (!coordinator_control->IsLeader()) {
    coordinator_control->RedirectResponse(response);
  }

  response->set_state(static_cast<pb::common::CoordinatorState>(0));
  response->set_status_detail("OK");

  if (get_memory_info) {
    auto *memory_info = response->mutable_memory_info();
    coordinator_control->GetMemoryInfo(*memory_info);
  }

  // response cluster state
  auto is_read_only = Server::GetInstance().IsReadOnly();
  bool is_force_read_only = coordinator_control->GetForceReadOnly();

  if (is_read_only || is_force_read_only) {
    response->mutable_cluster_state()->set_cluster_is_read_only(is_read_only);
    response->mutable_cluster_state()->set_cluster_is_force_read_only(is_force_read_only);
    DINGO_LOG(INFO) << "Hello response: cluster_is_read_only=" << is_read_only
                    << ", cluster_is_force_read_only=" << is_force_read_only;
  }

  if (FLAGS_hello_latency_ms > 0) {
    bthread_usleep(FLAGS_hello_latency_ms * 1000);
  }
}

void CoordinatorServiceImpl::Hello(google::protobuf::RpcController *controller,
                                   const pb::coordinator::HelloRequest *request,
                                   pb::coordinator::HelloResponse *response, google::protobuf::Closure *done) {
  if (FLAGS_async_hello) {
    brpc::ClosureGuard done_guard(done);

    // Run in queue.
    auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
    auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
      DoCoordinatorHello(controller, request, response, svr_done, coordinator_control_, engine_,
                         request->get_memory_info());
    });
    bool ret = worker_set_->ExecuteRR(task);
    if (!ret) {
      brpc::ClosureGuard done_guard(svr_done);
      ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
    }
  } else {
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

    if (!this->IsCoordinatorControlLeader()) {
      coordinator_control_->RedirectResponse(response);
    }

    response->set_state(static_cast<pb::common::CoordinatorState>(0));
    response->set_status_detail("OK");

    if (request->get_memory_info()) {
      auto *memory_info = response->mutable_memory_info();
      this->coordinator_control_->GetMemoryInfo(*memory_info);
    }

    // response cluster state
    auto is_read_only = Server::GetInstance().IsReadOnly();
    bool is_force_read_only = coordinator_control_->GetForceReadOnly();

    if (is_read_only || is_force_read_only) {
      response->mutable_cluster_state()->set_cluster_is_read_only(is_read_only);
      response->mutable_cluster_state()->set_cluster_is_force_read_only(is_force_read_only);
      DINGO_LOG(INFO) << "Hello response: cluster_is_read_only=" << is_read_only
                      << ", cluster_is_force_read_only=" << is_force_read_only;
    }

    if (FLAGS_hello_latency_ms > 0) {
      bthread_usleep(FLAGS_hello_latency_ms * 1000);
    }
  }
}

void CoordinatorServiceImpl::GetMemoryInfo(google::protobuf::RpcController *controller,
                                           const pb::coordinator::HelloRequest *request,
                                           pb::coordinator::HelloResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCoordinatorHello(controller, request, response, svr_done, coordinator_control_, engine_, true);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void DoCreateExecutor(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::CreateExecutorRequest *request,
                      pb::coordinator::CreateExecutorResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor
  pb::common::Executor executor_to_create;
  executor_to_create = request->executor();
  auto ret = coordinator_control->CreateExecutor(request->cluster_id(), executor_to_create, meta_increment);
  if (ret.ok()) {
    *(response->mutable_executor()) = executor_to_create;
  } else {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateExecutor Write failed:  executor_id=" << request->executor().id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoDeleteExecutor(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::DeleteExecutorRequest *request,
                      pb::coordinator::DeleteExecutorResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Executor Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  if (request->executor().id().length() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete executor
  auto ret = coordinator_control->DeleteExecutor(request->cluster_id(), request->executor(), meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "DeleteExecutor failed:  executor_id=" << request->executor().id();
    return;
  }
  DINGO_LOG(INFO) << "DeleteExecutor success:  executor_id=" << request->executor().id();

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DeleteExecutor Write failed:  executor_id=" << request->executor().id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoCreateExecutorUser(google::protobuf::RpcController * /*controller*/,
                          const pb::coordinator::CreateExecutorUserRequest *request,
                          pb::coordinator::CreateExecutorUserResponse *response, TrackClosure *done,
                          std::shared_ptr<CoordinatorControl> coordinator_control,
                          std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor user
  pb::common::ExecutorUser executor_user;
  executor_user = request->executor_user();
  auto ret = coordinator_control->CreateExecutorUser(request->cluster_id(), executor_user, meta_increment);
  if (ret.ok()) {
    *(response->mutable_executor_user()) = executor_user;
  } else {
    auto *error = response->mutable_error();
    error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    error->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "CreateExecutorUser: meta_increment is empty";
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateExecutorUser Write failed:  executor_id=" << request->executor_user().user();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoUpdateExecutorUser(google::protobuf::RpcController * /*controller*/,
                          const pb::coordinator::UpdateExecutorUserRequest *request,
                          pb::coordinator::UpdateExecutorUserResponse *response, TrackClosure *done,
                          std::shared_ptr<CoordinatorControl> coordinator_control,
                          std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Update Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->UpdateExecutorUser(request->cluster_id(), request->executor_user(),
                                                     request->executor_user_update(), meta_increment);
  if (ret.ok()) {
    *(response->mutable_executor_user()) = request->executor_user_update();
    response->mutable_executor_user()->set_user(request->executor_user().user());
  } else {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "UpdateExecutorUser: meta_increment is empty";
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateExecutorUser Write failed:  executor_id=" << request->executor_user().user();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoDeleteExecutorUser(google::protobuf::RpcController * /*controller*/,
                          const pb::coordinator::DeleteExecutorUserRequest *request,
                          pb::coordinator::DeleteExecutorUserResponse *response, TrackClosure *done,
                          std::shared_ptr<CoordinatorControl> coordinator_control,
                          std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Delete Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create executor user
  pb::common::ExecutorUser executor_user;
  executor_user = request->executor_user();
  auto ret = coordinator_control->DeleteExecutorUser(request->cluster_id(), executor_user, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "DeleteExecutorUser: meta_increment is empty";
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DeleteExecutorUser Write failed:  executor_id=" << request->executor_user().user();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoGetExecutorUserMap(google::protobuf::RpcController * /*controller*/,
                          const pb::coordinator::GetExecutorUserMapRequest *request,
                          pb::coordinator::GetExecutorUserMapResponse *response, TrackClosure *done,
                          std::shared_ptr<CoordinatorControl> coordinator_control,
                          std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Get Executor User Map Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::common::ExecutorUserMap executor_user_map;
  auto ret = coordinator_control->GetExecutorUserMap(request->cluster_id(), executor_user_map);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());

    return;
  }

  *(response->mutable_executor_user_map()) = executor_user_map;
}

void DoCreateStore(google::protobuf::RpcController * /*controller*/, const pb::coordinator::CreateStoreRequest *request,
                   pb::coordinator::CreateStoreResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Store Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create store
  int64_t store_id = 0;
  std::string keyring;
  auto ret = coordinator_control->CreateStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (ret.ok()) {
    response->set_store_id(store_id);
    response->set_keyring(keyring);
  } else {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(ERROR) << "CreateStore meta_incremnt=0:  store_id=" << store_id << ", keyring=" << keyring;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateStore Write failed:  store_id=" << store_id << ", keyring=" << keyring;
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoDeleteStore(google::protobuf::RpcController * /*controller*/, const pb::coordinator::DeleteStoreRequest *request,
                   pb::coordinator::DeleteStoreResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Store Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  if (request->store_id() == 0) {
    auto *error = response->mutable_error();
    error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete store
  int64_t const store_id = request->store_id();
  std::string const &keyring = request->keyring();
  auto ret = coordinator_control->DeleteStore(request->cluster_id(), store_id, keyring, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DeleteStore failed:  store_id=" << store_id << ", keyring=" << keyring;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(INFO) << "DeleteStore meta_incremnt=0:  store_id=" << store_id << ", keyring=" << keyring;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DeleteStore Write failed:  store_id=" << store_id << ", keyring=" << keyring;
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoUpdateStore(google::protobuf::RpcController * /*controller*/, const pb::coordinator::UpdateStoreRequest *request,
                   pb::coordinator::UpdateStoreResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(INFO) << "Receive Update Store Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update store
  auto ret = coordinator_control->UpdateStore(request->cluster_id(), request->store_id(), request->keyring(),
                                              request->store_in_state(), meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(ERROR) << "UpdateStore meta_incremnt=0:  store_id=" << request->store_id()
                     << ", keyring=" << request->keyring();
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateStore Write failed:  store_id=" << request->store_id()
                     << ", keyring=" << request->keyring();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoExecutorHeartbeat(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::ExecutorHeartbeatRequest *request,
                         pb::coordinator::ExecutorHeartbeatResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Executor Heartbeat Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  if (!request->has_executor()) {
    auto *error = response->mutable_error();
    error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "ExecutorHeartBeat has_executor() is false, reject heartbeat";
    return;
  }

  pb::common::Executor executor = request->executor();

  if (executor.id().length() <= 0) {
    DINGO_LOG(DEBUG) << "ExecutorHeartBeat generate executor_id, executor_id=" << executor.server_location().host()
                     << ":" << executor.server_location().port();
    executor.set_id(executor.server_location().host() + ":" + std::to_string(executor.server_location().port()));
  }

  auto ret = coordinator_control->ValidateExecutorUser(executor.executor_user());
  if (!ret) {
    DINGO_LOG(ERROR) << "ExecutorHeartBeat ValidateExecutor failed, reject heardbeat, executor_id="
                     << request->executor().id() << " keyring=" << request->executor().executor_user().keyring();
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update executor map
  int const new_executormap_epoch = coordinator_control->UpdateExecutorMap(executor, meta_increment);

  // if no need to update meta, just return
  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(DEBUG) << "ExecutorHeartbeat no need to update meta, store_id=" << request->executor().id();
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ExecutorHeartbeat Write failed:  executor_id=" << request->executor().id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  auto *new_executormap = response->mutable_executormap();
  coordinator_control->GetExecutorMap(*new_executormap);
  response->set_executormap_epoch(new_executormap_epoch);
}

void DoStoreHeartbeat(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::StoreHeartbeatRequest *request,
                      pb::coordinator::StoreHeartbeatResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive Store Heartbeat Request, IsLeader:" << is_leader
                       << ", Request:" << request->ShortDebugString();
    return coordinator_control->RedirectResponse(response);
  }

  // validate store
  if (!request->has_store()) {
    DINGO_LOG(ERROR) << "StoreHeartBeat has_store() is false, reject heartbeat";
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EILLEGAL_PARAMTETERS,
                            "StoreHeartBeat has_store() is false, reject heartbeat");
    return;
  }

  int const ret = coordinator_control->ValidateStore(request->store().id(), request->store().keyring());
  if (ret) {
    DINGO_LOG(ERROR) << "StoreHeartBeat ValidateStore failed, reject heardbeat, store_id=" << request->store().id()
                     << " keyring=" << request->store().keyring();
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EILLEGAL_PARAMTETERS,
                            "StoreHeartBeat ValidateStore failed, reject heardbeat");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update store map
  int const new_storemap_epoch = coordinator_control->UpdateStoreMap(request->store(), meta_increment);

  // update store metrics
  if (request->has_store_metrics()) {
    coordinator_control->UpdateStoreMetrics(request->store_metrics(), meta_increment);

    // update is_read_only
    auto is_read_only_from_store = request->store_metrics().store_own_metrics().is_ready_only();
    if (is_read_only_from_store) {
      Server::GetInstance().SetReadOnly(true);
    }
  }

  // response cluster state
  auto is_read_only = Server::GetInstance().IsReadOnly();
  bool is_force_read_only = coordinator_control->GetForceReadOnly();

  if (is_read_only || is_force_read_only) {
    response->mutable_cluster_state()->set_cluster_is_read_only(is_read_only);
    response->mutable_cluster_state()->set_cluster_is_force_read_only(is_force_read_only);
    DINGO_LOG(INFO) << "StoreHeartbeat response: cluster_is_read_only=" << is_read_only
                    << ", cluster_is_force_read_only=" << is_force_read_only;
  }

  // if no need to update meta, just skip raft submit
  if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(DEBUG) << "StoreHeartbeat no need to update meta, store_id=" << request->store().id();

    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(Constant::kMetaRegionId);
    ctx->SetTracker(tracker);

    // this is a async operation will be block by closure
    auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
    if (!ret2.ok()) {
      DINGO_LOG(ERROR) << "StoreHeartbeat Write failed:  store_id=" << request->store().id();
      ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
      return;
    }
  }

  auto *new_storemap = response->mutable_storemap();
  coordinator_control->GetStoreMap(*new_storemap);

  response->set_storemap_epoch(new_storemap_epoch);
}

void DoGetStoreMap(google::protobuf::RpcController * /*controller*/, const pb::coordinator::GetStoreMapRequest *request,
                   pb::coordinator::GetStoreMapResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  pb::common::StoreMap storemap;
  coordinator_control->GetStoreMap(storemap);
  *(response->mutable_storemap()) = storemap;
  response->set_epoch(storemap.epoch());
}

void DoGetStoreMetrics(google::protobuf::RpcController * /*controller*/,
                       const pb::coordinator::GetStoreMetricsRequest *request,
                       pb::coordinator::GetStoreMetricsResponse *response, TrackClosure *done,
                       std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  // get store metrics
  pb::common::StoreMetrics store_metrics;
  std::vector<pb::common::StoreMetrics> store_metrics_list;
  coordinator_control->GetStoreRegionMetrics(request->store_id(), request->region_id(), store_metrics_list);

  for (auto &store_metrics : store_metrics_list) {
    auto *new_store_metrics = response->add_store_metrics();
    *new_store_metrics = store_metrics;
  }
}

void DoDeleteStoreMetrics(google::protobuf::RpcController * /*controller*/,
                          const pb::coordinator::DeleteStoreMetricsRequest *request,
                          pb::coordinator::DeleteStoreMetricsResponse *response, TrackClosure *done,
                          std::shared_ptr<CoordinatorControl> coordinator_control,
                          std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Delete StoreMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  // get store metrics
  coordinator_control->DeleteStoreRegionMetrics(request->store_id());
}

void DoGetRegionMetrics(google::protobuf::RpcController * /*controller*/,
                        const pb::coordinator::GetRegionMetricsRequest *request,
                        pb::coordinator::GetRegionMetricsResponse *response, TrackClosure *done,
                        std::shared_ptr<CoordinatorControl> coordinator_control,
                        std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  // get region metrics
  std::vector<pb::common::RegionMetrics> region_metrics_list;
  coordinator_control->GetRegionMetrics(request->region_id(), region_metrics_list);

  for (auto &region_metrics : region_metrics_list) {
    auto *new_region_metrics = response->add_region_metrics();
    *new_region_metrics = region_metrics;
  }
}

void DoDeleteRegionMetrics(google::protobuf::RpcController * /*controller*/,
                           const pb::coordinator::DeleteRegionMetricsRequest *request,
                           pb::coordinator::DeleteRegionMetricsResponse *response, TrackClosure *done,
                           std::shared_ptr<CoordinatorControl> coordinator_control,
                           std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Delete RegionMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  // delete region metrics
  coordinator_control->DeleteRegionMetrics(request->region_id());
}

void DoGetExecutorMap(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::GetExecutorMapRequest *request,
                      pb::coordinator::GetExecutorMapResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get ExecutorMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  pb::common::ExecutorMap executormap;
  coordinator_control->GetExecutorMap(executormap);
  *(response->mutable_executormap()) = executormap;
  response->set_epoch(executormap.epoch());
}

void DoGetRegionMap(google::protobuf::RpcController * /*controller*/,
                    const pb::coordinator::GetRegionMapRequest *request,
                    pb::coordinator::GetRegionMapResponse *response, TrackClosure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  pb::common::RegionMap regionmap;
  coordinator_control->GetRegionMap(regionmap);

  *(response->mutable_regionmap()) = regionmap;
  response->set_epoch(regionmap.epoch());
}

void DoGetDeletedRegionMap(google::protobuf::RpcController * /*controller*/,
                           const pb::coordinator::GetDeletedRegionMapRequest *request,
                           pb::coordinator::GetDeletedRegionMapResponse *response, TrackClosure *done,
                           std::shared_ptr<CoordinatorControl> coordinator_control,
                           std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  pb::common::RegionMap regionmap;
  coordinator_control->GetDeletedRegionMap(regionmap);

  *(response->mutable_regionmap()) = regionmap;
  response->set_epoch(regionmap.epoch());
}

void DoAddDeletedRegionMap(google::protobuf::RpcController * /*controller*/,
                           const pb::coordinator::AddDeletedRegionMapRequest *request,
                           pb::coordinator::AddDeletedRegionMapResponse *response, TrackClosure *done,
                           std::shared_ptr<CoordinatorControl> coordinator_control,
                           std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  auto ret = coordinator_control->AddDeletedRegionMap(request->region_id(), request->force());
}

void DoCleanDeletedRegionMap(google::protobuf::RpcController * /*controller*/,
                             const pb::coordinator::CleanDeletedRegionMapRequest *request,
                             pb::coordinator::CleanDeletedRegionMapResponse *response, TrackClosure *done,
                             std::shared_ptr<CoordinatorControl> coordinator_control,
                             std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  auto ret = coordinator_control->CleanDeletedRegionMap(request->region_id());
}

void DoGetRegionCount(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::GetRegionCountRequest *request,
                      pb::coordinator::GetRegionCountResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    coordinator_control->RedirectResponse(response);
    return;
  }

  int64_t region_count;
  coordinator_control->GetRegionCount(region_count);

  response->set_region_count(region_count);
}

void DoGetCoordinatorMap(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::GetCoordinatorMapRequest *request,
                         pb::coordinator::GetCoordinatorMapResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<KvControl> kv_control,
                         std::shared_ptr<TsoControl> tso_control,
                         std::shared_ptr<AutoIncrementControl> auto_increment_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  if (request->get_coordinator_map()) {
    auto is_leader = coordinator_control->IsLeader();
    DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                     << ", Request:" << request->ShortDebugString();

    if (!is_leader) {
      coordinator_control->RedirectResponse(response);
      return;
    }
  }

  int64_t epoch;
  pb::common::Location leader_location;
  std::vector<pb::common::Location> locations;
  auto *coordinator_map = response->mutable_coordinator_map();
  coordinator_control->GetCoordinatorMap(request->cluster_id(), epoch, leader_location, locations, *coordinator_map);

  response->set_epoch(epoch);

  auto *leader_location_resp = response->mutable_leader_location();
  *leader_location_resp = leader_location;

  for (const auto &member_location : locations) {
    auto *location = response->add_coordinator_locations();
    *location = member_location;
  }

  // get kv leader location
  pb::common::Location kv_leader_location;
  kv_control->GetLeaderLocation(kv_leader_location);
  *(response->mutable_kv_leader_location()) = kv_leader_location;

  // get tso leader location
  pb::common::Location tso_leader_location;
  tso_control->GetLeaderLocation(tso_leader_location);
  *(response->mutable_tso_leader_location()) = tso_leader_location;

  // get autoincrement leader location
  pb::common::Location auto_increment_leader_location;
  auto_increment_control->GetLeaderLocation(auto_increment_leader_location);
  *(response->mutable_auto_increment_leader_location()) = auto_increment_leader_location;
}

void DoConfigCoordinator(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::ConfigCoordinatorRequest *request,
                         pb::coordinator::ConfigCoordinatorResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  if (request->set_force_read_only()) {
    // setup force_read_only
    auto ret = coordinator_control->UpdateForceReadOnly(request->is_force_read_only(), meta_increment);
    if (!ret.ok()) {
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      response->mutable_error()->set_errmsg(ret.error_str());
      return;
    }
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "ConfigCoordinator: meta_increment is empty";
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ConfigCoordinator Write failed, request: " << request->ShortDebugString();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  DINGO_LOG(INFO) << "ConfigCoordinator Success, request: << " << request->ShortDebugString();
}

// Region services
void DoCreateRegionId(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::CreateRegionIdRequest *request,
                      pb::coordinator::CreateRegionIdResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  if (request->count() <= 0) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(pb::error::Errno::EILLEGAL_PARAMTETERS));
    response->mutable_error()->set_errmsg("count must be greater than 0");
    return;
  } else if (request->count() > FLAGS_max_create_id_count) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(pb::error::Errno::EILLEGAL_PARAMTETERS));
    response->mutable_error()->set_errmsg("count must be less than " + std::to_string(FLAGS_max_create_id_count));
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  std::vector<int64_t> region_ids;
  auto ret = coordinator_control->CreateRegionId(request->count(), region_ids, meta_increment);
  if (ret.ok()) {
    // generate response
    for (int i = 0; i < region_ids.size(); i++) {
      DINGO_LOG(INFO) << "CreateRegionId Success [i:" << i << " region_id=" << region_ids[i] << "]";
      response->add_region_ids(region_ids[i]);
    }
  } else {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  if (meta_increment.ByteSizeLong() == 0) {
    DINGO_LOG(WARNING) << "CreateRegionId: meta_increment is empty";
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateRegionId Write failed:  count=" << request->count();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  DINGO_LOG(INFO) << "CreateRegionId Success region_id_count =" << request->count();
}

void DoQueryRegion(google::protobuf::RpcController * /*controller*/, const pb::coordinator::QueryRegionRequest *request,
                   pb::coordinator::QueryRegionResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  auto region_id = request->region_id();

  pb::common::Region region;
  auto ret = coordinator_control->QueryRegion(region_id, region);

  if (ret.ok()) {
    *(response->mutable_region()) = region;
  } else {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }
}

void DoCreateRegion(google::protobuf::RpcController * /*controller*/,
                    const pb::coordinator::CreateRegionRequest *request,
                    pb::coordinator::CreateRegionResponse *response, TrackClosure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  const std::string &region_name = request->region_name();
  const std::string &resource_tag = request->resource_tag();
  int64_t replica_num = request->replica_num();
  const pb::common::Range &range = request->range();
  pb::common::RawEngine raw_engine = request->raw_engine();
  int64_t schema_id = request->schema_id();
  int64_t table_id = request->table_id();
  int64_t index_id = request->index_id();
  int64_t part_id = request->part_id();
  int64_t tenant_id = request->tenant_id();
  int64_t split_from_region_id = request->split_from_region_id();
  int64_t new_region_id = 0;
  pb::common::RegionType region_type = request->region_type();
  pb::common::IndexParameter index_parameter = request->index_parameter();

  if (index_parameter.has_vector_index_parameter() && region_type != pb::common::RegionType::INDEX_REGION) {
    DINGO_LOG(WARNING) << "Create Region region_type is not INDEX_REGION, but index_parameter is not empty, request: "
                       << request->ShortDebugString();
    region_type = pb::common::RegionType::INDEX_REGION;
    index_parameter.set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
  }

  if (!Helper::IsClientRaw(range.start_key()) && !Helper::IsClientTxn(range.start_key())) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(pb::error::Errno::EILLEGAL_PARAMTETERS));
    response->mutable_error()->set_errmsg("This api can only create client raw/txn region");
    return;
  }

  butil::Status ret = butil::Status::OK();
  if (split_from_region_id > 0) {
    ret = coordinator_control->CreateRegionForSplit(region_name, region_type, resource_tag, range, split_from_region_id,
                                                    new_region_id, meta_increment);
  } else if (request->store_ids_size() > 0) {
    std::vector<int64_t> store_ids;
    for (auto id : request->store_ids()) {
      store_ids.push_back(id);
    }
    std::vector<pb::coordinator::StoreOperation> store_operations;
    ret = coordinator_control->CreateRegionFinal(region_name, region_type, raw_engine, resource_tag, replica_num, range,
                                                 schema_id, table_id, index_id, part_id, tenant_id, index_parameter,
                                                 store_ids, 0, new_region_id, store_operations, meta_increment);
  } else {
    // store_ids is empty, will auto select store
    std::vector<int64_t> store_ids;
    std::vector<pb::coordinator::StoreOperation> store_operations;

    ret = coordinator_control->CreateRegionFinal(region_name, region_type, raw_engine, resource_tag, replica_num, range,
                                                 schema_id, table_id, index_id, part_id, tenant_id, index_parameter,
                                                 store_ids, 0, new_region_id, store_operations, meta_increment);
  }

  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create Region Failed, errno=" << ret << " Request:" << request->ShortDebugString();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
  response->set_region_id(new_region_id);

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateRegion Write failed:  region_name=" << request->region_name();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoDropRegion(google::protobuf::RpcController * /*controller*/, const pb::coordinator::DropRegionRequest *request,
                  pb::coordinator::DropRegionResponse *response, TrackClosure *done,
                  std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto region_id = request->region_id();

  // auto ret = coordinator_control->DropRegion(region_id, true, meta_increment);
  auto ret = coordinator_control->DropRegion(region_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Drop Region Failed, errno=" << ret << " Request:" << request->ShortDebugString();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropRegion Write failed:  region_id=" << request->region_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoDropRegionPermanently(google::protobuf::RpcController * /*controller*/,
                             const pb::coordinator::DropRegionPermanentlyRequest *request,
                             pb::coordinator::DropRegionPermanentlyResponse *response, TrackClosure *done,
                             std::shared_ptr<CoordinatorControl> coordinator_control,
                             std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto region_id = request->region_id();
  auto cluster_id = request->cluster_id();

  auto ret = coordinator_control->DropRegionPermanently(region_id, meta_increment);

  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropRegionPermanently Write failed:  region_id=" << request->region_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoSplitRegion(google::protobuf::RpcController * /*controller*/, const pb::coordinator::SplitRegionRequest *request,
                   pb::coordinator::SplitRegionResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_split_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  const auto &split_request = request->split_request();
  bool store_create_region = request->split_request().store_create_region();

  auto ret = coordinator_control->SplitRegionWithTaskList(
      split_request.split_from_region_id(), split_request.split_to_region_id(), split_request.split_watershed_key(),
      store_create_region, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "SplitRegion Write failed:  split_from_region_id="
                     << request->split_request().split_from_region_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoMergeRegion(google::protobuf::RpcController * /*controller*/, const pb::coordinator::MergeRegionRequest *request,
                   pb::coordinator::MergeRegionResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_merge_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  const auto &merge_request = request->merge_request();

  auto ret = coordinator_control->MergeRegionWithTaskList(merge_request.source_region_id(),
                                                          merge_request.target_region_id(), meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "MergeRegion Write failed:  merge_from_region_id="
                     << request->merge_request().source_region_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoChangePeerRegion(google::protobuf::RpcController * /*controller*/,
                        const pb::coordinator::ChangePeerRegionRequest *request,
                        pb::coordinator::ChangePeerRegionResponse *response, TrackClosure *done,
                        std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (!request->has_change_peer_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  const auto &change_peer_request = request->change_peer_request();
  if (!change_peer_request.has_region_definition()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  const auto &region_definition = change_peer_request.region_definition();
  if (region_definition.peers_size() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<int64_t> new_store_ids;
  for (const auto &it : region_definition.peers()) {
    new_store_ids.push_back(it.store_id());
  }

  auto ret = coordinator_control->ChangePeerRegionWithTaskList(region_definition.id(), new_store_ids, meta_increment);

  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ChangePeerRegion Write failed:  region_id="
                     << request->change_peer_request().region_definition().id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoTransferLeaderRegion(google::protobuf::RpcController * /*controller*/,
                            const pb::coordinator::TransferLeaderRegionRequest *request,
                            pb::coordinator::TransferLeaderRegionResponse *response, TrackClosure *done,
                            std::shared_ptr<CoordinatorControl> coordinator_control,
                            std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // validate region_cmd
  if (request->region_id() <= 0 || request->leader_store_id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("region_id or leader_store_id is illegal");
    return;
  }

  auto ret = coordinator_control->TransferLeaderRegionWithTaskList(request->region_id(), request->leader_store_id(),
                                                                   meta_increment);

  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // if meta_increment is empty, means no need to update meta
  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion Write failed:  region_id=" << request->region_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoGetOrphanRegion(google::protobuf::RpcController * /*controller*/,
                       const pb::coordinator::GetOrphanRegionRequest *request,
                       pb::coordinator::GetOrphanRegionResponse *response, TrackClosure *done,
                       std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  std::map<int64_t, pb::common::RegionMetrics> orphan_regions;
  auto ret = coordinator_control->GetOrphanRegion(request->store_id(), orphan_regions);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (orphan_regions.empty()) {
    return;
  }

  for (const auto &it : orphan_regions) {
    response->mutable_orphan_regions()->insert({it.first, it.second});
  }
}

// StoreOperation service
void DoGetStoreOperation(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::GetStoreOperationRequest *request,
                         pb::coordinator::GetStoreOperationResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreOperation Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  // if store_id = 0, get all store operation
  if (request->store_id() == 0) {
    pb::common::StoreMap store_map;
    coordinator_control->GetStoreMap(store_map);

    for (const auto &store : store_map.stores()) {
      auto *new_store_operation = response->add_store_operations();
      coordinator_control->GetStoreOperation(store.id(), *new_store_operation);
    }
    return;
  }

  // get store_operation for id
  auto *store_operation = response->add_store_operations();
  coordinator_control->GetStoreOperation(request->store_id(), *store_operation);
}

void DoCleanStoreOperation(google::protobuf::RpcController * /*controller*/,
                           const pb::coordinator::CleanStoreOperationRequest *request,
                           pb::coordinator::CleanStoreOperationResponse *response, TrackClosure *done,
                           std::shared_ptr<CoordinatorControl> coordinator_control,
                           std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto store_id = request->store_id();

  auto ret = coordinator_control->CleanStoreOperation(store_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CleanStoreOperation failed, store_id:" << store_id << ", errcode:" << ret.error_code()
                     << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CleanStoreOperation Write failed:  store_id=" << request->store_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoAddStoreOperation(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::AddStoreOperationRequest *request,
                         pb::coordinator::AddStoreOperationResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  const auto &store_operation = request->store_operation();

  auto ret = coordinator_control->AddStoreOperation(store_operation, true, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "AddStoreOperation failed, store_id:" << store_operation.id()
                     << ", errcode:" << ret.error_code() << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "AddStoreOperation Write failed:  store_id=" << request->store_operation().id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoRemoveStoreOperation(google::protobuf::RpcController * /*controller*/,
                            const pb::coordinator::RemoveStoreOperationRequest *request,
                            pb::coordinator::RemoveStoreOperationResponse *response, TrackClosure *done,
                            std::shared_ptr<CoordinatorControl> coordinator_control,
                            std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto store_id = request->store_id();
  auto region_cmd_id = request->region_cmd_id();

  auto ret = coordinator_control->RemoveRegionCmd(store_id, region_cmd_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "RemoveStoreOperation failed, store_id:" << store_id << ", region_cmd_id:" << region_cmd_id
                     << ", errcode:" << ret.error_code() << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "RemoveStoreOperation Write failed:  store_id=" << request->store_id()
                     << ", region_cmd_id=" << request->region_cmd_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoGetRegionCmd(google::protobuf::RpcController * /*controller*/,
                    const pb::coordinator::GetRegionCmdRequest *request,
                    pb::coordinator::GetRegionCmdResponse *response, TrackClosure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  std::vector<pb::coordinator::RegionCmd> region_cmds;
  std::vector<pb::error::Error> region_cmd_errors;
  auto ret = coordinator_control->GetRegionCmd(request->store_id(), request->start_region_cmd_id(),
                                               request->end_region_cmd_id(), region_cmds, region_cmd_errors);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GetRegionCmd failed, store_id:" << request->store_id()
                     << ", start_region_cmd_id:" << request->start_region_cmd_id()
                     << ", end_region_cmd_id:" << request->end_region_cmd_id() << ", errcode:" << ret.error_code()
                     << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  for (const auto &it : region_cmds) {
    auto *new_region_cmd = response->add_region_cmds();
    *new_region_cmd = it;
  }

  for (const auto &it : region_cmd_errors) {
    auto *new_region_cmd_error = response->add_region_cmd_errors();
    *new_region_cmd_error = it;
  }
}

// task list
void DoGetTaskList(google::protobuf::RpcController * /*controller*/, const pb::coordinator::GetTaskListRequest *request,
                   pb::coordinator::GetTaskListResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreOperation Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  if (request->task_list_id() == 0) {
    butil::FlatMap<int64_t, pb::coordinator::TaskList> task_lists;
    task_lists.init(100);
    coordinator_control->GetTaskListAll(task_lists);

    if (request->get_task_list_id_only()) {
      for (const auto &it : task_lists) {
        auto *new_task_list = response->add_task_lists();
        new_task_list->set_id(it.first);
      }
    } else {
      for (const auto &it : task_lists) {
        auto *new_task_list = response->add_task_lists();
        *new_task_list = it.second;
        for (int i = 0; i < new_task_list->tasks_size(); ++i) {
          new_task_list->mutable_tasks(i)->set_step(i);
        }
      }
    }
  } else {
    pb::coordinator::TaskList task_list;
    coordinator_control->GetTaskList(request->task_list_id(), task_list);
    *response->add_task_lists() = task_list;
    for (int i = 0; i < task_list.tasks_size(); ++i) {
      response->mutable_task_lists(0)->mutable_tasks(i)->set_step(i);
    }
  }
}

void DoCleanTaskList(google::protobuf::RpcController * /*controller*/,
                     const pb::coordinator::CleanTaskListRequest *request,
                     pb::coordinator::CleanTaskListResponse *response, TrackClosure *done,
                     std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive Clean TaskList Request, IsLeader:" << is_leader
                       << ", Request:" << request->ShortDebugString();
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto task_list_id = request->task_list_id();

  auto ret = coordinator_control->CleanTaskList(task_list_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CleanTaskList failed, task_list_id:" << task_list_id << ", errcode:" << ret.error_code()
                     << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CleanTaskList Write failed:  task_list_id=" << request->task_list_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

// raft control
void DoRaftControl(google::protobuf::RpcController *controller, const pb::coordinator::RaftControlRequest *request,
                   pb::coordinator::RaftControlResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<KvControl> kv_control,
                   std::shared_ptr<TsoControl> tso_control,
                   std::shared_ptr<AutoIncrementControl> auto_increment_control,
                   std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
  int64_t log_id = 0;
  if (cntl->has_log_id()) {
    log_id = cntl->log_id();
  }

  std::shared_ptr<RaftNode> raft_node;
  bool is_leader = false;
  if (request->node_index() == pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex) {
    raft_node = coordinator_control->GetRaftNode();
    is_leader = raft_node->IsLeader();
    if (!is_leader && request->op_type() != pb::coordinator::RaftControlOp::Snapshot) {
      return coordinator_control->RedirectResponse(response);
    }
  } else if (request->node_index() == pb::coordinator::RaftControlNodeIndex::KvNodeIndex) {
    raft_node = kv_control->GetRaftNode();
    is_leader = raft_node->IsLeader();
    if (!is_leader && request->op_type() != pb::coordinator::RaftControlOp::Snapshot) {
      return kv_control->RedirectResponse(response);
    }
  } else if (request->node_index() == pb::coordinator::RaftControlNodeIndex::TsoNodeIndex) {
    raft_node = tso_control->GetRaftNode();
    is_leader = raft_node->IsLeader();
    if (!is_leader && request->op_type() != pb::coordinator::RaftControlOp::Snapshot) {
      return tso_control->RedirectResponse(response);
    }
  } else if (request->node_index() == pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex) {
    raft_node = auto_increment_control->GetRaftNode();
    is_leader = raft_node->IsLeader();
    if (!is_leader && request->op_type() != pb::coordinator::RaftControlOp::Snapshot) {
      return auto_increment_control->RedirectResponse(response);
    }
  }

  switch (request->op_type()) {
    case pb::coordinator::RaftControlOp::None: {
      response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      response->mutable_error()->set_errmsg("op_type is None, for test only");
      DINGO_LOG(ERROR) << "node:" << raft_node->GetRaftGroupName() << " " << raft_node->GetPeerId().to_string()
                       << " op_type is None, log_id:" << log_id;
      return;
    }
    case pb::coordinator::RaftControlOp::AddPeer: {
      // get raft location from add_peer
      // auto endpoint = Helper::StrToEndPoint(request->add_peer());
      // auto location = Helper::EndPointToLocation(endpoint);

      // pb::common::Location raft_location;
      // coordinator_control->GetRaftLocation(location, raft_location);
      // if (raft_location.ByteSizeLong() == 0) {
      //   response->mutable_error()->set_errcode(::dingodb::pb::error::pb::error::Errno::EILLEGAL_PARAMTETERS);
      //   response->mutable_error()->set_errmsg("add_peer not found, peer is " + request->add_peer());
      //   DINGO_LOG(ERROR) << "node:" << raft_node->GetRaftGroupName() << " " << raft_node->GetPeerId().to_string()
      //                    << " location:" << location.DebugString() << "add_peer=" << request->add_peer()
      //                    << " not found, log_id:" << log_id;
      //   return;
      // }

      // braft::PeerId add_peer(Helper::LocationToEndPoint(raft_location));

      DINGO_LOG(INFO) << "AddPeer:" << request->add_peer();
      braft::PeerId add_peer(Helper::StrToEndPoint(request->add_peer()));
      RaftControlClosure *add_peer_done =
          new RaftControlClosure(cntl, request, response, done_guard.release(), raft_node);
      raft_node->AddPeer(add_peer, add_peer_done);
      return;
    }
    case pb::coordinator::RaftControlOp::RemovePeer: {
      // get raft location from remove_peer
      // auto endpoint = Helper::StrToEndPoint(request->remove_peer());
      // auto location = Helper::EndPointToLocation(endpoint);

      // pb::common::Location raft_location;
      // coordinator_control->GetRaftLocation(location, raft_location);
      // if (raft_location.ByteSizeLong() == 0) {
      //   response->mutable_error()->set_errcode(::dingodb::pb::error::pb::error::Errno::EILLEGAL_PARAMTETERS);
      //   response->mutable_error()->set_errmsg("remove_peer not found, peer is " + request->remove_peer());
      //   DINGO_LOG(ERROR) << "node:" << raft_node->GetRaftGroupName() << " " << raft_node->GetPeerId().to_string()
      //                    << " location:" << location.DebugString() << "remove_peer=" << request->remove_peer()
      //                    << " not found, log_id:" << log_id;
      //   return;
      // }

      // braft::PeerId remove_peer(Helper::LocationToEndPoint(raft_location));

      DINGO_LOG(INFO) << "RemovePeer:" << request->remove_peer();
      braft::PeerId remove_peer(Helper::StrToEndPoint(request->remove_peer()));
      RaftControlClosure *remove_peer_done =
          new RaftControlClosure(cntl, request, response, done_guard.release(), raft_node);
      raft_node->RemovePeer(remove_peer, remove_peer_done);
      return;
    }
    case pb::coordinator::RaftControlOp::TransferLeader: {
      DINGO_LOG(INFO) << "TransferLeader:" << request->new_leader();
      braft::PeerId transfer_leader(Helper::StrToEndPoint(request->new_leader()));
      auto errcode = raft_node->TransferLeadershipTo(transfer_leader);
      if (errcode != 0) {
        response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(errcode));
      }
      return;
    }
    case pb::coordinator::RaftControlOp::Snapshot: {
      DINGO_LOG(INFO) << "Snapshot:" << request->node_index();
      RaftControlClosure *snapshot_done =
          new RaftControlClosure(cntl, request, response, done_guard.release(), raft_node);
      auto ctx = std::make_shared<Context>();
      ctx->SetDone(snapshot_done);
      raft_node->Snapshot(ctx, true);
      return;
    }
    case pb::coordinator::RaftControlOp::ResetPeer: {
      DINGO_LOG(INFO) << fmt::format("SetPeer: {}", request->new_peers().size());
      std::vector<braft::PeerId> new_peers;
      for (const auto &peer : request->new_peers()) {
        DINGO_LOG(INFO) << fmt::format("SetPeer: {}", peer);
        new_peers.emplace_back(Helper::StrToEndPoint(peer));
      }
      braft::Configuration new_conf(new_peers);
      auto status = raft_node->ResetPeers(new_conf);
      if (!status.ok()) {
        response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
        response->mutable_error()->set_errmsg(status.error_str());
        DINGO_LOG(ERROR) << "node:" << raft_node->GetRaftGroupName() << " " << raft_node->GetPeerId().to_string()
                         << " reset peers failed, log_id:" << log_id;
        return;
      }
      return;
    }
    default:
      response->mutable_error()->set_errcode(::dingodb::pb::error::Errno::ENOT_SUPPORT);
      DINGO_LOG(ERROR) << "node:" << raft_node->GetRaftGroupName() << " " << raft_node->GetPeerId().to_string()
                       << " unsupport request type:" << request->op_type() << ", log_id:" << log_id;
      return;
  }
}

void DoScanRegions(google::protobuf::RpcController * /*controller*/, const pb::coordinator::ScanRegionsRequest *request,
                   pb::coordinator::ScanRegionsResponse *response, TrackClosure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  std::vector<pb::coordinator_internal::RegionInternal> regions;
  auto ret = coordinator_control->ScanRegions(request->key(), request->range_end(), request->limit(), regions);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "ScanRegions failed, start_region_id:" << request->key()
                     << ", end_region_id:" << request->range_end() << ", limit: " << request->limit()
                     << ", errcode:" << ret.error_code() << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  for (const auto &part_region : regions) {
    auto *new_region = response->add_regions();
    new_region->set_region_id(part_region.definition().id());

    // region epoch
    *(new_region->mutable_region_epoch()) = part_region.definition().epoch();

    // region status
    auto *region_status = new_region->mutable_status();
    region_status->set_state(part_region.state());

    int64_t leader_id = 0;
    pb::common::RegionStatus inner_region_status;
    coordinator_control->GetRegionLeaderAndStatus(part_region.id(), inner_region_status, leader_id);

    region_status->set_raft_status(inner_region_status.raft_status());
    region_status->set_replica_status(inner_region_status.replica_status());
    region_status->set_heartbeat_state(inner_region_status.heartbeat_status());
    region_status->set_last_update_timestamp(inner_region_status.last_update_timestamp());

    region_status->set_region_type(part_region.region_type());
    region_status->set_create_timestamp(part_region.create_timestamp());

    // new_region range
    auto *part_range = new_region->mutable_range();
    *part_range = part_region.definition().range();

    // new_region leader location
    auto *leader_location = new_region->mutable_leader();

    // new_region voter & learner locations
    for (int j = 0; j < part_region.definition().peers_size(); j++) {
      const auto &part_peer = part_region.definition().peers(j);
      if (part_peer.store_id() == leader_id) {
        *leader_location = part_peer.server_location();
      }

      if (part_peer.role() == ::dingodb::pb::common::PeerRole::VOTER) {
        auto *voter_location = new_region->add_voters();
        *voter_location = part_peer.server_location();
      } else if (part_peer.role() == ::dingodb::pb::common::PeerRole::LEARNER) {
        auto *learner_location = new_region->add_learners();
        *learner_location = part_peer.server_location();
      }
    }
  }
}

void DoGetRangeRegionMap(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::GetRangeRegionMapRequest * /*request*/,
                         pb::coordinator::GetRangeRegionMapResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  std::vector<std::string> start_keys;
  std::vector<pb::coordinator_internal::RegionInternal> regions;
  auto ret = coordinator_control->GetRangeRegionMap(start_keys, regions);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  if (start_keys.size() != regions.size()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
    response->mutable_error()->set_errmsg("start_keys size not equal regions size");
    return;
  }

  for (int i = 0; i < start_keys.size(); i++) {
    auto *new_range_region_map = response->add_range_regions();
    new_range_region_map->set_start_key(start_keys[i]);
    new_range_region_map->set_end_key(regions[i].definition().range().end_key());
    new_range_region_map->set_region_id(regions[i].definition().id());
  }
}

void DoUpdateGCSafePoint(google::protobuf::RpcController * /*controller*/,
                         const pb::coordinator::UpdateGCSafePointRequest *request,
                         pb::coordinator::UpdateGCSafePointResponse *response, TrackClosure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_gc_safe_point = 0;
  bool gc_stop = false;
  std::map<int64_t, int64_t> tenant_safe_points;

  for (const auto &[tenant_id, safe_point] : request->tenant_safe_points()) {
    tenant_safe_points.insert({tenant_id, safe_point});
  }

  auto ret = coordinator_control->UpdateGCSafePoint(request->safe_point(), request->gc_flag(), new_gc_safe_point,
                                                    gc_stop, tenant_safe_points, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "UpdateGCSafePoint failed, gc_safe_point:" << request->safe_point()
                     << ", errcode:" << ret.error_code() << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_new_safe_point(new_gc_safe_point);
  response->set_gc_stop(gc_stop);

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateGCSafePoint Write failed:  gc_safe_point=" << request->safe_point();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void DoGetGCSafePoint(google::protobuf::RpcController * /*controller*/,
                      const pb::coordinator::GetGCSafePointRequest *request,
                      pb::coordinator::GetGCSafePointResponse *response, TrackClosure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  int64_t gc_safe_point = 0;
  bool gc_stop = false;

  std::vector<int64_t> tenant_ids;
  if (!request->tenant_ids().empty()) {
    tenant_ids.reserve(request->tenant_ids().size());
    for (const auto &tenant_id : request->tenant_ids()) {
      tenant_ids.push_back(tenant_id);
    }
  }

  std::map<int64_t, int64_t> tenant_safe_points;

  auto ret = coordinator_control->GetGCSafePoint(gc_safe_point, gc_stop, tenant_ids, request->get_all_tenant(),
                                                 tenant_safe_points);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GetGCSafePoint failed, errcode:" << ret.error_code() << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  response->set_safe_point(gc_safe_point);
  response->set_gc_stop(gc_stop);

  if (!tenant_safe_points.empty()) {
    auto *new_tenant_safe_points = response->mutable_tenant_safe_points();
    for (const auto [tenant_id, safe_point] : tenant_safe_points) {
      new_tenant_safe_points->insert({tenant_id, safe_point});
    }
  }

  DINGO_LOG(INFO) << "Response GetGCSafePoint Request:" << response->ShortDebugString();
}

void DoUpdateRegionCmdStatus(google::protobuf::RpcController * /*controller*/,
                             const pb::coordinator::UpdateRegionCmdStatusRequest *request,
                             pb::coordinator::UpdateRegionCmdStatusResponse *response, TrackClosure *done,
                             std::shared_ptr<CoordinatorControl> coordinator_control,
                             std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto is_leader = coordinator_control->IsLeader();
  if (!is_leader) {
    return coordinator_control->RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->UpdateRegionCmdStatus(request->task_list_id(), request->region_cmd_id(),
                                                        request->status(), request->error(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "UpdateRegionCmdStatus failed, task_list_id:" << request->task_list_id()
                     << ", region_cmd_id:" << request->region_cmd_id() << ", status:" << request->status()
                     << ", errcode:" << pb::error::Errno_Name(ret.error_code()) << ", errmsg:" << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);
  ctx->SetTracker(tracker);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateRegionCmdStatus Write failed:  task_list_id=" << request->task_list_id()
                     << ", region_cmd_id=" << request->region_cmd_id();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }
}

void CoordinatorServiceImpl::CreateExecutor(google::protobuf::RpcController *controller,
                                            const pb::coordinator::CreateExecutorRequest *request,
                                            pb::coordinator::CreateExecutorResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateExecutor(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DeleteExecutor(google::protobuf::RpcController *controller,
                                            const pb::coordinator::DeleteExecutorRequest *request,
                                            pb::coordinator::DeleteExecutorResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Executor Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  if (request->executor().id().length() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteExecutor(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CreateExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::CreateExecutorUserRequest *request,
                                                pb::coordinator::CreateExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateExecutorUser(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::UpdateExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::UpdateExecutorUserRequest *request,
                                                pb::coordinator::UpdateExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Update Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateExecutorUser(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DeleteExecutorUser(google::protobuf::RpcController *controller,
                                                const pb::coordinator::DeleteExecutorUserRequest *request,
                                                pb::coordinator::DeleteExecutorUserResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Delete Executor User Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteExecutorUser(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetExecutorUserMap(google::protobuf::RpcController *controller,
                                                const pb::coordinator::GetExecutorUserMapRequest *request,
                                                pb::coordinator::GetExecutorUserMapResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Get Executor User Map Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetExecutorUserMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CreateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::CreateStoreRequest *request,
                                         pb::coordinator::CreateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Create Store Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateStore(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DeleteStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::DeleteStoreRequest *request,
                                         pb::coordinator::DeleteStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Create Store Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  if (request->store_id() == 0) {
    auto *error = response->mutable_error();
    error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteStore(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::UpdateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::UpdateStoreRequest *request,
                                         pb::coordinator::UpdateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive Update Store Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateStore(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::ExecutorHeartbeat(google::protobuf::RpcController *controller,
                                               const pb::coordinator::ExecutorHeartbeatRequest *request,
                                               pb::coordinator::ExecutorHeartbeatResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Executor Heartbeat Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  if (!request->has_executor()) {
    auto *error = response->mutable_error();
    error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "ExecutorHeartBeat has_executor() is false, reject heartbeat";
    return;
  }

  pb::common::Executor executor = request->executor();

  if (executor.id().length() <= 0) {
    DINGO_LOG(DEBUG) << "ExecutorHeartBeat generate executor_id, executor_id=" << executor.server_location().host()
                     << ":" << executor.server_location().port();
    executor.set_id(executor.server_location().host() + ":" + std::to_string(executor.server_location().port()));
  }

  auto ret1 = coordinator_control_->ValidateExecutorUser(executor.executor_user());
  if (!ret1) {
    DINGO_LOG(ERROR) << "ExecutorHeartBeat ValidateExecutor failed, reject heardbeat, executor_id="
                     << request->executor().id() << " keyring=" << request->executor().executor_user().keyring();
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoExecutorHeartbeat(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController *controller,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Store Heartbeat Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // validate store
  if (!request->has_store()) {
    auto *error = response->mutable_error();
    error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    DINGO_LOG(ERROR) << "StoreHeartBeat has_store() is false, reject heartbeat";
    return;
  }

  auto ret1 = this->coordinator_control_->ValidateStore(request->store().id(), request->store().keyring());
  if (ret1) {
    DINGO_LOG(ERROR) << "StoreHeartBeat ValidateStore failed, reject heardbeat, store_id=" << request->store().id()
                     << " keyring=" << request->store().keyring();
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoStoreHeartbeat(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController *controller,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetStoreMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetStoreMetrics(google::protobuf::RpcController *controller,
                                             const pb::coordinator::GetStoreMetricsRequest *request,
                                             pb::coordinator::GetStoreMetricsResponse *response,
                                             google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetStoreMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DeleteStoreMetrics(google::protobuf::RpcController *controller,
                                                const pb::coordinator::DeleteStoreMetricsRequest *request,
                                                pb::coordinator::DeleteStoreMetricsResponse *response,
                                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Delete StoreMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteStoreMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetRegionMetrics(google::protobuf::RpcController *controller,
                                              const pb::coordinator::GetRegionMetricsRequest *request,
                                              pb::coordinator::GetRegionMetricsResponse *response,
                                              google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRegionMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DeleteRegionMetrics(google::protobuf::RpcController *controller,
                                                 const pb::coordinator::DeleteRegionMetricsRequest *request,
                                                 pb::coordinator::DeleteRegionMetricsResponse *response,
                                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Delete RegionMetrics Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteRegionMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetExecutorMap(google::protobuf::RpcController *controller,
                                            const pb::coordinator::GetExecutorMapRequest *request,
                                            pb::coordinator::GetExecutorMapResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get ExecutorMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetExecutorMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetRegionMap(google::protobuf::RpcController *controller,
                                          const pb::coordinator::GetRegionMapRequest *request,
                                          pb::coordinator::GetRegionMapResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRegionMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetDeletedRegionMap(google::protobuf::RpcController *controller,
                                                 const pb::coordinator::GetDeletedRegionMapRequest *request,
                                                 pb::coordinator::GetDeletedRegionMapResponse *response,
                                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetDeletedRegionMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::AddDeletedRegionMap(google::protobuf::RpcController *controller,
                                                 const pb::coordinator::AddDeletedRegionMapRequest *request,
                                                 pb::coordinator::AddDeletedRegionMapResponse *response,
                                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoAddDeletedRegionMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CleanDeletedRegionMap(google::protobuf::RpcController *controller,
                                                   const pb::coordinator::CleanDeletedRegionMapRequest *request,
                                                   pb::coordinator::CleanDeletedRegionMapResponse *response,
                                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanDeletedRegionMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetRegionCount(google::protobuf::RpcController *controller,
                                            const pb::coordinator::GetRegionCountRequest *request,
                                            pb::coordinator::GetRegionCountResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get RegionMap Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRegionCount(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetCoordinatorMap(google::protobuf::RpcController *controller,
                                               const pb::coordinator::GetCoordinatorMapRequest *request,
                                               pb::coordinator::GetCoordinatorMapResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Get CoordinatorMap Request:" << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetCoordinatorMap(controller, request, response, svr_done, coordinator_control_, kv_control_, tso_control_,
                        auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::ConfigCoordinator(google::protobuf::RpcController *controller,
                                               const pb::coordinator::ConfigCoordinatorRequest *request,
                                               pb::coordinator::ConfigCoordinatorResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "Receive Config Coordinator Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoConfigCoordinator(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

// Region services
void CoordinatorServiceImpl::CreateRegionId(google::protobuf::RpcController *controller,
                                            const pb::coordinator::CreateRegionIdRequest *request,
                                            pb::coordinator::CreateRegionIdResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Create Region Id Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateRegionId(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::QueryRegion(google::protobuf::RpcController *controller,
                                         const pb::coordinator::QueryRegionRequest *request,
                                         pb::coordinator::QueryRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Query Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoQueryRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CreateRegion(google::protobuf::RpcController *controller,
                                          const pb::coordinator::CreateRegionRequest *request,
                                          pb::coordinator::CreateRegionResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Create Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  if (request->has_index_parameter()) {
    const auto &index_parameter = request->index_parameter();
    if (index_parameter.index_type() == pb::common::IndexType::INDEX_TYPE_NONE) {
      DINGO_LOG(ERROR) << "CreateRegion index_type is INDEX_TYPE_NONE, reject create region";
      ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS,
                              "index_type is INDEX_TYPE_NONE");
      return;
    }

    if (index_parameter.has_vector_index_parameter() &&
        index_parameter.index_type() != pb::common::IndexType::INDEX_TYPE_VECTOR) {
      DINGO_LOG(ERROR) << "CreateRegion index_type is not INDEX_TYPE_VECTOR, reject create region";
      ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS,
                              "index_type is not INDEX_TYPE_VECTOR");
      return;
    }

    if (index_parameter.has_scalar_index_parameter() &&
        index_parameter.index_type() != pb::common::IndexType::INDEX_TYPE_SCALAR) {
      DINGO_LOG(ERROR) << "CreateRegion index_type is not INDEX_TYPE_SCALAR, reject create region";
      ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS,
                              "index_type is not INDEX_TYPE_SCALAR");
      return;
    }

    if (index_parameter.has_vector_index_parameter()) {
      auto ret = VectorIndexUtils::ValidateVectorIndexParameter(index_parameter.vector_index_parameter());
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "CreateRegion vector_index_parameter is invalid, reject create region";
        ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS,
                                "vector_index_parameter is invalid");
        return;
      }
    }
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DropRegion(google::protobuf::RpcController *controller,
                                        const pb::coordinator::DropRegionRequest *request,
                                        pb::coordinator::DropRegionResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Drop Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::DropRegionPermanently(google::protobuf::RpcController *controller,
                                                   const pb::coordinator::DropRegionPermanentlyRequest *request,
                                                   pb::coordinator::DropRegionPermanentlyResponse *response,
                                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Drop Region Permanently Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropRegionPermanently(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::SplitRegion(google::protobuf::RpcController *controller,
                                         const pb::coordinator::SplitRegionRequest *request,
                                         pb::coordinator::SplitRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Split Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoSplitRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::MergeRegion(google::protobuf::RpcController *controller,
                                         const pb::coordinator::MergeRegionRequest *request,
                                         pb::coordinator::MergeRegionResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Merge Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoMergeRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::ChangePeerRegion(google::protobuf::RpcController *controller,
                                              const pb::coordinator::ChangePeerRegionRequest *request,
                                              pb::coordinator::ChangePeerRegionResponse *response,
                                              google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Change Peer Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoChangePeerRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::TransferLeaderRegion(google::protobuf::RpcController *controller,
                                                  const pb::coordinator::TransferLeaderRegionRequest *request,
                                                  pb::coordinator::TransferLeaderRegionResponse *response,
                                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Transfer Leader Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTransferLeaderRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetOrphanRegion(google::protobuf::RpcController *controller,
                                             const pb::coordinator::GetOrphanRegionRequest *request,
                                             pb::coordinator::GetOrphanRegionResponse *response,
                                             google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive Get Orphan Region Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetOrphanRegion(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

// StoreOperation service
void CoordinatorServiceImpl::GetStoreOperation(google::protobuf::RpcController *controller,
                                               const pb::coordinator::GetStoreOperationRequest *request,
                                               pb::coordinator::GetStoreOperationResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreOperation Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetStoreOperation(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CleanStoreOperation(google::protobuf::RpcController *controller,
                                                 const pb::coordinator::CleanStoreOperationRequest *request,
                                                 pb::coordinator::CleanStoreOperationResponse *response,
                                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive CleanStoreOperation Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanStoreOperation(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::AddStoreOperation(google::protobuf::RpcController *controller,
                                               const pb::coordinator::AddStoreOperationRequest *request,
                                               pb::coordinator::AddStoreOperationResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive AddStoreOperation Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoAddStoreOperation(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::RemoveStoreOperation(google::protobuf::RpcController *controller,
                                                  const pb::coordinator::RemoveStoreOperationRequest *request,
                                                  pb::coordinator::RemoveStoreOperationResponse *response,
                                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive RemoveStoreOperation Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoRemoveStoreOperation(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetRegionCmd(google::protobuf::RpcController *controller,
                                          const pb::coordinator::GetRegionCmdRequest *request,
                                          pb::coordinator::GetRegionCmdResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive GetRegionCmd Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRegionCmd(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

// task list
void CoordinatorServiceImpl::GetTaskList(google::protobuf::RpcController *controller,
                                         const pb::coordinator::GetTaskListRequest *request,
                                         pb::coordinator::GetTaskListResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Get StoreOperation Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTaskList(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::CleanTaskList(google::protobuf::RpcController *controller,
                                           const pb::coordinator::CleanTaskListRequest *request,
                                           pb::coordinator::CleanTaskListResponse *response,
                                           google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = coordinator_control_->IsLeader();
  DINGO_LOG(DEBUG) << "Receive Clean TaskList Request, IsLeader:" << is_leader
                   << ", Request:" << request->ShortDebugString();

  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanTaskList(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

// raft control
void CoordinatorServiceImpl::RaftControl(google::protobuf::RpcController *controller,
                                         const pb::coordinator::RaftControlRequest *request,
                                         pb::coordinator::RaftControlResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "Receive RaftControl Request:" << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoRaftControl(controller, request, response, svr_done, coordinator_control_, kv_control_, tso_control_,
                  auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::ScanRegions(google::protobuf::RpcController *controller,
                                         const pb::coordinator::ScanRegionsRequest *request,
                                         pb::coordinator::ScanRegionsResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive ScanRegions Request:" << request->ShortDebugString();

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoScanRegions(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetRangeRegionMap(google::protobuf::RpcController *controller,
                                               const pb::coordinator::GetRangeRegionMapRequest *request,
                                               pb::coordinator::GetRangeRegionMapResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive GetRangeRegionMap Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }
  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRangeRegionMap(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
  }
}

void CoordinatorServiceImpl::UpdateGCSafePoint(google::protobuf::RpcController *controller,
                                               const pb::coordinator::UpdateGCSafePointRequest *request,
                                               pb::coordinator::UpdateGCSafePointResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "Receive UpdateGCSafePoint Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateGCSafePoint(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::GetGCSafePoint(google::protobuf::RpcController *controller,
                                            const pb::coordinator::GetGCSafePointRequest *request,
                                            pb::coordinator::GetGCSafePointResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive GetGCSafePoint Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetGCSafePoint(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void CoordinatorServiceImpl::UpdateRegionCmdStatus(google::protobuf::RpcController *controller,
                                                   const pb::coordinator::UpdateRegionCmdStatusRequest *request,
                                                   pb::coordinator::UpdateRegionCmdStatusResponse *response,
                                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Receive UpdateRegionCmdStatus Request:" << request->ShortDebugString();

  auto is_leader = coordinator_control_->IsLeader();
  if (!is_leader) {
    return coordinator_control_->RedirectResponse(response);
  }
  if (request->task_list_id() <= 0) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS, "task_list_id is illegal");
    return;
  }
  if (request->region_cmd_id() <= 0) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS, "region_cmd_id is illegal");
    return;
  }
  if (request->status() == pb::coordinator::RegionCmdStatus::STATUS_DONE) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS, "status is empty");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateRegionCmdStatus(controller, request, response, svr_done, coordinator_control_, engine_);
  });

  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
  }
}

}  // namespace dingodb
