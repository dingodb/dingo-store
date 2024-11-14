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

#include "server/diskann_service.h"

#include <cstdint>
#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "common/logging.h"
#include "diskann/diskann_item_runtime.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/service_helper.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

static butil::Status ValidateVectorNewRequest(const ::dingodb::pb::diskann::VectorNewRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (!request->has_vector_index_parameter()) {
    std::string s = "vector_index_parameter is empty";
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (!request->vector_index_parameter().has_diskann_parameter()) {
    std::string s = "diskann_parameter is empty";
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  butil::Status status = VectorIndexUtils::ValidateDiskannParameter(request->vector_index_parameter());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

static void DoVectorNew(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                        const ::dingodb::pb::diskann::VectorNewRequest* request,
                        ::dingodb::pb::diskann::VectorNewResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorNewRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorNew(ctx, request->vector_index_id(), request->vector_index_parameter());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorNew(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::diskann::VectorNewRequest* request,
                                   ::dingodb::pb::diskann::VectorNewResponse* response,
                                   ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorNew(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorPushData(const ::dingodb::pb::diskann::VectorPushDataRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (request->vectors_size() != request->vector_ids_size()) {
    std::string s = fmt::format("vectors_size : {} is not equal to vector_ids_size : {}", request->vectors_size(),
                                request->vector_ids_size());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (request->tso() <= 0) {
    std::string s = fmt::format("Invalid tso : {}", request->tso());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (request->ts() < 0) {
    std::string s = fmt::format("Invalid ts : {}", request->ts());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorPushData(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                             const ::dingodb::pb::diskann::VectorPushDataRequest* request,
                             ::dingodb::pb::diskann::VectorPushDataResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorPushData(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  auto vectors = std::vector<pb::common::Vector>();
  auto vector_ids = std::vector<int64_t>();
  for (int i = 0; i < request->vectors_size(); i++) {
    vectors.push_back(request->vectors(i));
    vector_ids.push_back(request->vector_ids(i));
  }

  status = handle->VectorPushData(ctx, request->vector_index_id(), vectors, vector_ids, request->has_more(),
                                  request->error(), request->force_to_load_data_if_exist(), request->ts(),
                                  request->tso(), request->already_send_vector_count());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorPushData(google::protobuf::RpcController* controller,
                                        const ::dingodb::pb::diskann::VectorPushDataRequest* request,
                                        ::dingodb::pb::diskann::VectorPushDataResponse* response,
                                        ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorPushData(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetImportWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorBuild(const ::dingodb::pb::diskann::VectorBuildRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorBuild(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                          const ::dingodb::pb::diskann::VectorBuildRequest* request,
                          ::dingodb::pb::diskann::VectorBuildResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorBuild(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorBuild(ctx, request->vector_index_id(), request->force_to_build_if_exist());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorBuild(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::diskann::VectorBuildRequest* request,
                                     ::dingodb::pb::diskann::VectorBuildResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorBuild(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetBuildWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorLoad(const ::dingodb::pb::diskann::VectorLoadRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorLoad(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                         const ::dingodb::pb::diskann::VectorLoadRequest* request,
                         ::dingodb::pb::diskann::VectorLoadResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorLoad(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorLoad(ctx, request->vector_index_id(), request->load_param());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorLoad(google::protobuf::RpcController* controller,
                                    const ::dingodb::pb::diskann::VectorLoadRequest* request,
                                    ::dingodb::pb::diskann::VectorLoadResponse* response,
                                    ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorLoad(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetLoadWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorTryLoad(const ::dingodb::pb::diskann::VectorTryLoadRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorTryLoad(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                            const ::dingodb::pb::diskann::VectorTryLoadRequest* request,
                            ::dingodb::pb::diskann::VectorTryLoadResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorTryLoad(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorTryLoad(ctx, request->vector_index_id(), request->load_param());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorTryLoad(google::protobuf::RpcController* controller,
                                       const ::dingodb::pb::diskann::VectorTryLoadRequest* request,
                                       ::dingodb::pb::diskann::VectorTryLoadResponse* response,
                                       ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorTryLoad(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetLoadWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorSearch(const ::dingodb::pb::diskann::VectorSearchRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorSearch(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                           const ::dingodb::pb::diskann::VectorSearchRequest* request,
                           ::dingodb::pb::diskann::VectorSearchResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorSearch(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  auto vectors = std::vector<pb::common::Vector>();

  for (int i = 0; i < request->vectors_size(); i++) {
    vectors.push_back(request->vectors(i));
  }

  status = handle->VectorSearch(ctx, request->vector_index_id(), request->top_n(), request->search_param(), vectors);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::diskann::VectorSearchRequest* request,
                                      ::dingodb::pb::diskann::VectorSearchResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorSearch(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetSearchWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorReset(const ::dingodb::pb::diskann::VectorResetRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorReset(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                          const ::dingodb::pb::diskann::VectorResetRequest* request,
                          ::dingodb::pb::diskann::VectorResetResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorReset(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorReset(ctx, request->vector_index_id(), request->delete_data_file());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorReset(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::diskann::VectorResetRequest* request,
                                     ::dingodb::pb::diskann::VectorResetResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorReset(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorClose(const ::dingodb::pb::diskann::VectorCloseRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorClose(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                          const ::dingodb::pb::diskann::VectorCloseRequest* request,
                          ::dingodb::pb::diskann::VectorCloseResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorClose(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorClose(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorClose(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::diskann::VectorCloseRequest* request,
                                     ::dingodb::pb::diskann::VectorCloseResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorClose(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorDestroy(const ::dingodb::pb::diskann::VectorDestroyRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorDestroy(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                            const ::dingodb::pb::diskann::VectorDestroyRequest* request,
                            ::dingodb::pb::diskann::VectorDestroyResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorDestroy(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorDestroy(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorDestroy(google::protobuf::RpcController* controller,
                                       const ::dingodb::pb::diskann::VectorDestroyRequest* request,
                                       ::dingodb::pb::diskann::VectorDestroyResponse* response,
                                       ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorDestroy(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorStatus(const ::dingodb::pb::diskann::VectorStatusRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorStatus(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                           const ::dingodb::pb::diskann::VectorStatusRequest* request,
                           ::dingodb::pb::diskann::VectorStatusResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorStatus(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  auto vectors = std::vector<pb::common::Vector>();

  status = handle->VectorStatus(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorStatus(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::diskann::VectorStatusRequest* request,
                                      ::dingodb::pb::diskann::VectorStatusResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorStatus(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorCount(const ::dingodb::pb::diskann::VectorCountRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorCount(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                          const ::dingodb::pb::diskann::VectorCountRequest* request,
                          ::dingodb::pb::diskann::VectorCountResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorCount(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorCount(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorCount(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::diskann::VectorCountRequest* request,
                                     ::dingodb::pb::diskann::VectorCountResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorCount(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorSetNoData(const ::dingodb::pb::diskann::VectorSetNoDataRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorSetNoData(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                              const ::dingodb::pb::diskann::VectorSetNoDataRequest* request,
                              ::dingodb::pb::diskann::VectorSetNoDataResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorSetNoData(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorSetNoData(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorSetNoData(google::protobuf::RpcController* controller,
                                         const ::dingodb::pb::diskann::VectorSetNoDataRequest* request,
                                         ::dingodb::pb::diskann::VectorSetNoDataResponse* response,
                                         ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorSetNoData(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorSetImportTooMany(
    const ::dingodb::pb::diskann::VectorSetImportTooManyRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorSetImportTooMany(std::shared_ptr<DiskAnnServiceHandle> handle,
                                     google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::diskann::VectorSetImportTooManyRequest* request,
                                     ::dingodb::pb::diskann::VectorSetImportTooManyResponse* response,
                                     TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorSetImportTooMany(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorSetImportTooMany(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorSetImportTooMany(google::protobuf::RpcController* controller,
                                                const ::dingodb::pb::diskann::VectorSetImportTooManyRequest* request,
                                                ::dingodb::pb::diskann::VectorSetImportTooManyResponse* response,
                                                ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorSetImportTooMany(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorDump(const ::dingodb::pb::diskann::VectorDumpRequest* request) {
  if (request->vector_index_id() <= 0) {
    std::string s = fmt::format("Invalid vector index id : {}", request->vector_index_id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

static void DoVectorDump(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                         const ::dingodb::pb::diskann::VectorDumpRequest* request,
                         ::dingodb::pb::diskann::VectorDumpResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorDump(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorDump(ctx, request->vector_index_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DiskAnnServiceImpl::VectorDump(google::protobuf::RpcController* controller,
                                    const ::dingodb::pb::diskann::VectorDumpRequest* request,
                                    ::dingodb::pb::diskann::VectorDumpResponse* response,
                                    ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorDump(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorDumpAll(const ::dingodb::pb::diskann::VectorDumpAllRequest* /*request*/) {
  return butil::Status::OK();
}

static void DoVectorDumpAll(std::shared_ptr<DiskAnnServiceHandle> handle, google::protobuf::RpcController* controller,
                            const ::dingodb::pb::diskann::VectorDumpAllRequest* request,
                            ::dingodb::pb::diskann::VectorDumpAllResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorDumpAll(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, true ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  status = handle->VectorDumpAll(ctx);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}
void DiskAnnServiceImpl::VectorDumpAll(google::protobuf::RpcController* controller,
                                       const ::dingodb::pb::diskann::VectorDumpAllRequest* request,
                                       ::dingodb::pb::diskann::VectorDumpAllResponse* response,
                                       ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorDumpAll(handle_, controller, request, response, svr_done);
  });

  bool ret = DiskANNItemRuntime::GetMiscWorkerSet()->Execute(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

}  // namespace dingodb
