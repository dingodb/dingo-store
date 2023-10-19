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

#include "server/store_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

namespace dingodb {

DEFINE_bool(enable_async_store_kvscan, true, "enable async store kvscan");
DEFINE_bool(enable_async_store_operation, true, "enable async store operation");
DEFINE_uint32(max_scan_lock_limit, 5000, "Max scan lock limit");
DECLARE_uint32(max_prewrite_count);

static void StoreRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

StoreServiceImpl::StoreServiceImpl() = default;

static butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvGet(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::KvGetRequest* request, dingodb::pb::store::KvGetResponse* response,
             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);

  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage->KvGet(ctx, keys, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvGet", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoKvGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(key);
  }

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchGet(StoragePtr storage, google::protobuf::RpcController* controller,
                  const dingodb::pb::store::KvBatchGetRequest* request,
                  dingodb::pb::store::KvBatchGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvBatchGet", done, request, response);

  if (request->keys().empty()) {
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoKvBatchGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvPut(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::KvPutRequest* request, dingodb::pb::store::KvPutResponse* response,
             google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateKvPutRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage->KvPut(ctx, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvPut", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvPut(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoKvPut(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchPut(StoragePtr storage, google::protobuf::RpcController* controller,
                  const dingodb::pb::store::KvBatchPutRequest* request,
                  dingodb::pb::store::KvBatchPutResponse* response, google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchPutRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  status = storage->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvBatchPut", done, request, response);

  if (request->kvs().empty()) {
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchPut(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvBatchPut(storage, controller, request, response, svr_done, true); });
  auto ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvPutIfAbsent(StoragePtr storage, google::protobuf::RpcController* controller,
                     const pb::store::KvPutIfAbsentRequest* request, pb::store::KvPutIfAbsentResponse* response,
                     google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvPutIfAbsentRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage->KvPutIfAbsent(ctx, kvs, true);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvPutIfAbsent", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvPutIfAbsent(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvPutIfAbsent(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchPutIfAbsent(StoragePtr storage, google::protobuf::RpcController* controller,
                          const dingodb::pb::store::KvBatchPutIfAbsentRequest* request,
                          dingodb::pb::store::KvBatchPutIfAbsentResponse* response, google::protobuf::Closure* done,
                          bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvBatchPutIfAbsent", done, request, response);

  if (request->kvs().empty()) {
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchPutIfAbsent(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoKvBatchPutIfAbsent(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(key);
  }

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchDelete(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvBatchDeleteRequest* request,
                     dingodb::pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done,
                     bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateKvBatchDeleteRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvBatchDelete", done, request, response);

  if (request->keys().empty()) {
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchDelete(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvBatchDelete(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvDeleteRangeRequest(const pb::store::KvDeleteRangeRequest* request,
                                                  store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvDeleteRangeRequest* request,
                     dingodb::pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done,
                     bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvDeleteRangeRequest(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    }
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto correction_range = Helper::IntersectRange(region->Range(), uniform_range);
  status = storage->KvDeleteRange(ctx, correction_range);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvDeleteRange", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvDeleteRange(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvDeleteRange(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvCompareAndSetRequest(const dingodb::pb::store::KvCompareAndSetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvCompareAndSet(StoragePtr storage, google::protobuf::RpcController* controller,
                       const dingodb::pb::store::KvCompareAndSetRequest* request,
                       dingodb::pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done,
                       bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateKvCompareAndSetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  status = storage->KvCompareAndSet(ctx, {request->kv()}, {request->expect_value()}, true);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvCompareAndSet(google::protobuf::RpcController* controller,
                                       const pb::store::KvCompareAndSetRequest* request,
                                       pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvCompareAndSet", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvCompareAndSet(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoKvCompareAndSet(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvBatchCompareAndSetRequest(
    const dingodb::pb::store::KvBatchCompareAndSetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }
  }

  if (request->expect_values().size() != request->kvs().size()) {
    return butil::Status(pb::error::EKEY_EMPTY, "expect_values size !=  kvs size");
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    keys.push_back(kv.key());
  }
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchCompareAndSet(StoragePtr storage, google::protobuf::RpcController* controller,
                            const dingodb::pb::store::KvBatchCompareAndSetRequest* request,
                            dingodb::pb::store::KvBatchCompareAndSetResponse* response, google::protobuf::Closure* done,
                            bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateKvBatchCompareAndSetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchCompareAndSetRequest*>(request);

  status = storage->KvCompareAndSet(ctx, Helper::PbRepeatedToVector(mut_request->kvs()),
                                    Helper::PbRepeatedToVector(mut_request->expect_values()), request->is_atomic());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvBatchCompareAndSet(google::protobuf::RpcController* controller,
                                            const pb::store::KvBatchCompareAndSetRequest* request,
                                            pb::store::KvBatchCompareAndSetResponse* response,
                                            google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvBatchCompareAndSet", done, request, response);

  if (request->kvs().empty()) {
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchCompareAndSet(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoKvBatchCompareAndSet(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvScanBeginRequest(const dingodb::pb::store::KvScanBeginRequest* request,
                                                store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvScanBegin(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::KvScanBeginRequest* request,
                   dingodb::pb::store::KvScanBeginResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvScanBeginRequest(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);

  auto correction_range = Helper::IntersectRange(region->Range(), uniform_range);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage->KvScanBegin(ctx, Constant::kStoreDataCF, region_id, correction_range, request->max_fetch_cnt(),
                                request->key_only(), request->disable_auto_release(), request->disable_coprocessor(),
                                request->coprocessor(), &scan_id, &kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;
}

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvScanBegin", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanBegin(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoKvScanBegin(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(
          request->context().region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region %lu at server %lu",
                         request->context().region_id(), Server::GetInstance()->Id());
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

void DoKvScanContinue(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::KvScanContinueRequest* request,
                      dingodb::pb::store::KvScanContinueResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanContinueRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs);

  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }
}

void StoreServiceImpl::KvScanContinue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                                      ::dingodb::pb::store::KvScanContinueResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvScanContinue", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanContinue(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvScanContinue(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(
          request->context().region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region %lu at server %lu",
                         request->context().region_id(), Server::GetInstance()->Id());
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  return butil::Status();
}

void DoKvScanRelease(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvScanReleaseRequest* request,
                     dingodb::pb::store::KvScanReleaseResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanReleaseRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);

  status = storage->KvScanRelease(ctx, request->scan_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
  }
}

void StoreServiceImpl::KvScanRelease(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                                     ::dingodb::pb::store::KvScanReleaseResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("KvScanRelease", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanRelease(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoKvScanRelease(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

// txn

static butil::Status ValidateTxnGetRequest(const dingodb::pb::store::TxnGetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGet(StoragePtr storage, google::protobuf::RpcController* controller,
              const dingodb::pb::store::TxnGetRequest* request, dingodb::pb::store::TxnGetResponse* response,
              google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));
  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void StoreServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnGet", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnScanRequest(const pb::store::TxnScanRequest* request, store::RegionPtr region,
                                            const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScan(StoragePtr storage, google::protobuf::RpcController* controller,
               const dingodb::pb::store::TxnScanRequest* request, dingodb::pb::store::TxnScanResponse* response,
               google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequest(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more;
  std::string end_key;

  auto correction_range = Helper::IntersectRange(region->Range(), uniform_range);
  status = storage->TxnScan(ctx, request->start_ts(), correction_range, request->limit(), request->key_only(),
                            request->is_reverse(), request->disable_coprocessor(), request->coprocessor(),
                            txn_result_info, kvs, has_more, end_key);

  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    response->mutable_txn_result()->CopyFrom(txn_result_info);
  }
  response->set_end_key(end_key);
  response->set_has_more(has_more);
}

void StoreServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnScan", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnScan(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnScan(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnPrewriteRequest(const dingodb::pb::store::TxnPrewriteRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations size is too large, max=1024");
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->txn_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "txn_size is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnPrewrite(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnPrewriteRequest* request,
                   dingodb::pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnPrewriteRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  pb::store::TxnResultInfo txn_result_info;
  std::vector<std::string> already_exists;
  int64_t one_pc_commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnPrewrite(ctx, mutations, request->primary_lock(), request->start_ts(), request->lock_ttl(),
                                request->txn_size(), request->try_one_pc(), request->max_commit_ts(), txn_result_info,
                                already_exists, one_pc_commit_ts);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::store::TxnPrewriteRequest* request,
                                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnPrewrite", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnPrewrite(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnPrewrite(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnCommitRequest(const dingodb::pb::store::TxnCommitRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCommit(StoragePtr storage, google::protobuf::RpcController* controller,
                 const dingodb::pb::store::TxnCommitRequest* request, dingodb::pb::store::TxnCommitResponse* response,
                 google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnCommitRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;
  int64_t commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnCommit(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info, commit_ts);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_commit_ts(commit_ts);
}

void StoreServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnCommit", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnCommit(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnCommit(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnCheckTxnStatusRequest(const dingodb::pb::store::TxnCheckTxnStatusRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->primary_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_key is empty");
  }

  if (request->lock_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ts is 0");
  }

  if (request->caller_start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "caller_start_ts is 0");
  }

  if (request->current_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "current_ts is 0");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_key());
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCheckTxnStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                         const dingodb::pb::store::TxnCheckTxnStatusRequest* request,
                         dingodb::pb::store::TxnCheckTxnStatusResponse* response, google::protobuf::Closure* done,
                         bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnCheckTxnStatusRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;
  int64_t commit_ts = 0;
  pb::store::Action action;
  pb::store::LockInfo lock_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnCheckTxnStatus(ctx, request->primary_key(), request->lock_ts(), request->caller_start_ts(),
                                      request->current_ts(), txn_result_info, lock_ttl, commit_ts, action, lock_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::store::TxnCheckTxnStatusRequest* request,
                                         pb::store::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnCheckTxnStatus", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnCheckTxnStatus(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnCheckTxnStatus(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnResolveLockRequest(const dingodb::pb::store::TxnResolveLockRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys_size() > 0) {
    for (const auto& key : request->keys()) {
      if (key.empty()) {
        return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
      }
      std::vector<std::string_view> keys;
      keys.push_back(key);
      auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
      if (!status.ok()) {
        return status;
      }
    }
  }

  return butil::Status();
}

void DoTxnResolveLock(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::TxnResolveLockRequest* request,
                      dingodb::pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done,
                      bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnResolveLockRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnResolveLock(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void StoreServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnResolveLockRequest* request,
                                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnResolveLock", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnResolveLock(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnResolveLock(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnBatchGetRequest(const dingodb::pb::store::TxnBatchGetRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchGet(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnBatchGetRequest* request,
                   dingodb::pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnBatchGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      response->add_kvs()->CopyFrom(kv);
    }
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void StoreServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::store::TxnBatchGetRequest* request,
                                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnBatchGet", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnBatchGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnBatchGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnBatchRollbackRequest(const dingodb::pb::store::TxnBatchRollbackRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                        const dingodb::pb::store::TxnBatchRollbackRequest* request,
                        dingodb::pb::store::TxnBatchRollbackResponse* response, google::protobuf::Closure* done,
                        bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnBatchRollbackRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchRollback(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void StoreServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                        const pb::store::TxnBatchRollbackRequest* request,
                                        pb::store::TxnBatchRollbackResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnBatchRollback", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnBatchRollback(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnBatchRollback(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnScanLockRequest(const dingodb::pb::store::TxnScanLockRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->max_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_ts is 0");
  }

  if (request->limit() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is 0");
  }

  if (request->limit() > FLAGS_max_scan_lock_limit) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is too large, max=1024");
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->start_key());

  std::string str(1, '\x01');
  std::string end_key = Helper::StringSubtractRightAlign(request->end_key(), str);

  keys.push_back(end_key);

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScanLock(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnScanLockRequest* request,
                   dingodb::pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnScanLockRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::LockInfo> locks;

  status = storage->TxnScanLock(ctx, request->max_ts(), request->start_key(), request->limit(), request->end_key(),
                                txn_result_info, locks);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  for (const auto& lock : locks) {
    response->add_locks()->CopyFrom(lock);
  }
}

void StoreServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                   const pb::store::TxnScanLockRequest* request,
                                   pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnScanLock", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnScanLock(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnScanLock(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnHeartBeatRequest(const dingodb::pb::store::TxnHeartBeatRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->advise_lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "advise_lock_ttl is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_lock());

  status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnHeartBeat(StoragePtr storage, google::protobuf::RpcController* controller,
                    const dingodb::pb::store::TxnHeartBeatRequest* request,
                    dingodb::pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnHeartBeatRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage->TxnHeartBeat(ctx, request->primary_lock(), request->start_ts(), request->advise_lock_ttl(),
                                 txn_result_info, lock_ttl);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_lock_ttl(lock_ttl);
}

void StoreServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                    const pb::store::TxnHeartBeatRequest* request,
                                    pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnHeartBeat", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnHeartBeat(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnHeartBeat(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnGcRequest(const dingodb::pb::store::TxnGcRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGc(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::TxnGcRequest* request, dingodb::pb::store::TxnGcResponse* response,
             google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnGcRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage->TxnGc(ctx, request->safe_point_ts(), txn_result_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnGc", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnGc(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnGc(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnDeleteRangeRequest(const dingodb::pb::store::TxnDeleteRangeRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() == request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is equal to end_key");
  }

  if (request->start_key().compare(request->end_key()) > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is greater than end_key");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::TxnDeleteRangeRequest* request,
                      dingodb::pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done,
                      bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnDeleteRangeRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  status = storage->TxnDeleteRange(ctx, request->start_key(), request->end_key());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                      const pb::store::TxnDeleteRangeRequest* request,
                                      pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnDeleteRange", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnDeleteRange(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnDeleteRange(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnDumpRequest(const dingodb::pb::store::TxnDumpRequest* request) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return status;
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() == request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is equal to end_key");
  }

  if (request->start_key().compare(request->end_key()) > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is greater than end_key");
  }

  if (request->end_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_ts is 0");
  }

  return butil::Status();
}

void DoTxnDump(StoragePtr storage, google::protobuf::RpcController* controller,
               const dingodb::pb::store::TxnDumpRequest* request, dingodb::pb::store::TxnDumpResponse* response,
               google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnDumpRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::TxnWriteKey> txn_write_keys;
  std::vector<pb::store::TxnWriteValue> txn_write_values;
  std::vector<pb::store::TxnLockKey> txn_lock_keys;
  std::vector<pb::store::TxnLockValue> txn_lock_values;
  std::vector<pb::store::TxnDataKey> txn_data_keys;
  std::vector<pb::store::TxnDataValue> txn_data_values;

  status = storage->TxnDump(ctx, request->start_key(), request->end_key(), request->start_ts(), request->end_ts(),
                            txn_result_info, txn_write_keys, txn_write_values, txn_lock_keys, txn_lock_values,
                            txn_data_keys, txn_data_values);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
  }
}

void StoreServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
                               pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnDump", done, request, response);

  if (!FLAGS_enable_async_store_operation) {
    return DoTxnDump(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnDump(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

}  // namespace dingodb
