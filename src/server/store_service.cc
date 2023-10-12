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

#include "common/constant.h"
#include "common/context.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_bool(enable_async_store_kvscan, true, "enable async store kvscan");
DEFINE_bool(enable_async_store_operation, true, "enable async store operation");

static void StoreRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

StoreServiceImpl::StoreServiceImpl() = default;

butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvGetTask : public TaskRunnable {
 public:
  KvGetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl, const dingodb::pb::store::KvGetRequest* request,
            dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done, std::shared_ptr<Context> ctx)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done), ctx_(ctx) {}
  ~KvGetTask() override = default;

  std::string Type() override { return "KV_GET"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // do operations
    std::vector<std::string> keys;
    auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request_);
    keys.emplace_back(std::move(*mut_request->release_key()));

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->KvGet(ctx_, keys, kvs);
    if (!status.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvGet request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }
    if (!kvs.empty()) {
      response_->set_value(kvs[0].value());
    }

    DINGO_LOG(DEBUG) << fmt::format("KvGet request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::KvGetRequest* request_;
  dingodb::pb::store::KvGetResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
};

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvGetTask>(storage_, cntl, request, response, done_guard.release(), ctx);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvGetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvGetTask execute failed");
      return;
    }

    return;
  }

  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->KvGet(ctx, keys, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }
  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }

  DINGO_LOG(DEBUG) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(key);
  }

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvBatchGetTask : public TaskRunnable {
 public:
  KvBatchGetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                 const dingodb::pb::store::KvBatchGetRequest* request, dingodb::pb::store::KvBatchGetResponse* response,
                 google::protobuf::Closure* done, std::shared_ptr<Context> ctx)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done), ctx_(ctx) {}
  ~KvBatchGetTask() override = default;

  std::string Type() override { return "KV_BATCH_GET"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // do operations
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::KvBatchGetRequest* request_;
  dingodb::pb::store::KvBatchGetResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
};

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  if (request->keys().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvBatchGetTask>(storage_, cntl, request, response, done_guard.release(), ctx);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvBatchGetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvBatchGetTask execute failed");
      return;
    }

    return;
  }

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage_->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());

  DINGO_LOG(DEBUG) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  // std::vector<std::string_view> keys = {request->kv().key()};
  // auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  // if (!status.ok()) {
  //   return status;
  // }

  // status = ServiceHelper::ValidateClusterReadOnly();
  // if (!status.ok()) {
  //   return status;
  // }

  return butil::Status();
}

class KvPutTask : public TaskRunnable {
 public:
  KvPutTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl, const dingodb::pb::store::KvPutRequest* request,
            dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvPutTask() override = default;

  std::string Type() override { return "KV_PUT"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "KvPutTask execute start, request: " << request_->ShortDebugString();

    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request_);
    std::vector<pb::common::KeyValue> kvs;
    kvs.emplace_back(std::move(*mut_request->release_kv()));
    auto status = storage_->KvPut(ctx, kvs);
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
    }

    DINGO_LOG(DEBUG) << "KvPutTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "KvPutTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvPutRequest* request_;
  dingodb::pb::store::KvPutResponse* response_;
};

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvPut request: " << request->ShortDebugString();

  butil::Status status = ValidateKvPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    DINGO_LOG(ERROR) << "KvPutTask execute start, request: " << request->ShortDebugString();
    auto task = std::make_shared<KvPutTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvPutTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvPutTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPut(ctx, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvBatchPutTask : public TaskRunnable {
 public:
  KvBatchPutTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                 const dingodb::pb::store::KvBatchPutRequest* request, dingodb::pb::store::KvBatchPutResponse* response,
                 google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvBatchPutTask() override = default;

  std::string Type() override { return "KV_BATCH_PUT"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "KvBatchPutTask execute start, request: " << request_->ShortDebugString();

    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request_);
    auto status = storage_->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());

      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvBatchPut request: {} kv count {} response: {}",
                                      request_->context().ShortDebugString(), request_->kvs_size(),
                                      response_->ShortDebugString());
    }

    DINGO_LOG(DEBUG) << "KvBatchPutTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "KvBatchPutTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvBatchPutRequest* request_;
  dingodb::pb::store::KvBatchPutResponse* response_;
};

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchPut request: " << request->ShortDebugString();
  DINGO_LOG(INFO) << "KvBatchPut request_count: " << request->kvs_size();
  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvBatchPut request: {} kv count {} response: {}",
                                    request->context().ShortDebugString(), request->kvs_size(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    DINGO_LOG(ERROR) << "KvBatchPutTask execute start, request: " << request->ShortDebugString();
    auto task = std::make_shared<KvBatchPutTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvBatchPutTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvBatchPutTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  status = storage_->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());

    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchPut request: {} kv count {} response: {}",
                                    request->context().ShortDebugString(), request->kvs_size(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvPutIfAbsentTask : public TaskRunnable {
 public:
  KvPutIfAbsentTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                    const dingodb::pb::store::KvPutIfAbsentRequest* request,
                    dingodb::pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvPutIfAbsentTask() override = default;

  std::string Type() override { return "KV_PUT_IF_ABSENT"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request_);
    std::vector<pb::common::KeyValue> kvs;
    kvs.emplace_back(std::move(*mut_request->release_kv()));
    auto status = storage_->KvPutIfAbsent(ctx, kvs, true);
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvPutIfAbsentRequest* request_;
  dingodb::pb::store::KvPutIfAbsentResponse* response_;
};

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "KvPutIfAbsent request: " << request->ShortDebugString();

  butil::Status status = ValidateKvPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvPutIfAbsentTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvPutIfAbsentTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvPutIfAbsentTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPutIfAbsent(ctx, kvs, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

class KvBatchPutIfAbsentTask : public TaskRunnable {
 public:
  KvBatchPutIfAbsentTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                         const dingodb::pb::store::KvBatchPutIfAbsentRequest* request,
                         dingodb::pb::store::KvBatchPutIfAbsentResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvBatchPutIfAbsentTask() override = default;

  std::string Type() override { return "KV_BATCH_PUT_IF_ABSENT"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request_);
    auto status =
        storage_->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request_->is_atomic());
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvBatchPutIfAbsent request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvBatchPutIfAbsentRequest* request_;
  dingodb::pb::store::KvBatchPutIfAbsentResponse* response_;
};

butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchPutIfAbsent request: " << request->ShortDebugString();
  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvBatchPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvBatchPutIfAbsentTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvBatchPutIfAbsentTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvBatchPutIfAbsentTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage_->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(key);
  }

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvBatchDeleteTask : public TaskRunnable {
 public:
  KvBatchDeleteTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                    const dingodb::pb::store::KvBatchDeleteRequest* request,
                    dingodb::pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvBatchDeleteTask() override = default;

  std::string Type() override { return "KV_BATCH_DELETE"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request_);
    auto status = storage_->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvBatchDelete request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvBatchDeleteRequest* request_;
  dingodb::pb::store::KvBatchDeleteResponse* response_;
};

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchDelete request: " << request->ShortDebugString();
  if (request->keys().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvBatchDelete request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvBatchDeleteTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvBatchDeleteTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvBatchDeleteTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage_->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchDelete request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvDeleteRangeRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->RawRange(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvDeleteRangeTask : public TaskRunnable {
 public:
  KvDeleteRangeTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                    const dingodb::pb::store::KvDeleteRangeRequest* request,
                    dingodb::pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done,
                    pb::common::Range correction_range)
      : storage_(storage),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done),
        correction_range_(correction_range) {}
  ~KvDeleteRangeTask() override = default;

  std::string Type() override { return "KV_DELETE_RANGE"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto status = storage_->KvDeleteRange(ctx, correction_range_);
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvDeleteRangeRequest* request_;
  dingodb::pb::store::KvDeleteRangeResponse* response_;
  pb::common::Range correction_range_;
};

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvDeleteRange request: " << request->ShortDebugString();

  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
    err->set_errmsg(epoch_ret.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    return;
  }

  auto region =
      Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(request->context().region_id());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvDeleteRangeRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      DINGO_LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("KvDeleteRange range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }

  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  if (FLAGS_enable_async_store_operation) {
    auto task =
        std::make_shared<KvDeleteRangeTask>(storage_, cntl, request, response, done_guard.release(), correction_range);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvDeleteRangeTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvDeleteRangeTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  status = storage_->KvDeleteRange(ctx, correction_range);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvCompareAndSetRequest(const dingodb::pb::store::KvCompareAndSetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvCompareAndSetTask : public TaskRunnable {
 public:
  KvCompareAndSetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                      const dingodb::pb::store::KvCompareAndSetRequest* request,
                      dingodb::pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvCompareAndSetTask() override = default;

  std::string Type() override { return "KV_COMPARE_AND_SET"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto status = storage_->KvCompareAndSet(ctx, {request_->kv()}, {request_->expect_value()}, true);
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvCompareAndSetRequest* request_;
  dingodb::pb::store::KvCompareAndSetResponse* response_;
};

void StoreServiceImpl::KvCompareAndSet(google::protobuf::RpcController* controller,
                                       const pb::store::KvCompareAndSetRequest* request,
                                       pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << fmt::format("KvCompareAndSet request: {}", request->ShortDebugString());

  butil::Status status = ValidateKvCompareAndSetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvCompareAndSetTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvCompareAndSetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvCompareAndSetTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  status = storage_->KvCompareAndSet(ctx, {request->kv()}, {request->expect_value()}, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvBatchCompareAndSetRequest(const dingodb::pb::store::KvBatchCompareAndSetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvBatchCompareAndSetTask : public TaskRunnable {
 public:
  KvBatchCompareAndSetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                           const dingodb::pb::store::KvBatchCompareAndSetRequest* request,
                           dingodb::pb::store::KvBatchCompareAndSetResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~KvBatchCompareAndSetTask() override = default;

  std::string Type() override { return "KV_BATCH_COMPARE_AND_SET"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // we have validate vector search request in store_service, so we can skip validate here
    BthreadCond cond(1);
    auto* closure = brpc::NewCallback(StoreRpcDone, &cond);

    // do operation
    std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl_, closure, request_, response_);
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);

    auto* mut_request = const_cast<dingodb::pb::store::KvBatchCompareAndSetRequest*>(request_);
    auto status =
        storage_->KvCompareAndSet(ctx, Helper::PbRepeatedToVector(mut_request->kvs()),
                                  Helper::PbRepeatedToVector(mut_request->expect_values()), request_->is_atomic());
    if (!status.ok()) {
      // if RaftNode commit failed, we must call done->Run
      brpc::ClosureGuard done_guard(ctx->Done());

      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvBatchCompareAndSet request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
    }

    cond.Wait();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::KvBatchCompareAndSetRequest* request_;
  dingodb::pb::store::KvBatchCompareAndSetResponse* response_;
};

void StoreServiceImpl::KvBatchCompareAndSet(google::protobuf::RpcController* controller,
                                            const pb::store::KvBatchCompareAndSetRequest* request,
                                            pb::store::KvBatchCompareAndSetResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << fmt::format("KvBatchCompareAndSet request: {}", request->ShortDebugString());

  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchCompareAndSetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvBatchCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<KvBatchCompareAndSetTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvBatchCompareAndSetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvBatchCompareAndSetTask execute failed");
      return;
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchCompareAndSetRequest*>(request);

  status = storage_->KvCompareAndSet(ctx, Helper::PbRepeatedToVector(mut_request->kvs()),
                                     Helper::PbRepeatedToVector(mut_request->expect_values()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvScanBeginRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->RawRange(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class KvScanBeginTask : public TaskRunnable {
 public:
  KvScanBeginTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                  const dingodb::pb::store::KvScanBeginRequest* request,
                  dingodb::pb::store::KvScanBeginResponse* response, google::protobuf::Closure* done,
                  std::shared_ptr<Context> ctx, pb::common::Range correction_range)
      : storage_(storage),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done),
        ctx_(ctx),
        correction_range_(correction_range) {}
  ~KvScanBeginTask() override = default;

  std::string Type() override { return "KV_SCAN_BEGIN"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // do operations
    std::vector<pb::common::KeyValue> kvs;  // NOLINT
    std::string scan_id;                    // NOLINT

    auto status =
        storage_->KvScanBegin(ctx_, Constant::kStoreDataCF, request_->context().region_id(), correction_range_,
                              request_->max_fetch_cnt(), request_->key_only(), request_->disable_auto_release(),
                              request_->disable_coprocessor(), request_->coprocessor(), &scan_id, &kvs);
    if (!status.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvScanBegin request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    if (!kvs.empty()) {
      Helper::VectorToPbRepeated(kvs, response_->mutable_kvs());
    }

    *response_->mutable_scan_id() = scan_id;

    DINGO_LOG(DEBUG) << fmt::format("KvScanBegin request: {} response: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::KvScanBeginRequest* request_;
  dingodb::pb::store::KvScanBeginResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
  pb::common::Range correction_range_;
};

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
    err->set_errmsg(epoch_ret.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    return;
  }

  auto region =
      Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(request->context().region_id());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvScanBeginRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());

      DINGO_LOG(ERROR) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("KvScanBegin range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }
  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  if (FLAGS_enable_async_store_kvscan) {
    auto task = std::make_shared<KvScanBeginTask>(storage_, cntl, request, response, done_guard.release(), ctx,
                                                  correction_range);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvScanBeginTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvScanBeginTask execute failed");
      return;
    }

    return;
  }

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage_->KvScanBegin(ctx, Constant::kStoreDataCF, request->context().region_id(), correction_range,
                                 request->max_fetch_cnt(), request->key_only(), request->disable_auto_release(),
                                 request->disable_coprocessor(), request->coprocessor(), &scan_id, &kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;

  DINGO_LOG(DEBUG) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

class KvScanContinueTask : public TaskRunnable {
 public:
  KvScanContinueTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                     const dingodb::pb::store::KvScanContinueRequest* request,
                     dingodb::pb::store::KvScanContinueResponse* response, google::protobuf::Closure* done,
                     std::shared_ptr<Context> ctx)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done), ctx_(ctx) {}
  ~KvScanContinueTask() override = default;

  std::string Type() override { return "KV_SCAN_CONTINUE"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // do operations
    std::vector<pb::common::KeyValue> kvs;  // NOLINT
    auto status = storage_->KvScanContinue(ctx_, request_->scan_id(), request_->max_fetch_cnt(), &kvs);

    if (!status.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvScanContinue request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    if (!kvs.empty()) {
      Helper::VectorToPbRepeated(kvs, response_->mutable_kvs());
    }

    DINGO_LOG(DEBUG) << fmt::format("KvScanContinue request: {} response: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::KvScanContinueRequest* request_;
  dingodb::pb::store::KvScanContinueResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
};

void StoreServiceImpl::KvScanContinue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                                      ::dingodb::pb::store::KvScanContinueResponse* response,
                                      ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanContinueRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  if (FLAGS_enable_async_store_kvscan) {
    auto task = std::make_shared<KvScanContinueTask>(storage_, cntl, request, response, done_guard.release(), ctx);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvScanContinueTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvScanContinueTask execute failed");
      return;
    }

    return;
  }

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs);

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  DINGO_LOG(DEBUG) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

class KvScanReleaseTask : public TaskRunnable {
 public:
  KvScanReleaseTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                    const dingodb::pb::store::KvScanReleaseRequest* request,
                    dingodb::pb::store::KvScanReleaseResponse* response, google::protobuf::Closure* done,
                    std::shared_ptr<Context> ctx)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done), ctx_(ctx) {}
  ~KvScanReleaseTask() override = default;

  std::string Type() override { return "KV_SCAN_RELEASE"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    // check if region_epoch is match
    auto epoch_ret =
        ServiceHelper::ValidateRegionEpoch(request_->context().region_epoch(), request_->context().region_id());
    if (!epoch_ret.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(epoch_ret.error_code()));
      err->set_errmsg(epoch_ret.error_str());
      ServiceHelper::GetStoreRegionInfo(request_->context().region_id(), *(err->mutable_store_region_info()));
      DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request_->ShortDebugString());
      return;
    }

    // do operations
    std::vector<pb::common::KeyValue> kvs;  // NOLINT
    auto status = storage_->KvScanRelease(ctx_, request_->scan_id());

    if (!status.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                    Server::GetInstance()->ServerAddr(), request_->context().region_id(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("KvScanRelease request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    DINGO_LOG(DEBUG) << fmt::format("KvScanRelease request: {} response: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::KvScanReleaseRequest* request_;
  dingodb::pb::store::KvScanReleaseResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
};

void StoreServiceImpl::KvScanRelease(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                                     ::dingodb::pb::store::KvScanReleaseResponse* response,
                                     ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanReleaseRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    ServiceHelper::GetStoreRegionInfo(request->context().region_id(), *(err->mutable_store_region_info()));
    DINGO_LOG(ERROR) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  if (FLAGS_enable_async_store_kvscan) {
    auto task = std::make_shared<KvScanReleaseTask>(storage_, cntl, request, response, done_guard.release(), ctx);
    auto ret = storage_->Execute(request->context().region_id(), task);
    if (!ret) {
      DINGO_LOG(ERROR) << "KvScanReleaseTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("KvScanReleaseTask execute failed");
      return;
    }

    return;
  }

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanRelease(ctx, request->scan_id());

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

void StoreServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb
