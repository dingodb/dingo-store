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
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_bool(enable_async_store_kvscan, true, "enable async store kvscan");
DEFINE_bool(enable_async_store_operation, true, "enable async store operation");
DEFINE_uint32(max_scan_lock_limit, 5000, "Max scan lock limit");
DECLARE_uint32(max_prewrite_count);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    std::vector<pb::common::KeyValue> kvs;
    auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request_);
    auto status = storage_->KvGet(ctx_, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
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
      DINGO_LOG(ERROR) << fmt::format("KvBatchGet request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    Helper::VectorToPbRepeated(kvs, response_->mutable_kvs());

    DINGO_LOG(DEBUG) << fmt::format("KvBatchGet request: {} response: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto task = std::make_shared<KvPutTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto task = std::make_shared<KvBatchPutTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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
    ON_SCOPE_EXIT([this]() { storage_->DecTaskCount(); });
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
    auto ret = storage_->ExecuteRR(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

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

// txn

butil::Status ValidateTxnGetRequest(const dingodb::pb::store::TxnGetRequest* request) {
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

class TxnGetTask : public TaskRunnable {
 public:
  TxnGetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl, const dingodb::pb::store::TxnGetRequest* request,
             dingodb::pb::store::TxnGetResponse* response, google::protobuf::Closure* done,
             std::shared_ptr<Context> ctx)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done), ctx_(ctx) {}
  ~TxnGetTask() override = default;

  std::string Type() override { return "TXN_GET"; }

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
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<std::string> keys;
    auto* mut_request = const_cast<dingodb::pb::store::TxnGetRequest*>(request_);
    keys.emplace_back(std::move(*mut_request->release_key()));
    pb::store::TxnResultInfo txn_result_info;

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->TxnBatchGet(ctx, request_->start_ts(), keys, txn_result_info, kvs);
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
      DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    if (!kvs.empty()) {
      response_->set_value(kvs[0].value());
    }
    response_->mutable_txn_result()->CopyFrom(txn_result_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnGet request: {} response: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::TxnGetRequest* request_;
  dingodb::pb::store::TxnGetResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
};

void StoreServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnGetTask>(storage_, cntl, request, response, done_guard.release(),
                                             std::make_shared<Context>());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnGetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnGetTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));
  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
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
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnScanRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
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

class TxnScanTask : public TaskRunnable {
 public:
  TxnScanTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
              const dingodb::pb::store::TxnScanRequest* request, dingodb::pb::store::TxnScanResponse* response,
              google::protobuf::Closure* done, std::shared_ptr<Context> ctx, pb::common::Range correction_range)
      : storage_(storage),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done),
        ctx_(ctx),
        correction_range_(correction_range) {}
  ~TxnScanTask() override = default;

  std::string Type() override { return "TXN_SCAN"; }

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
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    std::vector<pb::common::KeyValue> kvs;
    bool has_more;
    std::string end_key;

    auto status = storage_->TxnScan(ctx, request_->start_ts(), correction_range_, request_->limit(),
                                    request_->key_only(), request_->is_reverse(), request_->disable_coprocessor(),
                                    request_->coprocessor(), txn_result_info, kvs, has_more, end_key);
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
      DINGO_LOG(ERROR) << fmt::format("TxnKvScan request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    if (!kvs.empty()) {
      Helper::VectorToPbRepeated(kvs, response_->mutable_kvs());
    }

    if (txn_result_info.ByteSizeLong() > 0) {
      response_->mutable_txn_result()->CopyFrom(txn_result_info);
    }
    response_->set_end_key(end_key);
    response_->set_has_more(has_more);

    DINGO_LOG(DEBUG) << fmt::format("TxnKvScan request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::TxnScanRequest* request_;
  dingodb::pb::store::TxnScanResponse* response_;
  google::protobuf::Closure* done_;
  std::shared_ptr<Context> ctx_;
  pb::common::Range correction_range_;
};

void StoreServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
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
  butil::Status status = ValidateTxnScanRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());

      DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("TxnKvScan range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }

  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnScanTask>(storage_, cntl, request, response, done_guard.release(),
                                              std::make_shared<Context>(), correction_range);
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnScanTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnScanTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more;
  std::string end_key;

  status = storage_->TxnScan(ctx, request->start_ts(), correction_range, request->limit(), request->key_only(),
                             request->is_reverse(), request->disable_coprocessor(), request->coprocessor(),
                             txn_result_info, kvs, has_more, end_key);
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
    DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
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

  DINGO_LOG(DEBUG) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnPrewriteRequest(const dingodb::pb::store::TxnPrewriteRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  auto status = ServiceHelper::ValidateClusterReadOnly();
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

class TxnPrewriteTask : public TaskRunnable {
 public:
  TxnPrewriteTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                  const dingodb::pb::store::TxnPrewriteRequest* request,
                  dingodb::pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnPrewriteTask() override = default;

  std::string Type() override { return "TXN_PREWRITE"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnPrewriteTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<pb::store::Mutation> mutations;
    for (const auto& mutation : request_->mutations()) {
      mutations.emplace_back(mutation);
    }

    pb::store::TxnResultInfo txn_result_info;
    std::vector<std::string> already_exists;
    int64_t one_pc_commit_ts = 0;

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->TxnPrewrite(ctx, mutations, request_->primary_lock(), request_->start_ts(),
                                        request_->lock_ttl(), request_->txn_size(), request_->try_one_pc(),
                                        request_->max_commit_ts(), txn_result_info, already_exists, one_pc_commit_ts);
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
      DINGO_LOG(ERROR) << fmt::format("TxnPrewrite request: {} response: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    // response_->mutable_txn_result()->CopyFrom(txn_result_info);
    // response_->set_one_pc_commit_ts(one_pc_commit_ts);
    // for (const auto& key : already_exists) {
    //   response_->add_keys_already_exist()->set_key(key);
    // }

    DINGO_LOG(DEBUG) << fmt::format("TxnPrewrite request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnPrewriteTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnPrewriteTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnPrewriteRequest* request_;
  dingodb::pb::store::TxnPrewriteResponse* response_;
};

void StoreServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::store::TxnPrewriteRequest* request,
                                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnPrewriteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnPrewriteTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnPrewriteTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnPrewriteTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  pb::store::TxnResultInfo txn_result_info;
  std::vector<std::string> already_exists;
  int64_t one_pc_commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnPrewrite(ctx, mutations, request->primary_lock(), request->start_ts(), request->lock_ttl(),
                                 request->txn_size(), request->try_one_pc(), request->max_commit_ts(), txn_result_info,
                                 already_exists, one_pc_commit_ts);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  // response->mutable_txn_result()->CopyFrom(txn_result_info);
  // response->set_one_pc_commit_ts(one_pc_commit_ts);
  // for (const auto& key : already_exists) {
  //   response->add_keys_already_exist()->set_key(key);
  // }

  DINGO_LOG(DEBUG) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnCommitRequest(const dingodb::pb::store::TxnCommitRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  auto status = ServiceHelper::ValidateClusterReadOnly();
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

class TxnCommitTask : public TaskRunnable {
 public:
  TxnCommitTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                const dingodb::pb::store::TxnCommitRequest* request, dingodb::pb::store::TxnCommitResponse* response,
                google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnCommitTask() override = default;

  std::string Type() override { return "TXN_COMMIT"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnCommitTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<std::string> keys;
    for (const auto& key : request_->keys()) {
      keys.emplace_back(key);
    }

    pb::store::TxnResultInfo txn_result_info;
    int64_t commit_ts = 0;

    std::vector<pb::common::KeyValue> kvs;
    auto status =
        storage_->TxnCommit(ctx, request_->start_ts(), request_->commit_ts(), keys, txn_result_info, commit_ts);
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
      DINGO_LOG(ERROR) << fmt::format("TxnCommit request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    response_->mutable_txn_result()->CopyFrom(txn_result_info);
    response_->set_commit_ts(commit_ts);

    DINGO_LOG(DEBUG) << fmt::format("TxnCommit request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnCommitTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnCommitTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnCommitRequest* request_;
  dingodb::pb::store::TxnCommitResponse* response_;
};

void StoreServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnCommitRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnCommitTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnCommitTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnCommitTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;
  int64_t commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnCommit(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info, commit_ts);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_commit_ts(commit_ts);

  DINGO_LOG(DEBUG) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnCheckTxnStatusRequest(const dingodb::pb::store::TxnCheckTxnStatusRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnCheckTxnStatusTask : public TaskRunnable {
 public:
  TxnCheckTxnStatusTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                        const dingodb::pb::store::TxnCheckTxnStatusRequest* request,
                        dingodb::pb::store::TxnCheckTxnStatusResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnCheckTxnStatusTask() override = default;

  std::string Type() override { return "TXN_CHECK"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnCheckTxnStatusTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    int64_t lock_ttl = 0;
    int64_t commit_ts = 0;
    pb::store::Action action;
    pb::store::LockInfo lock_info;

    std::vector<pb::common::KeyValue> kvs;
    auto status =
        storage_->TxnCheckTxnStatus(ctx, request_->primary_key(), request_->lock_ts(), request_->caller_start_ts(),
                                    request_->current_ts(), txn_result_info, lock_ttl, commit_ts, action, lock_info);
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
      DINGO_LOG(ERROR) << fmt::format("TxnCheckTxnStatus request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    // response_->mutable_txn_result()->CopyFrom(txn_result_info);
    // response_->set_lock_ttl(lock_ttl);
    // response_->set_commit_ts(commit_ts);
    // response_->set_action(action);
    // response_->mutable_lock_info()->CopyFrom(lock_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnCheckTxnStatus request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnCheckTxnStatusTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnCheckTxnStatusTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnCheckTxnStatusRequest* request_;
  dingodb::pb::store::TxnCheckTxnStatusResponse* response_;
};

void StoreServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::store::TxnCheckTxnStatusRequest* request,
                                         pb::store::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnCheckTxnStatusRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnCheckTxnStatusTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnCheckTxnStatusTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnCheckTxnStatusTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;
  int64_t commit_ts = 0;
  pb::store::Action action;
  pb::store::LockInfo lock_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnCheckTxnStatus(ctx, request->primary_key(), request->lock_ts(), request->caller_start_ts(),
                                       request->current_ts(), txn_result_info, lock_ttl, commit_ts, action, lock_info);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  // response->mutable_txn_result()->CopyFrom(txn_result_info);
  // response->set_lock_ttl(lock_ttl);
  // response->set_commit_ts(commit_ts);
  // response->set_action(action);
  // response->mutable_lock_info()->CopyFrom(lock_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnResolveLockRequest(const dingodb::pb::store::TxnResolveLockRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

class TxnResolveLockTask : public TaskRunnable {
 public:
  TxnResolveLockTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                     const dingodb::pb::store::TxnResolveLockRequest* request,
                     dingodb::pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnResolveLockTask() override = default;

  std::string Type() override { return "TXN_RESOLVE"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnResolveLockTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<std::string> keys;
    for (const auto& key : request_->keys()) {
      keys.emplace_back(key);
    }

    pb::store::TxnResultInfo txn_result_info;

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->TxnResolveLock(ctx, request_->start_ts(), request_->commit_ts(), keys, txn_result_info);
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
      DINGO_LOG(ERROR) << fmt::format("TxnResolveLock request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    response_->mutable_txn_result()->CopyFrom(txn_result_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnResolveLock request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnResolveLockTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnResolveLockTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnResolveLockRequest* request_;
  dingodb::pb::store::TxnResolveLockResponse* response_;
};

void StoreServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnResolveLockRequest* request,
                                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnResolveLockRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnResolveLockTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnResolveLockTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnResolveLockTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnResolveLock(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnBatchGetRequest(const dingodb::pb::store::TxnBatchGetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnBatchGetTask : public TaskRunnable {
 public:
  TxnBatchGetTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                  const dingodb::pb::store::TxnBatchGetRequest* request,
                  dingodb::pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnBatchGetTask() override = default;

  std::string Type() override { return "TXN_BATCH_GET"; }

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
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<std::string> keys;
    for (const auto& key : request_->keys()) {
      keys.emplace_back(key);
    }

    pb::store::TxnResultInfo txn_result_info;

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->TxnBatchGet(ctx, request_->start_ts(), keys, txn_result_info, kvs);
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
      DINGO_LOG(ERROR) << fmt::format("TxnBatchGet request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    if (!kvs.empty()) {
      for (const auto& kv : kvs) {
        response_->add_kvs()->CopyFrom(kv);
      }
    }
    response_->mutable_txn_result()->CopyFrom(txn_result_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnBatchGet request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::TxnBatchGetRequest* request_;
  dingodb::pb::store::TxnBatchGetResponse* response_;
  google::protobuf::Closure* done_;
};

void StoreServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::store::TxnBatchGetRequest* request,
                                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnBatchGetTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnBatchGetTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnBatchGetTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
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
    DINGO_LOG(ERROR) << fmt::format("TxnBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      response->add_kvs()->CopyFrom(kv);
    }
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnBatchGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnBatchRollbackRequest(const dingodb::pb::store::TxnBatchRollbackRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnBatchRollbackTask : public TaskRunnable {
 public:
  TxnBatchRollbackTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                       const dingodb::pb::store::TxnBatchRollbackRequest* request,
                       dingodb::pb::store::TxnBatchRollbackResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnBatchRollbackTask() override = default;

  std::string Type() override { return "TXN_ROLLBACK"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnBatchRollbackTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    std::vector<std::string> keys;
    for (const auto& key : request_->keys()) {
      keys.emplace_back(key);
    }

    pb::store::TxnResultInfo txn_result_info;

    std::vector<pb::common::KeyValue> kvs;
    auto status = storage_->TxnBatchRollback(ctx, request_->start_ts(), keys, txn_result_info, kvs);
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
      DINGO_LOG(ERROR) << fmt::format("TxnBatchRollback request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    response_->mutable_txn_result()->CopyFrom(txn_result_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnBatchRollback request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnBatchRollbackTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnBatchRollbackTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnBatchRollbackRequest* request_;
  dingodb::pb::store::TxnBatchRollbackResponse* response_;
};

void StoreServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                        const pb::store::TxnBatchRollbackRequest* request,
                                        pb::store::TxnBatchRollbackResponse* response,
                                        google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnBatchRollbackRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnBatchRollbackTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnBatchRollbackTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnBatchRollbackTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchRollback(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnBatchRollback request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnBatchRollback request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnScanLockRequest(const dingodb::pb::store::TxnScanLockRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnScanLockTask : public TaskRunnable {
 public:
  TxnScanLockTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                  const dingodb::pb::store::TxnScanLockRequest* request,
                  dingodb::pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnScanLockTask() override = default;

  std::string Type() override { return "TXN_SCAN_LOCK"; }

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
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    std::vector<pb::store::LockInfo> locks;

    auto status = storage_->TxnScanLock(ctx, request_->max_ts(), request_->start_key(), request_->limit(),
                                        request_->end_key(), txn_result_info, locks);
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
      DINGO_LOG(ERROR) << fmt::format("TxnScanLock request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    response_->mutable_txn_result()->CopyFrom(txn_result_info);
    for (const auto& lock : locks) {
      response_->add_locks()->CopyFrom(lock);
    }

    DINGO_LOG(DEBUG) << fmt::format("TxnScanLock request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::TxnScanLockRequest* request_;
  dingodb::pb::store::TxnScanLockResponse* response_;
  google::protobuf::Closure* done_;
};

void StoreServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                   const pb::store::TxnScanLockRequest* request,
                                   pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnScanLockRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnScanLockTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnScanLockTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnScanLockTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::LockInfo> locks;

  status = storage_->TxnScanLock(ctx, request->max_ts(), request->start_key(), request->limit(), request->end_key(),
                                 txn_result_info, locks);
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
    DINGO_LOG(ERROR) << fmt::format("TxnScanLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  for (const auto& lock : locks) {
    response->add_locks()->CopyFrom(lock);
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnScanLock request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnHeartBeatRequest(const dingodb::pb::store::TxnHeartBeatRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  auto status = ServiceHelper::ValidateClusterReadOnly();
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

class TxnHeartBeatTask : public TaskRunnable {
 public:
  TxnHeartBeatTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                   const dingodb::pb::store::TxnHeartBeatRequest* request,
                   dingodb::pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnHeartBeatTask() override = default;

  std::string Type() override { return "TXN_HB"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnHeartBeatTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    int64_t lock_ttl = 0;

    auto status = storage_->TxnHeartBeat(ctx, request_->primary_lock(), request_->start_ts(),
                                         request_->advise_lock_ttl(), txn_result_info, lock_ttl);
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
      DINGO_LOG(ERROR) << fmt::format("TxnHeartBeat request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    response_->mutable_txn_result()->CopyFrom(txn_result_info);
    response_->set_lock_ttl(lock_ttl);

    DINGO_LOG(DEBUG) << fmt::format("TxnHeartBeat request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnHeartBeatTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnHeartBeatTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnHeartBeatRequest* request_;
  dingodb::pb::store::TxnHeartBeatResponse* response_;
};

void StoreServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                    const pb::store::TxnHeartBeatRequest* request,
                                    pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnHeartBeatRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnHeartBeatTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnHeartBeatTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnHeartBeatTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage_->TxnHeartBeat(ctx, request->primary_lock(), request->start_ts(), request->advise_lock_ttl(),
                                  txn_result_info, lock_ttl);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnHeartBeat request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_lock_ttl(lock_ttl);

  DINGO_LOG(DEBUG) << fmt::format("TxnHeartBeat request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnGcRequest(const dingodb::pb::store::TxnGcRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnGcTask : public TaskRunnable {
 public:
  TxnGcTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl, const dingodb::pb::store::TxnGcRequest* request,
            dingodb::pb::store::TxnGcResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnGcTask() override = default;

  std::string Type() override { return "TXN_GC"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnGcTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    int64_t lock_ttl = 0;

    auto status = storage_->TxnGc(ctx, request_->safe_point_ts(), txn_result_info);
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
      DINGO_LOG(ERROR) << fmt::format("TxnGc request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    // response_->mutable_txn_result()->CopyFrom(txn_result_info);

    DINGO_LOG(DEBUG) << fmt::format("TxnGc request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnGcTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnGcTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnGcRequest* request_;
  dingodb::pb::store::TxnGcResponse* response_;
};

void StoreServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnGcRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnGcTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnGcTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnGcTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage_->TxnGc(ctx, request->safe_point_ts(), txn_result_info);
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnGc request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  // response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnGc request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnDeleteRangeRequest(const dingodb::pb::store::TxnDeleteRangeRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

class TxnDeleteRangeTask : public TaskRunnable {
 public:
  TxnDeleteRangeTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                     const dingodb::pb::store::TxnDeleteRangeRequest* request,
                     dingodb::pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnDeleteRangeTask() override = default;

  std::string Type() override { return "TXN_DELETE_RANGE"; }

  void Run() override {
    DINGO_LOG(DEBUG) << "TxnDeleteRangeTask execute start, request: " << request_->ShortDebugString();

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
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    auto status = storage_->TxnDeleteRange(ctx, request_->start_key(), request_->end_key());
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
      DINGO_LOG(ERROR) << fmt::format("TxnDeleteRange request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    DINGO_LOG(DEBUG) << fmt::format("TxnDeleteRange request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());

    DINGO_LOG(DEBUG) << "TxnDeleteRangeTask execute end, request: " << request_->ShortDebugString();

    cond.Wait();

    DINGO_LOG(DEBUG) << "TxnDeleteRangeTask execute end cond wait, request: " << request_->ShortDebugString();
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::store::TxnDeleteRangeRequest* request_;
  dingodb::pb::store::TxnDeleteRangeResponse* response_;
};

void StoreServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                      const pb::store::TxnDeleteRangeRequest* request,
                                      pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnDeleteRangeRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnDeleteRangeTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnDeleteRangeTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnDeleteRangeTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  status = storage_->TxnDeleteRange(ctx, request->start_key(), request->end_key());
  if (!status.ok()) {
    // if RaftNode commit failed, we must call done->Run
    brpc::ClosureGuard done_guard(ctx->Done());

    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnDeleteRange request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnDeleteRange request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnDumpRequest(const dingodb::pb::store::TxnDumpRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

class TxnDumpTask : public TaskRunnable {
 public:
  TxnDumpTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
              const dingodb::pb::store::TxnDumpRequest* request, dingodb::pb::store::TxnDumpResponse* response,
              google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~TxnDumpTask() override = default;

  std::string Type() override { return "TXN_DUMP"; }

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
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    ctx->SetRegionId(request_->context().region_id()).SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(request_->context().region_epoch());
    ctx->SetIsolationLevel(request_->context().isolation_level());

    pb::store::TxnResultInfo txn_result_info;
    std::vector<pb::store::TxnWriteKey> txn_write_keys;
    std::vector<pb::store::TxnWriteValue> txn_write_values;
    std::vector<pb::store::TxnLockKey> txn_lock_keys;
    std::vector<pb::store::TxnLockValue> txn_lock_values;
    std::vector<pb::store::TxnDataKey> txn_data_keys;
    std::vector<pb::store::TxnDataValue> txn_data_values;

    auto status = storage_->TxnDump(ctx, request_->start_key(), request_->end_key(), request_->start_ts(),
                                    request_->end_ts(), txn_result_info, txn_write_keys, txn_write_values,
                                    txn_lock_keys, txn_lock_values, txn_data_keys, txn_data_values);
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
      DINGO_LOG(ERROR) << fmt::format("TxnDump request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    DINGO_LOG(DEBUG) << fmt::format("TxnDump request_: {} response_: {}", request_->ShortDebugString(),
                                    response_->ShortDebugString());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  const dingodb::pb::store::TxnDumpRequest* request_;
  dingodb::pb::store::TxnDumpResponse* response_;
  google::protobuf::Closure* done_;
};

void StoreServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
                               pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnDumpRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_store_operation) {
    auto task = std::make_shared<TxnDumpTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->ExecuteHash(request->context().region_id(), task);
    if (!ret) {
      // if Execute is failed, we must call done->Run
      brpc::ClosureGuard done_guard(done);

      DINGO_LOG(ERROR) << "TxnDumpTask execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("TxnDumpTask execute failed");
      return;
    }

    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::TxnWriteKey> txn_write_keys;
  std::vector<pb::store::TxnWriteValue> txn_write_values;
  std::vector<pb::store::TxnLockKey> txn_lock_keys;
  std::vector<pb::store::TxnLockValue> txn_lock_values;
  std::vector<pb::store::TxnDataKey> txn_data_keys;
  std::vector<pb::store::TxnDataValue> txn_data_values;

  status = storage_->TxnDump(ctx, request->start_key(), request->end_key(), request->start_ts(), request->end_ts(),
                             txn_result_info, txn_write_keys, txn_write_values, txn_lock_keys, txn_lock_values,
                             txn_data_keys, txn_data_values);
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
    DINGO_LOG(ERROR) << fmt::format("TxnDump request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnDump request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

}  // namespace dingodb
