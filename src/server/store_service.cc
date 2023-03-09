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

#include "common/context.h"
#include "common/helper.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

StoreServiceImpl::StoreServiceImpl() = default;

void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::store::AddRegionRequest* request,
                                 dingodb::pb::store::AddRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "AddRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto errcode = store_control->AddRegion(ctx, std::make_shared<pb::common::Region>(request->region()));
  if (errcode != pb::error::OK) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(errcode);
    mut_err->set_errmsg("Add region failed!");
  }
}

void StoreServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                    const pb::store::ChangeRegionRequest* request,
                                    pb::store::ChangeRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "ChangeRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();
  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto errcode = store_control->ChangeRegion(ctx, std::make_shared<pb::common::Region>(request->region()));
  if (errcode != pb::error::OK) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(errcode);
    mut_err->set_errmsg("Change region failed!");
  }
}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::DestroyRegionRequest* request,
                                     dingodb::pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "DestroyRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();
  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto errcode = store_control->DeleteRegion(ctx, request->region_id());
  if (errcode != pb::error::OK) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(errcode);
    mut_err->set_errmsg("Destroy region failed!");
  }
}

pb::error::Errno ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  if (request->key().empty()) {
    return pb::error::EKEY_EMPTY;
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvGet request: " << request->key();

  auto errcode = ValidateKvGetRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Get key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);
  std::string value;
  errcode = storage_->KvGet(ctx, request->key(), value);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("KvGet failed");
  } else {
    response->set_value(value);
  }
}

pb::error::Errno ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return pb::error::EKEY_EMPTY;
    }
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvBatchGet request";

  auto errcode = ValidateKvBatchGetRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Get key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;
  errcode = storage_->KvBatchGet(ctx, Helper::PbRepeatedToVector<std::string>(request->keys()), kvs);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("KvGet failed");
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
}

pb::error::Errno ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  if (request->kv().key().empty()) {
    return pb::error::EKEY_EMPTY;
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvPut request: " << request->kv().key();

  auto errcode = ValidateKvPutRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Put key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);
  errcode = storage_->KvPut(ctx, request->kv());
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Put key failed");
    brpc::ClosureGuard done_guard(done);
  }
}

pb::error::Errno ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return pb::error::EKEY_EMPTY;
    }
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto errcode = ValidateKvBatchPutRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Batch Put key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);
  errcode = storage_->KvBatchPut(ctx, Helper::PbRepeatedToVector(request->kvs()));
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Put key failed");
    brpc::ClosureGuard done_guard(done);
  }
}

pb::error::Errno ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  if (request->kv().key().empty()) {
    return pb::error::EKEY_EMPTY;
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvPutIfAbsent request: ";
  auto errcode = ValidateKvPutIfAbsentRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Put key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);
  errcode = storage_->KvPutIfAbsent(ctx, request->kv());
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Put if absent key failed");
    brpc::ClosureGuard done_guard(done);
  }
}

pb::error::Errno ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return pb::error::EREGION_NOT_FOUND;
  }

  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return pb::error::EKEY_EMPTY;
    }
  }

  return pb::error::OK;
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvBatchPutIfAbsent request: ";

  auto errcode = ValidateKvBatchPutIfAbsentRequest(request);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Batch Put key failed");
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->set_region_id(request->region_id()).set_cf_name(kStoreDataCF);

  std::vector<std::string> put_keys;
  errcode = storage_->KvBatchPutIfAbsent(ctx, Helper::PbRepeatedToVector(request->kvs()), put_keys);
  if (errcode != pb::error::OK) {
    auto* err = response->mutable_error();
    err->set_errcode(errcode);
    err->set_errmsg("Batch put if absent key failed");
    brpc::ClosureGuard done_guard(done);
  }
}

void StoreServiceImpl::set_storage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb
