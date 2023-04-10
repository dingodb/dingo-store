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

#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

using dingodb::pb::error::Errno;

namespace dingodb {

StoreServiceImpl::StoreServiceImpl() = default;

// Set error response leader location.
// Priority from local storemap query, and then from remote node query.
template <typename T>
void RedirectLeader(std::string addr, T* response) {
  DINGO_LOG(INFO) << "Redirect leader " << addr;
  auto raft_endpoint = Helper::StrToEndPoint(addr);
  if (raft_endpoint.port == 0) return;

  // From local store map query.
  butil::EndPoint server_endpoint = Helper::QueryServerEndpointByRaftEndpoint(
      Server::GetInstance()->GetStoreMetaManager()->GetAllStore(), raft_endpoint);
  if (server_endpoint.port == 0) {
    // From remote node query.
    pb::common::Location server_location;
    Helper::GetServerLocation(Helper::EndPointToLocation(raft_endpoint), server_location);
    if (server_location.port() > 0) {
      server_endpoint = Helper::LocationToEndPoint(server_location);
    }
  }

  Helper::SetPbMessageErrorLeader(server_endpoint, response);
}

void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::store::AddRegionRequest* request,
                                 dingodb::pb::store::AddRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "AddRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto status = store_control->AddRegion(ctx, std::make_shared<pb::common::Region>(request->region()));
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                    const pb::store::ChangeRegionRequest* request,
                                    pb::store::ChangeRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "ChangeRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();
  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto status = store_control->ChangeRegion(ctx, std::make_shared<pb::common::Region>(request->region()));
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::DestroyRegionRequest* request,
                                     dingodb::pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "DestroyRegion request...";

  auto store_control = Server::GetInstance()->GetStoreControl();
  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto status = store_control->DeleteRegion(ctx, request->region_id());
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::Snapshot(google::protobuf::RpcController* controller, const pb::store::SnapshotRequest* request,
                                pb::store::SnapshotResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "Snapshot request... ";

  auto store_control = Server::GetInstance()->GetStoreControl();
  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  auto status = store_control->Snapshot(ctx, request->region_id());
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

butil::Status ValidateRegion(uint64_t region_id) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto region = store_meta_manager->GetRegion(region_id);
  // Check is exist region.
  if (!region) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }
  if (region->state() == pb::common::REGION_NEW) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is new, waiting later");
  }
  if (region->state() == pb::common::REGION_DELETE || region->state() == pb::common::REGION_DELETING ||
      region->state() == pb::common::REGION_DELETED) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is deleting");
  }

  return butil::Status();
}

butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto status = ValidateRegion(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "KvGet request: " << request->key();

  butil::Status status = ValidateKvGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  auto mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->KvGet(ctx, keys, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    return;
  }
  if (kvs.size() > 0) {
    response->set_value(kvs[0].value());
  }
}

butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }
  }

  auto status = ValidateRegion(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "KvBatchGet request";

  butil::Status status = ValidateKvBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;
  auto mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage_->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
}

butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto status = ValidateRegion(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "KvPut request: " << request->kv().key();

  butil::Status status = ValidateKvPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPut(ctx, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
  }

  auto status = ValidateRegion(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  status = storage_->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto status = ValidateRegion(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "KvPutIfAbsent request: ";
  butil::Status status = ValidateKvPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPutIfAbsent(ctx, kvs, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }
  }

  auto status = ValidateRegion(request->region_id());
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
  DINGO_LOG(INFO) << "KvBatchPutIfAbsent request: ";

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage_->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage_->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status ValidateKvDeleteRangeRequest(const dingodb::pb::store::KvDeleteRangeRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->range().start_key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "start_key is empty");
  }

  if (request->range().end_key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "end_key is empty");
  }

  return butil::Status();
}

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvDeleteRangeRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvDeleteRangeRequest*>(request);
  status = storage_->KvDeleteRange(ctx, *mut_request->mutable_range());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status ValidateKvScanBeginRequest(const dingodb::pb::store::KvScanBeginRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (BAIDU_UNLIKELY(request->range().range().start_key().empty() || request->range().range().end_key().empty())) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (BAIDU_UNLIKELY(request->range().range().start_key() > request->range().range().end_key())) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");

  } else if (BAIDU_UNLIKELY(request->range().range().start_key() == request->range().range().end_key())) {
    if (request->range().with_start() && !request->range().with_end()) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
    }
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "KvScanBegin request";

  butil::Status status = ValidateKvScanBeginRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage_->KvScanBegin(ctx, Constant::kStoreDataCF, request->region_id(), request->range(),
                                 request->max_fetch_cnt(), request->key_only(), request->disable_auto_release(),
                                 &scan_id, &kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!response->kvs().empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;
}

butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

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
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs);

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!response->kvs().empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }
}

butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  return butil::Status();
}

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
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanRelease(ctx, request->scan_id());

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      RedirectLeader(status.error_str(), response);
    }
    return;
  }
}

void StoreServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb
