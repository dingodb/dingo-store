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
#include "region/store_region_manager.h"


namespace dingodb {


StoreServiceImpl::StoreServiceImpl() {
}

bool validateAddRegion(const dingodb::pb::store::AddRegionRequest* request,
                       std::string& error_msg) {
  return true;
}

void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::store::AddRegionRequest* request,
                                 dingodb::pb::store::AddRegionResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "AddRegion request...";

  // valiate region
  std::string error_msg;
  if (!validateAddRegion(request, error_msg)) {
    cntl->SetFailed(brpc::EREQUEST, "%s", error_msg.c_str());
    return;
  }

  LOG(INFO) << "here 00001";

  // Add raft node
  storage_->AddRegion(request->region().region_id(), request->region());

  LOG(INFO) << "here 00002";
  
  // Add region to store region manager
  StoreRegionManager::GetInstance()->AddRegion(request->region().region_id(),
                                               request->region());

  LOG(INFO) << "here 00003";
}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::DestroyRegionRequest* request,
                                     dingodb::pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "DestroyRegion request...";
}

bool validateKvGetRequest(const dingodb::pb::store::KvGetRequest* request,
                          std::string& error_msg) {
  return true;
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvGet request: " << request->key();
  // const google::protobuf::Message *msg = static_cast<const google::protobuf::Message*>(request);
  std::string error_msg;
  if (!validateKvGetRequest(request, error_msg)) {
    cntl->SetFailed(brpc::EREQUEST, "%s", error_msg.c_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->set_region_id(request->region_id());
  storage_->KvGet(ctx, request->key());
}

bool validateKvPutRequest(const dingodb::pb::store::KvPutRequest* request,
                          std::string& error_msg) {
  return true;
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvPut request: " << request->key();
  std::string error_msg;
  if (!validateKvPutRequest(request, error_msg)) {
    cntl->SetFailed(brpc::EREQUEST, "%s", error_msg.c_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->set_region_id(request->region_id());
  storage_->KvPut(ctx, request->key(), request->value());
}

void StoreServiceImpl::KvBatchPutIfAbsent(
  google::protobuf::RpcController* controller,
  const dingodb::pb::store::KvBatchPutIfAbsentRequest* request,
  dingodb::pb::store::KvBatchPutIfAbsentResponse* response,
  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvBatchPutIfAbsent request: ";
}

void StoreServiceImpl::set_storage(std::shared_ptr<Storage> storage) {
  storage_ = storage;
}

} // namespace dingodb
