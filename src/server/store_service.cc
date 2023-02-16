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

#include "region/store_region_manager.h"


namespace dingodb {


StoreServiceImpl::StoreServiceImpl(std::shared_ptr<Storage> storage)
  : storage_(storage) {
}

void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::store::AddRegionRequest* request,
                                 dingodb::pb::store::AddRegionResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "AddRegion request...";


  // todo valiate region

  // Add raft node
  storage_->AddRegion(request->region_info().region_id(), request->region_info());
  
  // Add region to store region manager
  StoreRegionManager::GetInstance()->AddRegion(request->region_info().region_id(),
                                               request->region_info());

}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::DestroyRegionRequest* request,
                                     dingodb::pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvGet request...";
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvGet request: " << request->key();
  // const google::protobuf::Message *msg = static_cast<const google::protobuf::Message*>(request);

  response->set_value("KvGetResponse value");
  // cntl->SetFailed(5000, "key error");
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvPut request: " << request->key();
}

} // namespace dingodb
