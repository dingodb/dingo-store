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

#include "store/store_service.h"


namespace dingodb {


void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const pb::store::AddRegionRequest* request,
                                 pb::store::AddRegionResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const pb::store::DestroyRegionRequest* request,
                                     pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const pb::store::KvGetRequest* request,
                             pb::store::KvGetResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "KvGet request: " << request->key();

  // response->set_value("world");
  cntl->SetFailed(5000, "key error");
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const pb::store::KvPutRequest* request,
                             pb::store::KvPutResponse* response,
                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
}

} // namespace dingodb
