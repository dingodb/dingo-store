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

#ifndef DINGODB_COORDINATOR_CLOSURE_H__
#define DINGODB_COORDINATOR_CLOSURE_H__

#include "braft/util.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

using CreateStoreRequest = pb::coordinator::CreateStoreRequest;
using CreateStoreRespone = pb::coordinator::CreateStoreResponse;

class CreateStoreClosure : public braft::Closure {
 public:
  CreateStoreClosure(const CreateStoreRequest* request, CreateStoreRespone* response, google::protobuf::Closure* done)
      : request_(request), response_(response), done_(done) {}
  ~CreateStoreClosure() override = default;

  const CreateStoreRequest* request() const { return request_; }  // NOLINT
  CreateStoreRespone* response() const { return response_; }      // NOLINT
  void Run() override;

 private:
  const CreateStoreRequest* request_;
  CreateStoreRespone* response_;
  google::protobuf::Closure* done_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_COMMON_H_
