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

#ifndef DINGODB_UTIL_SERVICE_H_
#define DINGODB_UTIL_SERVICE_H_

#include "engine/storage.h"
#include "proto/util.pb.h"

namespace dingodb {

class UtilServiceImpl : public pb::util::UtilService {
 public:
  UtilServiceImpl() = default;

  void VectorCalcDistance(google::protobuf::RpcController* controller,
                          const ::dingodb::pb::index::VectorCalcDistanceRequest* request,
                          ::dingodb::pb::index::VectorCalcDistanceResponse* response,
                          ::google::protobuf::Closure* done) override;

  void SetStorage(StoragePtr storage) { storage_ = storage; }
  void SetReadWorkSet(PriorWorkerSetPtr worker_set) { read_worker_set_ = worker_set; }

 private:
  std::shared_ptr<Storage> storage_;
  // Run service request.
  PriorWorkerSetPtr read_worker_set_;
};

void DoVectorCalcDistance(StoragePtr storage, google::protobuf::RpcController* controller,
                          const pb::index::VectorCalcDistanceRequest* request,
                          pb::index::VectorCalcDistanceResponse* response, google::protobuf::Closure* done);

}  // namespace dingodb

#endif  // DINGODB_INDEx_SERVICE_H_
