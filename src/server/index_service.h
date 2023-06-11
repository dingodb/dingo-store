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

#ifndef DINGODB_INDEX_SERVICE_H_
#define DINGODB_INDEX_SERVICE_H_

#include "brpc/controller.h"
#include "brpc/server.h"
#include "engine/storage.h"
#include "hnswlib/hnswlib.h"
#include "proto/index.pb.h"

namespace dingodb {

class IndexServiceImpl : public pb::index::IndexService {
 public:
  IndexServiceImpl();

  void AddRegion(google::protobuf::RpcController* controller, const pb::index::AddRegionRequest* request,
                 pb::index::AddRegionResponse* response, google::protobuf::Closure* done) override;

  void ChangeRegion(google::protobuf::RpcController* controller, const pb::index::ChangeRegionRequest* request,
                    pb::index::ChangeRegionResponse* response, google::protobuf::Closure* done) override;

  void DestroyRegion(google::protobuf::RpcController* controller, const pb::index::DestroyRegionRequest* request,
                     pb::index::DestroyRegionResponse* response, google::protobuf::Closure* done) override;

  void Snapshot(google::protobuf::RpcController* controller, const pb::index::SnapshotRequest* request,
                pb::index::SnapshotResponse* response, google::protobuf::Closure* done) override;

  void TransferLeader(google::protobuf::RpcController* controller, const pb::index::TransferLeaderRequest* request,
                      pb::index::TransferLeaderResponse* response, google::protobuf::Closure* done) override;

  void KvGet(google::protobuf::RpcController* controller, const pb::index::KvGetRequest* request,
             pb::index::KvGetResponse* response, google::protobuf::Closure* done) override;

  void KvBatchGet(google::protobuf::RpcController* controller, const pb::index::KvBatchGetRequest* request,
                  pb::index::KvBatchGetResponse* response, google::protobuf::Closure* done) override;

  void KvPut(google::protobuf::RpcController* controller, const pb::index::KvPutRequest* request,
             pb::index::KvPutResponse* response, google::protobuf::Closure* done) override;

  void KvBatchPut(google::protobuf::RpcController* controller, const pb::index::KvBatchPutRequest* request,
                  pb::index::KvBatchPutResponse* response, google::protobuf::Closure* done) override;

  void KvPutIfAbsent(google::protobuf::RpcController* controller, const pb::index::KvPutIfAbsentRequest* request,
                     pb::index::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) override;

  void KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                          const pb::index::KvBatchPutIfAbsentRequest* request,
                          pb::index::KvBatchPutIfAbsentResponse* response, google::protobuf::Closure* done) override;

  void KvBatchDelete(google::protobuf::RpcController* controller, const pb::index::KvBatchDeleteRequest* request,
                     pb::index::KvBatchDeleteResponse* response, google::protobuf::Closure* done) override;

  void Debug(google::protobuf::RpcController* controller, const ::dingodb::pb::index::DebugRequest* request,
             ::dingodb::pb::index::DebugResponse* response, ::google::protobuf::Closure* done) override;

  void SetStorage(std::shared_ptr<Storage> storage);

 private:
  std::shared_ptr<Storage> storage_;
};

}  // namespace dingodb

#endif  // DINGODB_INDEx_SERVICE_H_
