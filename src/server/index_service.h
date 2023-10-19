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
#include "proto/index.pb.h"

namespace dingodb {

class IndexServiceImpl : public pb::index::IndexService {
 public:
  IndexServiceImpl();

  // vector
  void VectorBatchQuery(google::protobuf::RpcController* controller, const pb::index::VectorBatchQueryRequest* request,
                        pb::index::VectorBatchQueryResponse* response, google::protobuf::Closure* done) override;
  void VectorSearch(google::protobuf::RpcController* controller, const pb::index::VectorSearchRequest* request,
                    pb::index::VectorSearchResponse* response, google::protobuf::Closure* done) override;
  void VectorAdd(google::protobuf::RpcController* controller, const pb::index::VectorAddRequest* request,
                 pb::index::VectorAddResponse* response, google::protobuf::Closure* done) override;
  void VectorDelete(google::protobuf::RpcController* controller, const pb::index::VectorDeleteRequest* request,
                    pb::index::VectorDeleteResponse* response, google::protobuf::Closure* done) override;
  void VectorGetBorderId(google::protobuf::RpcController* controller,
                         const pb::index::VectorGetBorderIdRequest* request,
                         pb::index::VectorGetBorderIdResponse* response, google::protobuf::Closure* done) override;
  void VectorScanQuery(google::protobuf::RpcController* controller, const pb::index::VectorScanQueryRequest* request,
                       pb::index::VectorScanQueryResponse* response, google::protobuf::Closure* done) override;
  void VectorGetRegionMetrics(google::protobuf::RpcController* controller,
                              const pb::index::VectorGetRegionMetricsRequest* request,
                              pb::index::VectorGetRegionMetricsResponse* response,
                              google::protobuf::Closure* done) override;

  void VectorCount(google::protobuf::RpcController* controller, const pb::index::VectorCountRequest* request,
                   pb::index::VectorCountResponse* response, ::google::protobuf::Closure* done) override;

  // for debug
  void VectorSearchDebug(google::protobuf::RpcController* controller,
                         const pb::index::VectorSearchDebugRequest* request,
                         pb::index::VectorSearchDebugResponse* response, google::protobuf::Closure* done) override;

  // txn api
  void TxnGet(google::protobuf::RpcController* controller, const pb::index::TxnGetRequest* request,
              pb::index::TxnGetResponse* response, google::protobuf::Closure* done) override;
  void TxnScan(google::protobuf::RpcController* controller, const pb::index::TxnScanRequest* request,
               pb::index::TxnScanResponse* response, google::protobuf::Closure* done) override;
  void TxnPrewrite(google::protobuf::RpcController* controller, const pb::index::TxnPrewriteRequest* request,
                   pb::index::TxnPrewriteResponse* response, google::protobuf::Closure* done) override;
  void TxnCommit(google::protobuf::RpcController* controller, const pb::index::TxnCommitRequest* request,
                 pb::index::TxnCommitResponse* response, google::protobuf::Closure* done) override;
  void TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                         const pb::index::TxnCheckTxnStatusRequest* request,
                         pb::index::TxnCheckTxnStatusResponse* response, google::protobuf::Closure* done) override;
  void TxnResolveLock(google::protobuf::RpcController* controller, const pb::index::TxnResolveLockRequest* request,
                      pb::index::TxnResolveLockResponse* response, google::protobuf::Closure* done) override;
  void TxnBatchGet(google::protobuf::RpcController* controller, const pb::index::TxnBatchGetRequest* request,
                   pb::index::TxnBatchGetResponse* response, google::protobuf::Closure* done) override;
  void TxnBatchRollback(google::protobuf::RpcController* controller, const pb::index::TxnBatchRollbackRequest* request,
                        pb::index::TxnBatchRollbackResponse* response, google::protobuf::Closure* done) override;
  void TxnScanLock(google::protobuf::RpcController* controller, const pb::index::TxnScanLockRequest* request,
                   pb::index::TxnScanLockResponse* response, google::protobuf::Closure* done) override;
  void TxnHeartBeat(google::protobuf::RpcController* controller, const pb::index::TxnHeartBeatRequest* request,
                    pb::index::TxnHeartBeatResponse* response, google::protobuf::Closure* done) override;
  void TxnGc(google::protobuf::RpcController* controller, const pb::index::TxnGcRequest* request,
             pb::index::TxnGcResponse* response, google::protobuf::Closure* done) override;
  void TxnDeleteRange(google::protobuf::RpcController* controller, const pb::index::TxnDeleteRangeRequest* request,
                      pb::index::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) override;
  void TxnDump(google::protobuf::RpcController* controller, const pb::index::TxnDumpRequest* request,
               pb::index::TxnDumpResponse* response, google::protobuf::Closure* done) override;

  void SetStorage(StoragePtr storage) { storage_ = storage; }
  void SetWorkSet(WorkerSetPtr worker_set) { worker_set_ = worker_set; }

 private:
  StoragePtr storage_;
  // Run service request.
  WorkerSetPtr worker_set_;
};

}  // namespace dingodb

#endif  // DINGODB_INDEx_SERVICE_H_
