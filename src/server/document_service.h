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

#ifndef DINGODB_DOCUMENT_SERVICE_H_
#define DINGODB_DOCUMENT_SERVICE_H_

#include "document/document_index_manager.h"
#include "engine/storage.h"
#include "proto/document.pb.h"

namespace dingodb {

class DocumentServiceImpl : public pb::document::DocumentService {
 public:
  DocumentServiceImpl();

  void Hello(google::protobuf::RpcController* controller, const pb::document::HelloRequest* request,
             pb::document::HelloResponse* response, google::protobuf::Closure* done) override;
  void GetMemoryInfo(google::protobuf::RpcController* controller, const pb::document::HelloRequest* request,
                     pb::document::HelloResponse* response, google::protobuf::Closure* done) override;

  // document read
  void DocumentBatchQuery(google::protobuf::RpcController* controller,
                          const pb::document::DocumentBatchQueryRequest* request,
                          pb::document::DocumentBatchQueryResponse* response, google::protobuf::Closure* done) override;
  void DocumentSearch(google::protobuf::RpcController* controller, const pb::document::DocumentSearchRequest* request,
                      pb::document::DocumentSearchResponse* response, google::protobuf::Closure* done) override;
  void DocumentGetBorderId(google::protobuf::RpcController* controller,
                           const pb::document::DocumentGetBorderIdRequest* request,
                           pb::document::DocumentGetBorderIdResponse* response,
                           google::protobuf::Closure* done) override;
  void DocumentScanQuery(google::protobuf::RpcController* controller,
                         const pb::document::DocumentScanQueryRequest* request,
                         pb::document::DocumentScanQueryResponse* response, google::protobuf::Closure* done) override;
  void DocumentGetRegionMetrics(google::protobuf::RpcController* controller,
                                const pb::document::DocumentGetRegionMetricsRequest* request,
                                pb::document::DocumentGetRegionMetricsResponse* response,
                                google::protobuf::Closure* done) override;
  void DocumentCount(google::protobuf::RpcController* controller, const pb::document::DocumentCountRequest* request,
                     pb::document::DocumentCountResponse* response, ::google::protobuf::Closure* done) override;

  // document write
  void DocumentAdd(google::protobuf::RpcController* controller, const pb::document::DocumentAddRequest* request,
                   pb::document::DocumentAddResponse* response, google::protobuf::Closure* done) override;
  void DocumentDelete(google::protobuf::RpcController* controller, const pb::document::DocumentDeleteRequest* request,
                      pb::document::DocumentDeleteResponse* response, google::protobuf::Closure* done) override;

  // txn read
  void TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) override;
  void TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) override;
  void TxnBatchGet(google::protobuf::RpcController* controller, const pb::store::TxnBatchGetRequest* request,
                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) override;
  void TxnScanLock(google::protobuf::RpcController* controller, const pb::store::TxnScanLockRequest* request,
                   pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) override;
  void TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
               pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) override;

  // txn write
  void TxnPessimisticLock(google::protobuf::RpcController* controller,
                          const pb::store::TxnPessimisticLockRequest* request,
                          pb::store::TxnPessimisticLockResponse* response, google::protobuf::Closure* done) override;
  void TxnPessimisticRollback(google::protobuf::RpcController* controller,
                              const pb::store::TxnPessimisticRollbackRequest* request,
                              pb::store::TxnPessimisticRollbackResponse* response,
                              google::protobuf::Closure* done) override;
  void TxnPrewrite(google::protobuf::RpcController* controller, const pb::store::TxnPrewriteRequest* request,
                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) override;
  void TxnCommit(google::protobuf::RpcController* controller, const pb::store::TxnCommitRequest* request,
                 pb::store::TxnCommitResponse* response, google::protobuf::Closure* done) override;
  void TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                         const pb::store::TxnCheckTxnStatusRequest* request,
                         pb::store::TxnCheckTxnStatusResponse* response, google::protobuf::Closure* done) override;
  void TxnResolveLock(google::protobuf::RpcController* controller, const pb::store::TxnResolveLockRequest* request,
                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) override;
  void TxnBatchRollback(google::protobuf::RpcController* controller, const pb::store::TxnBatchRollbackRequest* request,
                        pb::store::TxnBatchRollbackResponse* response, google::protobuf::Closure* done) override;
  void TxnHeartBeat(google::protobuf::RpcController* controller, const pb::store::TxnHeartBeatRequest* request,
                    pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) override;
  void TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) override;
  void TxnDeleteRange(google::protobuf::RpcController* controller, const pb::store::TxnDeleteRangeRequest* request,
                      pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) override;

  void SetStorage(StoragePtr storage) { storage_ = storage; }
  void SetReadWorkSet(WorkerSetPtr worker_set) { read_worker_set_ = worker_set; }
  void SetWriteWorkSet(WorkerSetPtr worker_set) { write_worker_set_ = worker_set; }
  void SetDocumentIndexManager(DocumentIndexManagerPtr document_index_manager) {
    document_index_manager_ = document_index_manager;
  }

  bool IsBackgroundPendingTaskCountExceed();

 private:
  StoragePtr storage_;
  // Run service request.
  WorkerSetPtr read_worker_set_;
  WorkerSetPtr write_worker_set_;
  DocumentIndexManagerPtr document_index_manager_;
};

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_SERVICE_H_
