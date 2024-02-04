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

#ifndef DINGODB_STORE_SERVICE_H_
#define DINGODB_STORE_SERVICE_H_

#include "engine/storage.h"
#include "proto/store.pb.h"

namespace dingodb {

class StoreServiceImpl : public pb::store::StoreService {
 public:
  StoreServiceImpl();

  // debug
  void Hello(google::protobuf::RpcController* controller, const pb::store::HelloRequest* request,
             pb::store::HelloResponse* response, google::protobuf::Closure* done) override;
  void GetMemoryInfo(google::protobuf::RpcController* controller, const pb::store::HelloRequest* request,
                     pb::store::HelloResponse* response, google::protobuf::Closure* done) override;

  // raw kv read
  void KvGet(google::protobuf::RpcController* controller, const pb::store::KvGetRequest* request,
             pb::store::KvGetResponse* response, google::protobuf::Closure* done) override;

  void KvBatchGet(google::protobuf::RpcController* controller, const pb::store::KvBatchGetRequest* request,
                  pb::store::KvBatchGetResponse* response, google::protobuf::Closure* done) override;

  void KvScanBegin(google::protobuf::RpcController* controller, const ::dingodb::pb::store::KvScanBeginRequest* request,
                   ::dingodb::pb::store::KvScanBeginResponse* response, ::google::protobuf::Closure* done) override;

  void KvScanContinue(google::protobuf::RpcController* controller,
                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                      ::dingodb::pb::store::KvScanContinueResponse* response,
                      ::google::protobuf::Closure* done) override;

  void KvScanRelease(google::protobuf::RpcController* controller,
                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                     ::dingodb::pb::store::KvScanReleaseResponse* response, ::google::protobuf::Closure* done) override;

  void KvScanBeginV2(google::protobuf::RpcController* controller,
                     const ::dingodb::pb::store::KvScanBeginRequestV2* request,
                     ::dingodb::pb::store::KvScanBeginResponseV2* response, ::google::protobuf::Closure* done) override;

  void KvScanContinueV2(::google::protobuf::RpcController* controller,
                        const ::dingodb::pb::store::KvScanContinueRequestV2* request,
                        ::dingodb::pb::store::KvScanContinueResponseV2* response,
                        ::google::protobuf::Closure* done) override;

  void KvScanReleaseV2(::google::protobuf::RpcController* controller,
                       const ::dingodb::pb::store::KvScanReleaseRequestV2* request,
                       ::dingodb::pb::store::KvScanReleaseResponseV2* response,
                       ::google::protobuf::Closure* done) override;

  // rawkv write
  void KvPut(google::protobuf::RpcController* controller, const pb::store::KvPutRequest* request,
             pb::store::KvPutResponse* response, google::protobuf::Closure* done) override;

  void KvBatchPut(google::protobuf::RpcController* controller, const pb::store::KvBatchPutRequest* request,
                  pb::store::KvBatchPutResponse* response, google::protobuf::Closure* done) override;

  void KvBatchDelete(google::protobuf::RpcController* controller, const pb::store::KvBatchDeleteRequest* request,
                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) override;

  void KvDeleteRange(google::protobuf::RpcController* controller, const pb::store::KvDeleteRangeRequest* request,
                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) override;

  void KvPutIfAbsent(google::protobuf::RpcController* controller, const pb::store::KvPutIfAbsentRequest* request,
                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) override;

  void KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                          const pb::store::KvBatchPutIfAbsentRequest* request,
                          pb::store::KvBatchPutIfAbsentResponse* response, google::protobuf::Closure* done) override;

  void KvCompareAndSet(google::protobuf::RpcController* controller, const pb::store::KvCompareAndSetRequest* request,
                       pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done) override;

  void KvBatchCompareAndSet(google::protobuf::RpcController* controller,
                            const pb::store::KvBatchCompareAndSetRequest* request,
                            pb::store::KvBatchCompareAndSetResponse* response,
                            google::protobuf::Closure* done) override;

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
  void SetReadWorkSet(PriorWorkerSetPtr worker_set) { read_worker_set_ = worker_set; }
  void SetWriteWorkSet(PriorWorkerSetPtr worker_set) { write_worker_set_ = worker_set; }
  void SetRaftApplyWorkSet(PriorWorkerSetPtr worker_set) { raft_apply_worker_set_ = worker_set; }

  bool IsRaftApplyPendingExceed();

 private:
  StoragePtr storage_;
  // Run service request.
  PriorWorkerSetPtr read_worker_set_;
  PriorWorkerSetPtr write_worker_set_;
  PriorWorkerSetPtr raft_apply_worker_set_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVICE_H_
