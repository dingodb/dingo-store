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

  void SetStorage(std::shared_ptr<Storage> storage);

 private:
  butil::Status ValidateVectorBatchQueryQequest(const dingodb::pb::index::VectorBatchQueryRequest* request);
  butil::Status ValidateVectorSearchRequest(const dingodb::pb::index::VectorSearchRequest* request);
  butil::Status ValidateVectorAddRequest(const dingodb::pb::index::VectorAddRequest* request);
  butil::Status ValidateVectorDeleteRequest(const dingodb::pb::index::VectorDeleteRequest* request);
  butil::Status ValidateVectorGetBorderIdRequest(const dingodb::pb::index::VectorGetBorderIdRequest* request);
  butil::Status ValidateVectorScanQueryRequest(const dingodb::pb::index::VectorScanQueryRequest* request);
  butil::Status ValidateVectorGetRegionMetricsRequest(const dingodb::pb::index::VectorGetRegionMetricsRequest* request);
  std::shared_ptr<Storage> storage_;
};

}  // namespace dingodb

#endif  // DINGODB_INDEx_SERVICE_H_
