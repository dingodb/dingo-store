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

#ifndef DINGODB_DISKANN_SERVICE_H_
#define DINGODB_DISKANN_SERVICE_H_

#include <memory>

#include "diskann/diskann_service_handle.h"
#include "proto/diskann.pb.h"

namespace dingodb {

class DiskAnnServiceImpl : public pb::diskann::DiskAnnService {
 public:
  void VectorNew(google::protobuf::RpcController* controller, const ::dingodb::pb::diskann::VectorNewRequest* request,
                 ::dingodb::pb::diskann::VectorNewResponse* response, ::google::protobuf::Closure* done) override;

  void VectorPushData(google::protobuf::RpcController* controller,
                      const ::dingodb::pb::diskann::VectorPushDataRequest* request,
                      ::dingodb::pb::diskann::VectorPushDataResponse* response,
                      ::google::protobuf::Closure* done) override;

  void VectorBuild(google::protobuf::RpcController* controller,
                   const ::dingodb::pb::diskann::VectorBuildRequest* request,
                   ::dingodb::pb::diskann::VectorBuildResponse* response, ::google::protobuf::Closure* done) override;

  void VectorLoad(google::protobuf::RpcController* controller, const ::dingodb::pb::diskann::VectorLoadRequest* request,
                  ::dingodb::pb::diskann::VectorLoadResponse* response, ::google::protobuf::Closure* done) override;

  void VectorTryLoad(google::protobuf::RpcController* controller,
                     const ::dingodb::pb::diskann::VectorTryLoadRequest* request,
                     ::dingodb::pb::diskann::VectorTryLoadResponse* response,
                     ::google::protobuf::Closure* done) override;

  void VectorSearch(google::protobuf::RpcController* controller,
                    const ::dingodb::pb::diskann::VectorSearchRequest* request,
                    ::dingodb::pb::diskann::VectorSearchResponse* response, ::google::protobuf::Closure* done) override;

  void VectorReset(google::protobuf::RpcController* controller,
                   const ::dingodb::pb::diskann::VectorResetRequest* request,
                   ::dingodb::pb::diskann::VectorResetResponse* response, ::google::protobuf::Closure* done) override;

  void VectorClose(google::protobuf::RpcController* controller,
                   const ::dingodb::pb::diskann::VectorCloseRequest* request,
                   ::dingodb::pb::diskann::VectorCloseResponse* response, ::google::protobuf::Closure* done) override;

  void VectorDestroy(google::protobuf::RpcController* controller,
                     const ::dingodb::pb::diskann::VectorDestroyRequest* request,
                     ::dingodb::pb::diskann::VectorDestroyResponse* response,
                     ::google::protobuf::Closure* done) override;

  void VectorStatus(google::protobuf::RpcController* controller,
                    const ::dingodb::pb::diskann::VectorStatusRequest* request,
                    ::dingodb::pb::diskann::VectorStatusResponse* response, ::google::protobuf::Closure* done) override;
  
  void VectorCount(google::protobuf::RpcController* controller,
                   const ::dingodb::pb::diskann::VectorCountRequest* request,
                   ::dingodb::pb::diskann::VectorCountResponse* response, ::google::protobuf::Closure* done) override;

  void VectorDump(google::protobuf::RpcController* controller, const ::dingodb::pb::diskann::VectorDumpRequest* request,
                  ::dingodb::pb::diskann::VectorDumpResponse* response, ::google::protobuf::Closure* done) override;

  void VectorDumpAll(google::protobuf::RpcController* controller,
                     const ::dingodb::pb::diskann::VectorDumpAllRequest* request,
                     ::dingodb::pb::diskann::VectorDumpAllResponse* response,
                     ::google::protobuf::Closure* done) override;
  void SetHandle(std::shared_ptr<DiskAnnServiceHandle> handle) { handle_ = handle; }

 private:
  std::shared_ptr<DiskAnnServiceHandle> handle_;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_SERVICE_H_