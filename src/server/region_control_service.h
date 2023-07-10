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

#ifndef DINGODB_REGION_CONTROL_SERVICE_H_
#define DINGODB_REGION_CONTROL_SERVICE_H_

#include "brpc/controller.h"
#include "brpc/server.h"
#include "proto/region_control.pb.h"

namespace dingodb {

class RegionControlServiceImpl : public pb::region_control::RegionControlService {
 public:
  RegionControlServiceImpl() = default;

  void AddRegion(google::protobuf::RpcController* controller, const pb::region_control::AddRegionRequest* request,
                 pb::region_control::AddRegionResponse* response, google::protobuf::Closure* done) override;

  void ChangeRegion(google::protobuf::RpcController* controller, const pb::region_control::ChangeRegionRequest* request,
                    pb::region_control::ChangeRegionResponse* response, google::protobuf::Closure* done) override;

  void DestroyRegion(google::protobuf::RpcController* controller,
                     const pb::region_control::DestroyRegionRequest* request,
                     pb::region_control::DestroyRegionResponse* response, google::protobuf::Closure* done) override;

  void Snapshot(google::protobuf::RpcController* controller, const pb::region_control::SnapshotRequest* request,
                pb::region_control::SnapshotResponse* response, google::protobuf::Closure* done) override;

  void TransferLeader(google::protobuf::RpcController* controller,
                      const pb::region_control::TransferLeaderRequest* request,
                      pb::region_control::TransferLeaderResponse* response, google::protobuf::Closure* done) override;

  void SnapshotVectorIndex(google::protobuf::RpcController* controller,
                           const pb::region_control::SnapshotVectorIndexRequest* request,
                           pb::region_control::SnapshotVectorIndexResponse* response,
                           google::protobuf::Closure* done) override;

  void Debug(google::protobuf::RpcController* controller, const ::dingodb::pb::region_control::DebugRequest* request,
             ::dingodb::pb::region_control::DebugResponse* response, ::google::protobuf::Closure* done) override;
};

}  // namespace dingodb

#endif  // DINGODB_REGION_CONTROL_SERVICE_H_
