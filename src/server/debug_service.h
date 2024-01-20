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

#ifndef DINGODB_DEBUG_SERVICE_H_
#define DINGODB_DEBUG_SERVICE_H_

#include "proto/debug.pb.h"

namespace dingodb {

class DebugServiceImpl : public pb::debug::DebugService {
 public:
  DebugServiceImpl() = default;

  void AddRegion(google::protobuf::RpcController* controller, const pb::debug::AddRegionRequest* request,
                 pb::debug::AddRegionResponse* response, google::protobuf::Closure* done) override;

  void ChangeRegion(google::protobuf::RpcController* controller, const pb::debug::ChangeRegionRequest* request,
                    pb::debug::ChangeRegionResponse* response, google::protobuf::Closure* done) override;

  void MergeRegion(google::protobuf::RpcController* controller, const pb::debug::MergeRegionRequest* request,
                   pb::debug::MergeRegionResponse* response, google::protobuf::Closure* done) override;

  void DestroyRegion(google::protobuf::RpcController* controller, const pb::debug::DestroyRegionRequest* request,
                     pb::debug::DestroyRegionResponse* response, google::protobuf::Closure* done) override;

  void Snapshot(google::protobuf::RpcController* controller, const pb::debug::SnapshotRequest* request,
                pb::debug::SnapshotResponse* response, google::protobuf::Closure* done) override;

  void TransferLeader(google::protobuf::RpcController* controller, const pb::debug::TransferLeaderRequest* request,
                      pb::debug::TransferLeaderResponse* response, google::protobuf::Closure* done) override;

  void SnapshotVectorIndex(google::protobuf::RpcController* controller,
                           const pb::debug::SnapshotVectorIndexRequest* request,
                           pb::debug::SnapshotVectorIndexResponse* response, google::protobuf::Closure* done) override;

  void TriggerVectorIndexSnapshot(google::protobuf::RpcController* controller,
                                  const pb::debug::TriggerVectorIndexSnapshotRequest* request,
                                  pb::debug::TriggerVectorIndexSnapshotResponse* response,
                                  google::protobuf::Closure* done) override;

  void Compact(google::protobuf::RpcController* controller, const pb::debug::CompactRequest* request,
               pb::debug::CompactResponse* response, google::protobuf::Closure* done) override;

  void Debug(google::protobuf::RpcController* controller, const ::dingodb::pb::debug::DebugRequest* request,
             ::dingodb::pb::debug::DebugResponse* response, ::google::protobuf::Closure* done) override;

  void GetMemoryStats(google::protobuf::RpcController* controller,
                      const ::dingodb::pb::debug::GetMemoryStatsRequest* request,
                      ::dingodb::pb::debug::GetMemoryStatsResponse* response,
                      ::google::protobuf::Closure* done) override;

  void ReleaseFreeMemory(google::protobuf::RpcController* controller,
                         const ::dingodb::pb::debug::ReleaseFreeMemoryRequest* request,
                         ::dingodb::pb::debug::ReleaseFreeMemoryResponse* response,
                         ::google::protobuf::Closure* done) override;

  void TraceWorkQueue(google::protobuf::RpcController* controller,
                      const ::dingodb::pb::debug::TraceWorkQueueRequest* request,
                      ::dingodb::pb::debug::TraceWorkQueueResponse* response,
                      ::google::protobuf::Closure* done) override;
};

}  // namespace dingodb

#endif  // DINGODB_DEBUG_SERVICE_H_
