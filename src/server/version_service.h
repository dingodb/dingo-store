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

#ifndef DINGODB_LISTEN_SERVICE_H_
#define DINGODB_LISTEN_SERVICE_H_

#include <atomic>
#include <cstdint>
#include <iostream>
#include <unordered_map>

#include "brpc/server.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "coordinator/coordinator_control.h"
#include "engine/engine.h"
#include "google/protobuf/stubs/callback.h"
#include "proto/version.pb.h"

using dingodb::pb::version::GetCurrVersionRequest;
using dingodb::pb::version::GetCurrVersionResponse;
using dingodb::pb::version::GetNewVersionRequest;
using dingodb::pb::version::GetNewVersionResponse;
using dingodb::pb::version::VersionId;
using dingodb::pb::version::VersionType;
using dingodb::pb::version::VersionType_ARRAYSIZE;

namespace dingodb {

struct VersionListener {
  std::atomic_int ref_count;
  std::atomic_uint64_t version;
  bthread::Mutex mutex;
  bthread::ConditionVariable condition;
};

class VersionService {
 public:
  static VersionService& GetInstance();

  VersionService(const VersionService&) = delete;
  VersionService& operator=(const VersionService&) = delete;

  int AddListenableVersion(VersionType type, uint64_t id, uint64_t version);
  int DelListenableVersion(VersionType type, uint64_t id);

  uint64_t GetCurrentVersion(VersionType type, uint64_t id);
  uint64_t GetNewVersion(VersionType type, uint64_t id, uint64_t curr_version, uint wait_seconds);

  uint64_t IncVersion(VersionType type, uint64_t id);
  int UpdateVersion(VersionType type, uint64_t id, uint64_t version);

  static int AddListenableVersion(VersionId id, uint64_t version) {
    return GetInstance().AddListenableVersion(id.type(), id.id(), version);
  }
  static int DelListenableVersion(VersionId id) { return GetInstance().DelListenableVersion(id.type(), id.id()); }

  static uint64_t GetCurrentVersion(VersionId id) { return GetInstance().GetCurrentVersion(id.type(), id.id()); }
  static uint64_t GetNewVersion(VersionId id, uint64_t curr_version, uint wait_seconds) {
    return GetInstance().GetNewVersion(id.type(), id.id(), curr_version, wait_seconds);
  }

  static uint64_t IncVersion(VersionId id) { return GetInstance().IncVersion(id.type(), id.id()); }
  static int UpdateVersion(VersionId id, uint64_t version) {
    return GetInstance().UpdateVersion(id.type(), id.id(), version);
  }

 private:
  VersionService() = default;
  ~VersionService() = default;

  bthread::Mutex mutex_;
  std::array<std::unordered_map<uint64_t, VersionListener*>, VersionType_ARRAYSIZE> version_listeners_;
};

class VersionServiceProtoImpl : public dingodb::pb::version::VersionService {
 public:
  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    this->coordinator_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    error_in_response->mutable_leader_location()->CopyFrom(leader_location);
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
  }

  template <typename T>
  void RedirectResponse(std::shared_ptr<RaftNode> raft_node, T response) {
    // parse leader raft location from string
    auto leader_string = raft_node->GetLeaderId().to_string();

    pb::common::Location leader_raft_location;
    int ret = Helper::PeerIdToLocation(raft_node->GetLeaderId(), leader_raft_location);
    if (ret < 0) {
      return;
    }

    // GetServerLocation
    pb::common::Location leader_server_location;
    coordinator_control_->GetServerLocation(leader_raft_location, leader_server_location);

    auto* error_in_response = response->mutable_error();
    error_in_response->mutable_leader_location()->CopyFrom(leader_server_location);
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };
  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    this->coordinator_control_ = coordinator_control;
  };

  void GetNewVersion(google::protobuf::RpcController* cntl_base, const GetNewVersionRequest* request,
                     GetNewVersionResponse* response, google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    response->set_version(dingodb::VersionService::GetNewVersion(request->verid(), request->version(), 60));
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->response_attachment().append(cntl->request_attachment());
  }

  void GetCurrVersion(google::protobuf::RpcController* cntl_base, const GetCurrVersionRequest* request,
                      GetCurrVersionResponse* response, google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    response->set_version(dingodb::VersionService::GetCurrentVersion(request->verid()));
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->response_attachment().append(cntl->request_attachment());
  }

  // lease
  void LeaseGrant(google::protobuf::RpcController* cntl_basecontroller, const pb::version::LeaseGrantRequest* request,
                  pb::version::LeaseGrantResponse* response, google::protobuf::Closure* done) override;

  void LeaseRevoke(google::protobuf::RpcController* cntl_basecontroller, const pb::version::LeaseRevokeRequest* request,
                   pb::version::LeaseRevokeResponse* response, google::protobuf::Closure* done) override;

  void LeaseRenew(google::protobuf::RpcController* cntl_basecontroller, const pb::version::LeaseRenewRequest* request,
                  pb::version::LeaseRenewResponse* response, google::protobuf::Closure* done) override;

  void LeaseQuery(google::protobuf::RpcController* cntl_basecontroller, const pb::version::LeaseQueryRequest* request,
                  pb::version::LeaseQueryResponse* response, google::protobuf::Closure* done) override;

  void ListLeases(google::protobuf::RpcController* cntl_basecontroller, const pb::version::ListLeasesRequest* request,
                  pb::version::ListLeasesResponse* response, google::protobuf::Closure* done) override;

  // kv
  void GetRawKvIndex(google::protobuf::RpcController* controller, const pb::version::GetRawKvIndexRequest* request,
                     pb::version::GetRawKvIndexResponse* response, google::protobuf::Closure* done) override;
  void GetRawKvRev(google::protobuf::RpcController* controller, const pb::version::GetRawKvRevRequest* request,
                   pb::version::GetRawKvRevResponse* response, google::protobuf::Closure* done) override;
  void KvRange(google::protobuf::RpcController* controller, const pb::version::RangeRequest* request,
               pb::version::RangeResponse* response, google::protobuf::Closure* done) override;
  void KvPut(google::protobuf::RpcController* controller, const pb::version::PutRequest* request,
             pb::version::PutResponse* response, google::protobuf::Closure* done) override;
  void KvDeleteRange(google::protobuf::RpcController* controller, const pb::version::DeleteRangeRequest* request,
                     pb::version::DeleteRangeResponse* response, google::protobuf::Closure* done) override;

  // watch
  void Watch(google::protobuf::RpcController* controller, const pb::version::WatchRequest* request,
             pb::version::WatchResponse* response, google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_control_;
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif
