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

#include "server/version_service.h"

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <optional>
#include <vector>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "common/constant.h"
#include "coordinator/coordinator_closure.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/version.pb.h"

namespace dingodb {

int VersionService::AddListenableVersion(VersionType type, uint64_t id, uint64_t version) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    return -1;
  }
  VersionListener* version_listener = new VersionListener;
  version_listener->version = version;
  version_listener->ref_count = 0;
  version_listeners_[type].insert({id, version_listener});
  return 0;
}

int VersionService::DelListenableVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    version_listeners_[type].erase(id);
    VersionListener* version_listener = exist->second;
    version_listener->version.store(0);
    while (version_listener->ref_count != 0) {
      lock.unlock();
      version_listener->condition.notify_all();
      bthread_usleep(1000000);
      lock.lock();
    }
    delete version_listener;
    return 0;
  }
  return -1;
}

uint64_t VersionService::GetCurrentVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    return exist->second->version;
  }
  return 0;
}

uint64_t VersionService::GetNewVersion(VersionType type, uint64_t id, uint64_t curr_version, uint wait_seconds) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    if (version_listener->version != curr_version) {
      return version_listener->version;
    }
    version_listener->ref_count++;
    lock.unlock();
    std::unique_lock<bthread::Mutex> lock(version_listener->mutex);
    version_listener->condition.wait_for(lock, std::chrono::microseconds(std::chrono::seconds(wait_seconds)).count());
    uint64_t version = version_listener->version;
    version_listener->ref_count--;
    return version;
  }
  return 0;
}

int VersionService::UpdateVersion(VersionType type, uint64_t id, uint64_t version) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    version_listener->version = version;
    version_listener->condition.notify_all();
    return 0;
  }
  return -1;
}

uint64_t VersionService::IncVersion(VersionType type, uint64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    uint64_t new_version = version_listener->version++;
    version_listener->condition.notify_all();
    return new_version;
  }
  return 0;
}

VersionService& VersionService::GetInstance() {
  static VersionService service;
  return service;
}

void VersionServiceProtoImpl::LeaseGrant(google::protobuf::RpcController* controller,
                                         const pb::version::LeaseGrantRequest* request,
                                         pb::version::LeaseGrantResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(INFO) << "Receive LeaseGrant Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->ttl() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("ttl is zero or negative");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // lease grant
  uint64_t granted_id = 0;
  int64_t granted_ttl_seconds = 0;

  auto ret =
      coordinator_control_->LeaseGrant(request->id(), request->ttl(), granted_id, granted_ttl_seconds, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseGrant failed:  lease_id=" << granted_id;
    return;
  }
  DINGO_LOG(INFO) << "LeaseGrant success:  lease_id=" << granted_id;

  response->set_id(granted_id);
  response->set_ttl(granted_ttl_seconds);

  // prepare for raft process
  CoordinatorClosure<pb::version::LeaseGrantRequest, pb::version::LeaseGrantResponse>* meta_closure =
      new CoordinatorClosure<pb::version::LeaseGrantRequest, pb::version::LeaseGrantResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(controller), meta_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
}

void VersionServiceProtoImpl::LeaseRevoke(google::protobuf::RpcController* controller,
                                          const pb::version::LeaseRevokeRequest* request,
                                          pb::version::LeaseRevokeResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive LeaseRevoke Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control_->LeaseRevoke(request->id(), meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseRevoke failed:  lease_id=" << request->id();
    return;
  }
  DINGO_LOG(INFO) << "LeaseRevoke success:  lease_id=" << request->id();

  // prepare for raft process
  CoordinatorClosure<pb::version::LeaseRevokeRequest, pb::version::LeaseRevokeResponse>* meta_closure =
      new CoordinatorClosure<pb::version::LeaseRevokeRequest, pb::version::LeaseRevokeResponse>(request, response,
                                                                                                done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(controller), meta_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
}

void VersionServiceProtoImpl::LeaseRenew(google::protobuf::RpcController* controller,
                                         const pb::version::LeaseRenewRequest* request,
                                         pb::version::LeaseRenewResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive LeaseRenew Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t ttl_seconds = 0;
  auto ret = coordinator_control_->LeaseRenew(request->id(), ttl_seconds, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseRenew failed:  lease_id=" << request->id();
    return;
  }
  DINGO_LOG(INFO) << "LeaseRenew success:  lease_id=" << request->id();

  response->set_id(request->id());
  response->set_ttl(ttl_seconds);

  // prepare for raft process
  CoordinatorClosure<pb::version::LeaseRenewRequest, pb::version::LeaseRenewResponse>* meta_closure =
      new CoordinatorClosure<pb::version::LeaseRenewRequest, pb::version::LeaseRenewResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(controller), meta_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
}

void VersionServiceProtoImpl::LeaseQuery(google::protobuf::RpcController* /*controller*/,
                                         const pb::version::LeaseQueryRequest* request,
                                         pb::version::LeaseQueryResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive LeaseTimeToLive Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  int64_t granted_ttl_seconde = 0;
  int64_t remaining_ttl_seconds = 0;
  std::set<std::string> keys;

  auto ret = coordinator_control_->LeaseQuery(request->id(), request->keys(), granted_ttl_seconde,
                                              remaining_ttl_seconds, keys);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseTimeToLive failed:  lease_id=" << request->id();
    return;
  }
  DINGO_LOG(INFO) << "LeaseTimeToLive success:  lease_id=" << request->id();

  response->set_id(request->id());
  response->set_grantedttl(granted_ttl_seconde);
  response->set_ttl(remaining_ttl_seconds);
  if (!keys.empty()) {
    for (const auto& key : keys) {
      response->add_keys(key);
    }
  }
}

void VersionServiceProtoImpl::ListLeases(google::protobuf::RpcController* /*controller*/,
                                         const pb::version::ListLeasesRequest* request,
                                         pb::version::ListLeasesResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive ListLeases Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  std::vector<pb::coordinator_internal::LeaseInternal> leases;

  auto ret = coordinator_control_->ListLeases(leases);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "ListLeases failed";
    return;
  }
  DINGO_LOG(INFO) << "ListLeases success";

  if (!leases.empty()) {
    for (const auto& lease : leases) {
      auto* lease_status = response->add_leases();
      lease_status->set_id(lease.id());
    }
  }
}

}  // namespace dingodb
