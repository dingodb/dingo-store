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
#include "proto/common.pb.h"
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

void VersionServiceProtoImpl::GetRawKvIndex(google::protobuf::RpcController*,
                                            const pb::version::GetRawKvIndexRequest* request,
                                            pb::version::GetRawKvIndexResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive GetRawKvIndex Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  pb::coordinator_internal::KvIndexInternal kv_index;
  auto ret = coordinator_control_->GetRawKvIndex(request->key(), kv_index);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "GetRawKvIndex failed: key=" << request->key();
    return;
  }
  DINGO_LOG(INFO) << "GetRawKvIndex success: key=" << request->key();

  auto* resp_kv_index = response->mutable_kvindex();
  resp_kv_index->set_id(kv_index.id());
  resp_kv_index->mutable_mod_revision()->set_main(kv_index.mod_revision().main());
  resp_kv_index->mutable_mod_revision()->set_sub(kv_index.mod_revision().sub());
  for (const auto& generation : kv_index.generations()) {
    auto* new_generation = resp_kv_index->add_generations();
    new_generation->set_verison(generation.verison());
    new_generation->mutable_create_revision()->set_main(generation.create_revision().main());
    new_generation->mutable_create_revision()->set_sub(generation.create_revision().sub());
    for (const auto& revision : generation.revisions()) {
      auto* new_revision = new_generation->add_revisions();
      new_revision->set_main(revision.main());
      new_revision->set_sub(revision.sub());
    }
  }
}

void VersionServiceProtoImpl::GetRawKvRev(google::protobuf::RpcController*,
                                          const pb::version::GetRawKvRevRequest* request,
                                          pb::version::GetRawKvRevResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive GetRawKvRev Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (!request->has_revision()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("revision is empty");
    return;
  }

  pb::coordinator_internal::KvRevInternal kv_rev;
  pb::coordinator_internal::RevisionInternal revision;
  revision.set_main(request->revision().main());
  revision.set_sub(request->revision().sub());
  auto ret = coordinator_control_->GetRawKvRev(revision, kv_rev);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "GetRawKvRev failed: revision=" << request->revision().main() << "."
                     << request->revision().sub();
    return;
  }

  DINGO_LOG(INFO) << "GetRawKvRev success: revision=" << request->revision().main() << "." << request->revision().sub();

  auto* resp_kv_rev = response->mutable_kvrev();
  resp_kv_rev->set_id(kv_rev.id());
  auto* resp_kv = resp_kv_rev->mutable_kv();
  resp_kv->set_id(kv_rev.kv().id());
  resp_kv->set_value(kv_rev.kv().value());
  resp_kv->mutable_mod_revision()->set_main(kv_rev.kv().mod_revision().main());
  resp_kv->mutable_mod_revision()->set_sub(kv_rev.kv().mod_revision().sub());
  resp_kv->mutable_create_revision()->set_main(kv_rev.kv().create_revision().main());
  resp_kv->mutable_create_revision()->set_sub(kv_rev.kv().create_revision().sub());
  resp_kv->set_version(kv_rev.kv().version());
  resp_kv->set_lease(kv_rev.kv().lease());
  resp_kv->set_is_deleted(kv_rev.kv().is_deleted());
}

void VersionServiceProtoImpl::KvRange(google::protobuf::RpcController* /*controller*/,
                                      const pb::version::RangeRequest* request, pb::version::RangeResponse* response,
                                      google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Range Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  int64_t real_limit = 0;
  if (request->limit() <= 0) {
    real_limit = INT64_MAX;
  } else {
    real_limit = request->limit();
  }

  std::vector<pb::version::Kv> kvs;
  uint64_t total_count_in_range = 0;
  auto ret = coordinator_control_->KvRange(request->key(), request->range_end(), real_limit, request->keys_only(),
                                           request->count_only(), kvs, total_count_in_range);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      auto* resp_kv = response->add_kvs();
      resp_kv->CopyFrom(kv);
    }
    response->set_count(total_count_in_range);
    response->set_more(total_count_in_range > real_limit);
  }

  DINGO_LOG(INFO) << "Range success: key=" << request->key() << ", end_key=" << request->range_end()
                  << ", limit=" << request->limit();
}

void VersionServiceProtoImpl::KvPut(google::protobuf::RpcController* controller, const pb::version::PutRequest* request,
                                    pb::version::PutResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive Put Request: IsLeader:" << is_leader << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key_value().key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key_value is empty");
    return;
  }

  // begin to do kv_put
  pb::coordinator_internal::MetaIncrement meta_increment;

  pb::version::Kv prev_kv;
  uint64_t main_revision =
      coordinator_control_->GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment);
  uint64_t sub_revision = 1;
  uint64_t lease_grant_id = 0;

  auto ret = coordinator_control_->KvPut(request->key_value(), request->lease(), request->need_prev_kv(),
                                         request->ignore_value(), request->ignore_lease(), main_revision, sub_revision,
                                         prev_kv, lease_grant_id, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  DINGO_LOG(INFO) << "Put success: key_valuee=" << request->key_value().ShortDebugString()
                  << ", lease_grant_id=" << lease_grant_id << ", revision=" << main_revision << "." << sub_revision;

  if (request->need_prev_kv()) {
    response->mutable_prev_kv()->CopyFrom(prev_kv);
  }
  response->mutable_header()->set_revision(main_revision);

  // prepare for raft process
  auto* meta_closure = new CoordinatorClosure<pb::version::PutRequest, pb::version::PutResponse>(request, response,
                                                                                                 done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(controller), meta_closure, response);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
}

void VersionServiceProtoImpl::KvDeleteRange(google::protobuf::RpcController* /*controller*/,
                                            const pb::version::DeleteRangeRequest* request,
                                            pb::version::DeleteRangeResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->coordinator_control_->IsLeader();
  DINGO_LOG(WARNING) << "Receive DeleteRange Request: IsLeader:" << is_leader
                     << ", Request: " << request->DebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  std::vector<pb::version::Kv> prev_kvs;
  uint64_t main_revision =
      coordinator_control_->GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment);
  uint64_t sub_revision = 1;

  auto ret = coordinator_control_->KvDeleteRange(request->key(), request->range_end(), request->need_prev_kv(),
                                                 main_revision, sub_revision, prev_kvs, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "DeleteRange failed: key=" << request->key() << ", end_key=" << request->range_end();
    return;
  }

  DINGO_LOG(INFO) << "DeleteRange success: key=" << request->key() << ", end_key=" << request->range_end()
                  << ", revision=" << main_revision << "." << sub_revision;

  if (request->need_prev_kv()) {
    for (const auto& kv : prev_kvs) {
      auto* resp_kv = response->add_prev_kvs();
      resp_kv->CopyFrom(kv);
    }
  }
  response->mutable_header()->set_revision(main_revision);

  // prepare for raft process
  auto* meta_closure = new CoordinatorClosure<pb::version::DeleteRangeRequest, pb::version::DeleteRangeResponse>(
      request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(nullptr), meta_closure, response);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
}

}  // namespace dingodb
