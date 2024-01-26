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
#include <string>
#include <vector>

#include "brpc/closure_guard.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "engine/engine.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/version.pb.h"
#include "server/service_helper.h"

namespace dingodb {

void VersionSetError(pb::error::Error* error, int errcode, const std::string& errmsg) {
  error->set_errcode(static_cast<pb::error::Errno>(errcode));
  error->set_errmsg(errmsg);
}

int VersionService::AddListenableVersion(VersionType type, int64_t id, int64_t version) {
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

int VersionService::DelListenableVersion(VersionType type, int64_t id) {
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

int64_t VersionService::GetCurrentVersion(VersionType type, int64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    return exist->second->version;
  }
  return 0;
}

int64_t VersionService::GetNewVersion(VersionType type, int64_t id, int64_t curr_version, uint wait_seconds) {
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
    int64_t version = version_listener->version;
    version_listener->ref_count--;
    return version;
  }
  return 0;
}

int VersionService::UpdateVersion(VersionType type, int64_t id, int64_t version) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    version_listener->version = version;
    version_listener->condition.notify_all();
    return 0;
  }
  return -1;
}

int64_t VersionService::IncVersion(VersionType type, int64_t id) {
  std::unique_lock<bthread::Mutex> lock(mutex_);
  if (auto exist = version_listeners_[type].find(id); version_listeners_[type].end() != exist) {
    VersionListener* version_listener = exist->second;
    int64_t new_version = version_listener->version++;
    version_listener->condition.notify_all();
    return new_version;
  }
  return 0;
}

VersionService& VersionService::GetInstance() {
  static VersionService service;
  return service;
}

void DoLeaseRevoke(google::protobuf::RpcController* /*controller*/, const pb::version::LeaseRevokeRequest* request,
                   pb::version::LeaseRevokeResponse* response, TrackClosure* done,
                   std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive LeaseRevoke Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive LeaseRevoke Request: " << request->ShortDebugString();

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = kv_control->LeaseRevoke(request->id(), meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseRevoke failed:  lease_id=" << request->id();
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kKvRegionId);
  ctx->SetTracker(done->Tracker());

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "LeaseRevoke failed:  lease_id=" << request->id() << ", error=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  DINGO_LOG(INFO) << "LeaseRevoke success:  lease_id=" << request->id();
}

void DoLeaseGrant(google::protobuf::RpcController* /*controller*/, const pb::version::LeaseGrantRequest* request,
                  pb::version::LeaseGrantResponse* response, TrackClosure* done, std::shared_ptr<KvControl> kv_control,
                  std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive LeaseGrant Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive LeaseGrant Request: " << request->ShortDebugString();

  if (request->ttl() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("ttl is zero or negative");
    return;
  }

  if (request->id() < 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is negative");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // lease grant
  int64_t granted_id = 0;
  int64_t granted_ttl_seconds = 0;

  auto ret = kv_control->LeaseGrant(request->id(), request->ttl(), granted_id, granted_ttl_seconds, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseGrant failed:  lease_id=" << granted_id;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kKvRegionId);
  ctx->SetTracker(done->Tracker());

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "LeaseGrant failed:  lease_id=" << granted_id << ", error=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  response->set_id(granted_id);
  response->set_ttl(granted_ttl_seconds);
  DINGO_LOG(INFO) << "LeaseGrant success:  lease_id=" << granted_id;
}

void DoLeaseRenew(google::protobuf::RpcController* /*controller*/, const pb::version::LeaseRenewRequest* request,
                  pb::version::LeaseRenewResponse* response, TrackClosure* done, std::shared_ptr<KvControl> kv_control,
                  std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive LeaseRenew Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive LeaseRenew Request: " << request->ShortDebugString();

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t ttl_seconds = 0;
  auto ret = kv_control->LeaseRenew(request->id(), ttl_seconds, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseRenew failed:  lease_id=" << request->id();
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kKvRegionId);
  ctx->SetTracker(done->Tracker());

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "LeaseRenew failed:  lease_id=" << request->id() << ", error=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  response->set_id(request->id());
  response->set_ttl(ttl_seconds);
  DINGO_LOG(INFO) << "LeaseRenew success:  lease_id=" << request->id();
}

void DoLeaseQuery(google::protobuf::RpcController* /*controller*/, const pb::version::LeaseQueryRequest* request,
                  pb::version::LeaseQueryResponse* response, google::protobuf::Closure* done,
                  std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();

  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive LeaseTimeToLive Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  if (request->id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED);
    response->mutable_error()->set_errmsg("lease id is zero or negative");
    return;
  }

  int64_t granted_ttl_seconde = 0;
  int64_t remaining_ttl_seconds = 0;
  std::set<std::string> keys;

  auto ret = kv_control->LeaseQuery(request->id(), request->keys(), granted_ttl_seconde, remaining_ttl_seconds, keys);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "LeaseTimeToLive failed:  lease_id=" << request->id();
    return;
  }

  response->set_id(request->id());
  response->set_grantedttl(granted_ttl_seconde);
  response->set_ttl(remaining_ttl_seconds);
  if (!keys.empty()) {
    for (const auto& key : keys) {
      response->add_keys(key);
    }
  }

  DINGO_LOG(DEBUG) << "LeaseTimeToLive success:  lease_id=" << request->id();
}

void DoListLeases(google::protobuf::RpcController* /*controller*/, const pb::version::ListLeasesRequest* request,
                  pb::version::ListLeasesResponse* response, google::protobuf::Closure* done,
                  std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();

  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive ListLeases Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  std::vector<pb::coordinator_internal::LeaseInternal> leases;

  auto ret = kv_control->ListLeases(leases);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "ListLeases failed";
    return;
  }

  if (!leases.empty()) {
    for (const auto& lease : leases) {
      auto* lease_status = response->add_leases();
      lease_status->set_id(lease.id());
    }
  }

  DINGO_LOG(DEBUG) << "ListLeases success";
}

void DoGetRawKvIndex(google::protobuf::RpcController*, const pb::version::GetRawKvIndexRequest* request,
                     pb::version::GetRawKvIndexResponse* response, google::protobuf::Closure* done,
                     std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive GetRawKvIndex Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  pb::coordinator_internal::KvIndexInternal kv_index;
  auto ret = kv_control->GetRawKvIndex(request->key(), kv_index);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "GetRawKvIndex failed: key=" << request->key();
    return;
  }

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

  DINGO_LOG(DEBUG) << "GetRawKvIndex success: key=" << request->key();
}

void DoGetRawKvRev(google::protobuf::RpcController*, const pb::version::GetRawKvRevRequest* request,
                   pb::version::GetRawKvRevResponse* response, google::protobuf::Closure* done,
                   std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive GetRawKvRev Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
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
  auto ret = kv_control->GetRawKvRev(revision, kv_rev);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "GetRawKvRev failed: revision=" << request->revision().main() << "."
                     << request->revision().sub();
    return;
  }

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

  DINGO_LOG(DEBUG) << "GetRawKvRev success: revision=" << request->revision().main() << "."
                   << request->revision().sub();
}

void DoKvRange(google::protobuf::RpcController* /*controller*/, const pb::version::RangeRequest* request,
               pb::version::RangeResponse* response, google::protobuf::Closure* done,
               std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive Range Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
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
  int64_t total_count_in_range = 0;
  bool has_more = false;
  auto ret = kv_control->KvRange(request->key(), request->range_end(), real_limit, request->keys_only(),
                                 request->count_only(), kvs, total_count_in_range, has_more);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      auto* resp_kv = response->add_kvs();
      *resp_kv = kv;
    }
  }
  response->set_more(has_more);
  response->set_count(total_count_in_range);

  DINGO_LOG(DEBUG) << "Range success: key=" << request->key() << ", end_key=" << request->range_end()
                   << ", limit=" << request->limit();
}

void DoKvPut(google::protobuf::RpcController* controller, const pb::version::PutRequest* request,
             pb::version::PutResponse* response, TrackClosure* done, std::shared_ptr<KvControl> kv_control,
             std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive Put Request: IsLeader:" << is_leader << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive Put Request: " << request->ShortDebugString();

  if (request->key_value().key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key_value is empty");
    return;
  }

  // begin to do kv_put
  pb::coordinator_internal::MetaIncrement meta_increment;

  pb::version::Kv prev_kv;
  int64_t lease_grant_id = 0;

  auto ret = kv_control->KvPut(request->key_value(), request->lease(), request->need_prev_kv(), request->ignore_value(),
                               request->ignore_lease(), prev_kv, lease_grant_id, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller*>(controller), nullptr, response);
  ctx->SetRegionId(Constant::kKvRegionId);
  ctx->SetTracker(done->Tracker());

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "KvPut failed: key_valuee=" << request->key_value().ShortDebugString()
                     << ", lease_grant_id=" << lease_grant_id << ", error=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  if (request->need_prev_kv()) {
    *(response->mutable_prev_kv()) = prev_kv;
  }

  DINGO_LOG(INFO) << "KvPut success: key_value=" << request->key_value().ShortDebugString()
                  << ", lease_grant_id=" << lease_grant_id << ", revision=" << response->header().revision();
}

void DoKvDeleteRange(google::protobuf::RpcController* /*controller*/, const pb::version::DeleteRangeRequest* request,
                     pb::version::DeleteRangeResponse* response, TrackClosure* done,
                     std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = kv_control->IsLeader();

  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive DeleteRange Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive DeleteRange Request: " << request->ShortDebugString();

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  std::vector<pb::version::Kv> prev_kvs;
  int64_t deleted_count = 0;

  auto ret = kv_control->KvDeleteRange(request->key(), request->range_end(), request->need_prev_kv(), true,
                                       deleted_count, prev_kvs, meta_increment);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "DeleteRange failed: key=" << request->key() << ", end_key=" << request->range_end();
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(nullptr, nullptr, response);
  ctx->SetRegionId(Constant::kKvRegionId);
  ctx->SetTracker(done->Tracker());

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DeleteRange failed: key=" << request->key() << ", end_key=" << request->range_end()
                     << ", error=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    return;
  }

  if (request->need_prev_kv()) {
    for (const auto& kv : prev_kvs) {
      auto* resp_kv = response->add_prev_kvs();
      *resp_kv = kv;
    }
  }

  response->set_deleted(deleted_count);

  DINGO_LOG(INFO) << "DeleteRange success: key=" << request->key() << ", end_key=" << request->range_end()
                  << ", revision=" << response->header().revision();
}

void DoKvCompaction(google::protobuf::RpcController* /*controller*/, const pb::version::CompactionRequest* request,
                    pb::version::CompactionResponse* response, google::protobuf::Closure* done,
                    std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = kv_control->IsLeader();
  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive DeleteRange Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive Compaction Request: " << request->ShortDebugString();

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  std::vector<std::string> keys_to_compact;
  int64_t total_count_in_range = 0;
  auto ret = kv_control->KvRangeRawKeys(request->key(), request->range_end(), keys_to_compact);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }

  pb::coordinator_internal::RevisionInternal compact_revision;

  if (keys_to_compact.empty()) {
    DINGO_LOG(INFO) << "No keys to compact: key=" << request->key() << ", end_key=" << request->range_end();
    return;
  }
  response->set_compaction_count(keys_to_compact.size());

  std::vector<pb::version::Kv> prev_kvs;

  compact_revision.set_main(request->compact_revision());
  ret = kv_control->KvCompact(keys_to_compact, compact_revision);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    DINGO_LOG(ERROR) << "Compaction failed: key=" << request->key() << ", end_key=" << request->range_end();
    return;
  }

  DINGO_LOG(INFO) << "Compaction success: key=" << request->key() << ", end_key=" << request->range_end();
}

void DoWatch(google::protobuf::RpcController* controller, const pb::version::WatchRequest* request,
             pb::version::WatchResponse* response, google::protobuf::Closure* done,
             std::shared_ptr<KvControl> kv_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = kv_control->IsLeader();

  if (!is_leader) {
    DINGO_LOG(WARNING) << "Receive Watch Request: IsLeader:" << is_leader
                       << ", Request: " << request->ShortDebugString();
    return kv_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "Receive Watch Request: " << request->ShortDebugString();

  if (!request->has_one_time_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("only one_time_request is supported now");
    return;
  }

  if (request->one_time_request().key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  const auto& one_time_req = request->one_time_request();

  bool no_put_event = false;
  bool no_delete_event = false;

  if (one_time_req.filters_size() > 0) {
    for (const auto& filter : one_time_req.filters()) {
      if (filter == pb::version::EventFilterType::NOPUT) {
        no_put_event = true;
      } else if (filter == pb::version::EventFilterType::NODELETE) {
        no_delete_event = true;
      }
    }
  }

  if (no_put_event && no_delete_event) {
    DINGO_LOG(ERROR) << "Watch failed: no put event and no delete event";
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("no put event and no delete event");
  }

  kv_control->OneTimeWatch(one_time_req.key(), one_time_req.start_revision(), no_put_event, no_delete_event,
                           one_time_req.need_prev_kv(), one_time_req.wait_on_not_exist_key(), done_guard.release(),
                           response, static_cast<brpc::Controller*>(controller));
}

void VersionServiceProtoImpl::LeaseGrant(google::protobuf::RpcController* controller,
                                         const pb::version::LeaseGrantRequest* request,
                                         pb::version::LeaseGrantResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(INFO) << "Receive LeaseGrant Request: IsLeader:" << is_leader << ", lease_id: " << request->id()
                  << ", ttl: " << request->ttl();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->ttl() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("ttl is zero or negative");
    return;
  }

  if (request->id() < 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is negative");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoLeaseGrant(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::LeaseRevoke(google::protobuf::RpcController* controller,
                                          const pb::version::LeaseRevokeRequest* request,
                                          pb::version::LeaseRevokeResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive LeaseRevoke Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoLeaseRevoke(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::LeaseRenew(google::protobuf::RpcController* controller,
                                         const pb::version::LeaseRenewRequest* request,
                                         pb::version::LeaseRenewResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive LeaseRenew Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("lease id is zero");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoLeaseRenew(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::LeaseQuery(google::protobuf::RpcController* controller,
                                         const pb::version::LeaseQueryRequest* request,
                                         pb::version::LeaseQueryResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(INFO) << "Receive LeaseTimeToLive Request: IsLeader:" << is_leader
                  << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::ELEASE_NOT_EXISTS_OR_EXPIRED);
    response->mutable_error()->set_errmsg("lease id is zero or negative");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoLeaseQuery(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::ListLeases(google::protobuf::RpcController* controller,
                                         const pb::version::ListLeasesRequest* request,
                                         pb::version::ListLeasesResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive ListLeases Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoListLeases(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::GetRawKvIndex(google::protobuf::RpcController* controller,
                                            const pb::version::GetRawKvIndexRequest* request,
                                            pb::version::GetRawKvIndexResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive GetRawKvIndex Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRawKvIndex(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::GetRawKvRev(google::protobuf::RpcController* controller,
                                          const pb::version::GetRawKvRevRequest* request,
                                          pb::version::GetRawKvRevResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive GetRawKvRev Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (!request->has_revision()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("revision is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetRawKvRev(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::KvRange(google::protobuf::RpcController* controller,
                                      const pb::version::RangeRequest* request, pb::version::RangeResponse* response,
                                      google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive Range Request: IsLeader:" << is_leader << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvRange(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::KvPut(google::protobuf::RpcController* controller, const pb::version::PutRequest* request,
                                    pb::version::PutResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(INFO) << "Receive Put Request: IsLeader:" << is_leader << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key_value().key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key_value is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvPut(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                            const pb::version::DeleteRangeRequest* request,
                                            pb::version::DeleteRangeResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive DeleteRange Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvDeleteRange(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::KvCompaction(google::protobuf::RpcController* controller,
                                           const pb::version::CompactionRequest* request,
                                           pb::version::CompactionResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(WARNING) << "Receive DeleteRange Request: IsLeader:" << is_leader
                     << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (request->key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  // Run in queue.
  auto* svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvCompaction(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::Watch(google::protobuf::RpcController* controller,
                                    const pb::version::WatchRequest* request, pb::version::WatchResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto is_leader = this->IsKvControlLeader();
  DINGO_LOG(INFO) << "Receive Watch Request: IsLeader:" << is_leader << ", Request: " << request->ShortDebugString();

  if (!is_leader) {
    return RedirectResponse(response);
  }

  if (!request->has_one_time_request()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("only one_time_request is supported now");
    return;
  }

  if (request->one_time_request().key().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("key is empty");
    return;
  }

  auto* svr_done = new CoordinatorServiceClosure("Watch", done_guard.release(), request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoWatch(controller, request, response, svr_done, kv_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void DoKvHello(google::protobuf::RpcController* /*controller*/, const pb::version::HelloRequest* request,
               pb::version::HelloResponse* response, TrackClosure* done, std::shared_ptr<KvControl> kv_control,
               std::shared_ptr<Engine> /*raft_engine*/, bool get_memory_info) {
  brpc::ClosureGuard const done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  if (!kv_control->IsLeader()) {
    kv_control->RedirectResponse(response);
  }

  if (get_memory_info) {
    auto* memory_info = response->mutable_memory_info();
    kv_control->GetMemoryInfo(*memory_info);
  }
}

void VersionServiceProtoImpl::Hello(google::protobuf::RpcController* controller,
                                    const pb::version::HelloRequest* request, pb::version::HelloResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  auto* svr_done = new CoordinatorServiceClosure("Hello", done_guard.release(), request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvHello(controller, request, response, svr_done, kv_control_, engine_, request->get_memory_info());
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void VersionServiceProtoImpl::GetMemoryInfo(google::protobuf::RpcController* controller,
                                            const pb::version::HelloRequest* request,
                                            pb::version::HelloResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  auto* svr_done = new CoordinatorServiceClosure("Hello", done_guard.release(), request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvHello(controller, request, response, svr_done, kv_control_, engine_, true);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

}  // namespace dingodb
