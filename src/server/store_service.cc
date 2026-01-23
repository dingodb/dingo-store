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

#include "server/store_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/latch.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "common/tracker.h"
#include "common/version.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "vector/vector_index.h"

namespace dingodb {

DEFINE_bool(enable_async_store_kvscan, true, "enable async store kvscan");
DEFINE_bool(enable_async_store_operation, true, "enable async store operation");
DECLARE_int64(stream_message_max_limit_size);
DECLARE_int64(max_prewrite_count);

DECLARE_bool(dingo_log_switch_scalar_speed_up_detail);

static void StoreRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

StoreServiceImpl::StoreServiceImpl() = default;

static butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  std::vector<std::string_view> keys = {request->key()};
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvGet(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::KvGetRequest* request, dingodb::pb::store::KvGetResponse* response,
             TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvGetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  ctx->SetTs(request->ts());

  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage->KvGet(ctx, keys, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvGet(storage_, controller, request, response, svr_done);
  });

  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request,
                                               store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(key);
  }

  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchGet(StoragePtr storage, google::protobuf::RpcController* controller,
                  const dingodb::pb::store::KvBatchGetRequest* request,
                  dingodb::pb::store::KvBatchGetResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchGetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  ctx->SetTs(request->ts());

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (request->keys().empty()) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchGet(storage_, controller, request, response, svr_done);
  }
  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvBatchGet(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvPut(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::KvPutRequest* request, dingodb::pb::store::KvPutResponse* response,
             TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateKvPutRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  keys_for_lock.push_back(request->kv().key());

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage->KvPut(ctx, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvPut(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvPut(storage_, controller, request, response, svr_done, true);
  });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request,
                                               store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchPut(StoragePtr storage, google::protobuf::RpcController* controller,
                  const dingodb::pb::store::KvBatchPutRequest* request,
                  dingodb::pb::store::KvBatchPutResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchPutRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& kv : request->kvs()) {
    keys_for_lock.push_back(kv.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  auto kvs = Helper::PbRepeatedToVector(mut_request->mutable_kvs());
  status = storage->KvPut(ctx, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (request->kvs().empty()) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchPut(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvBatchPut(storage_, controller, request, response, svr_done, true);
  });

  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request,
                                                  store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvPutIfAbsent(StoragePtr storage, google::protobuf::RpcController* controller,
                     const pb::store::KvPutIfAbsentRequest* request, pb::store::KvPutIfAbsentResponse* response,
                     TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvPutIfAbsentRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  keys_for_lock.push_back(request->kv().key());

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<bool> key_states;
  auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage->KvPutIfAbsent(ctx, kvs, true, key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }

  if (!key_states.empty()) {
    response->set_key_state(key_states[0]);
  }
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvPutIfAbsent(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvPutIfAbsent(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request,
                                                       store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchPutIfAbsent(StoragePtr storage, google::protobuf::RpcController* controller,
                          const dingodb::pb::store::KvBatchPutIfAbsentRequest* request,
                          dingodb::pb::store::KvBatchPutIfAbsentResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& kv : request->kvs()) {
    keys_for_lock.push_back(kv.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<bool> key_states;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic(),
                                  key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }

  for (const auto& key_state : key_states) {
    response->add_key_states(key_state);
  }
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (request->kvs().empty()) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchPutIfAbsent(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvBatchPutIfAbsent(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request,
                                                  store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(key);
  }

  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchDelete(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvBatchDeleteRequest* request,
                     dingodb::pb::store::KvBatchDeleteResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateKvBatchDeleteRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<bool> key_states;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }

  for (const auto& key_state : key_states) {
    response->add_key_states(key_state);
  }
}

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (request->keys().empty()) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchDelete(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvBatchDelete(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvDeleteRangeRequest(const pb::store::KvDeleteRangeRequest* request,
                                                  store::RegionPtr region, const pb::common::Range& req_range) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvDeleteRangeRequest* request,
                     dingodb::pb::store::KvDeleteRangeResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvDeleteRangeRequest(request, region, uniform_range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    }
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);
  status = storage->KvDeleteRange(ctx, correction_range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvDeleteRange(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvDeleteRange(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvCompareAndSetRequest(const dingodb::pb::store::KvCompareAndSetRequest* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvCompareAndSet(StoragePtr storage, google::protobuf::RpcController* controller,
                       const dingodb::pb::store::KvCompareAndSetRequest* request,
                       dingodb::pb::store::KvCompareAndSetResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateKvCompareAndSetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  auto start_time_us = butil::gettimeofday_us();
  std::vector<std::string> keys_for_lock;
  keys_for_lock.push_back(request->kv().key());

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<bool> key_states;
  status = storage->KvCompareAndSet(ctx, {request->kv()}, {request->expect_value()}, true, key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }

  if (!key_states.empty()) {
    response->set_key_state(key_states[0]);
  }
}

void StoreServiceImpl::KvCompareAndSet(google::protobuf::RpcController* controller,
                                       const pb::store::KvCompareAndSetRequest* request,
                                       pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvCompareAndSet(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvCompareAndSet(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvBatchCompareAndSetRequest(const dingodb::pb::store::KvBatchCompareAndSetRequest* request,
                                                         store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }
  }

  if (request->expect_values().size() != request->kvs().size()) {
    return butil::Status(pb::error::EKEY_EMPTY, "expect_values size !=  kvs size");
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    keys.push_back(kv.key());
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvBatchCompareAndSet(StoragePtr storage, google::protobuf::RpcController* controller,
                            const dingodb::pb::store::KvBatchCompareAndSetRequest* request,
                            dingodb::pb::store::KvBatchCompareAndSetResponse* response, TrackClosure* done,
                            bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateKvBatchCompareAndSetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& kv : request->kvs()) {
    keys_for_lock.push_back(kv.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchCompareAndSetRequest*>(request);

  std::vector<bool> key_states;
  status = storage->KvCompareAndSet(ctx, Helper::PbRepeatedToVector(mut_request->kvs()),
                                    Helper::PbRepeatedToVector(mut_request->expect_values()), request->is_atomic(),
                                    key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }

  for (const auto& key_state : key_states) {
    response->add_key_states(key_state);
  }
}

void StoreServiceImpl::KvBatchCompareAndSet(google::protobuf::RpcController* controller,
                                            const pb::store::KvBatchCompareAndSetRequest* request,
                                            pb::store::KvBatchCompareAndSetResponse* response,
                                            google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (request->kvs().empty()) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvBatchCompareAndSet(storage_, controller, request, response, svr_done, true);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvBatchCompareAndSet(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvScanBeginRequest(const dingodb::pb::store::KvScanBeginRequest* request,
                                                store::RegionPtr region, const pb::common::Range& req_range) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoKvScanBegin(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::KvScanBeginRequest* request,
                   dingodb::pb::store::KvScanBeginResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvScanBeginRequest(request, region, uniform_range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  ctx->SetTs(request->ts());

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage->KvScanBegin(ctx, Constant::kStoreDataCF, region_id, correction_range, request->max_fetch_cnt(),
                                request->key_only(), request->disable_auto_release(), request->disable_coprocessor(),
                                request->coprocessor(), &scan_id, &kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;
}

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanBegin(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanBegin(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request,
                                                   store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

void DoKvScanContinue(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::KvScanContinueRequest* request,
                      dingodb::pb::store::KvScanContinueResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanContinueRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  bool has_more = false;
  status = storage->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs, has_more);

  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }
}

void StoreServiceImpl::KvScanContinue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                                      ::dingodb::pb::store::KvScanContinueResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanContinue(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanContinue(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request,
                                                  store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  return butil::Status();
}

void DoKvScanRelease(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvScanReleaseRequest* request,
                     dingodb::pb::store::KvScanReleaseResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanReleaseRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->KvScanRelease(ctx, request->scan_id());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void StoreServiceImpl::KvScanRelease(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                                     ::dingodb::pb::store::KvScanReleaseResponse* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanRelease(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanRelease(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// txn

static butil::Status ValidateTxnGetRequest(const dingodb::pb::store::TxnGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

static butil::Status ValidateKvScanBeginRequestV2(const dingodb::pb::store::KvScanBeginRequestV2* request,
                                                  store::RegionPtr region, const pb::common::Range& req_range) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}
void DoKvScanBeginV2(StoragePtr storage, google::protobuf::RpcController* controller,
                     const dingodb::pb::store::KvScanBeginRequestV2* request,
                     dingodb::pb::store::KvScanBeginResponseV2* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvScanBeginRequestV2(request, region, uniform_range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    } else {
      DINGO_LOG(ERROR) << fmt::format("error : {} region_id : {} scan_id : {}", status.error_cstr(), region_id,
                                      request->scan_id());
    }
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  ctx->SetTs(request->ts());

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  int64_t scan_id = request->scan_id();

  status = storage->KvScanBeginV2(ctx, Constant::kStoreDataCF, region_id, correction_range, request->max_fetch_cnt(),
                                  request->key_only(), request->disable_auto_release(), !request->has_coprocessor(),
                                  request->coprocessor(), scan_id, &kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  response->set_scan_id(scan_id);
}

void StoreServiceImpl::KvScanBeginV2(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanBeginRequestV2* request,
                                     ::dingodb::pb::store::KvScanBeginResponseV2* response,
                                     ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanBeginV2(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanBeginV2(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvScanContinueRequestV2(const dingodb::pb::store::KvScanContinueRequestV2* request,
                                                     store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  // Ignore scan_id check!
  // if (request->scan_id().empty()) {
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  // }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

void DoKvScanContinueV2(StoragePtr storage, google::protobuf::RpcController* controller,
                        const dingodb::pb::store::KvScanContinueRequestV2* request,
                        dingodb::pb::store::KvScanContinueResponseV2* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanContinueRequestV2(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  bool has_more = false;
  status = storage->KvScanContinueV2(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs, has_more);

  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  response->set_has_more(has_more);
}

void StoreServiceImpl::KvScanContinueV2(::google::protobuf::RpcController* controller,
                                        const ::dingodb::pb::store::KvScanContinueRequestV2* request,
                                        ::dingodb::pb::store::KvScanContinueResponseV2* response,
                                        ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanContinueV2(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanContinueV2(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateKvScanReleaseRequestV2(const dingodb::pb::store::KvScanReleaseRequestV2* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  // Ignore scan_id check!
  // if (request->scan_id().empty()) {
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  // }

  return butil::Status();
}

void DoKvScanReleaseV2(StoragePtr storage, google::protobuf::RpcController* controller,
                       const dingodb::pb::store::KvScanReleaseRequestV2* request,
                       dingodb::pb::store::KvScanReleaseResponseV2* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateKvScanReleaseRequestV2(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->KvScanReleaseV2(ctx, request->scan_id());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void StoreServiceImpl::KvScanReleaseV2(::google::protobuf::RpcController* controller,
                                       const ::dingodb::pb::store::KvScanReleaseRequestV2* request,
                                       ::dingodb::pb::store::KvScanReleaseResponseV2* response,
                                       ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_store_operation) {
    return DoKvScanReleaseV2(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoKvScanReleaseV2(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void DoTxnGet(StoragePtr storage, google::protobuf::RpcController* controller,
              const dingodb::pb::store::TxnGetRequest* request, dingodb::pb::store::TxnGetResponse* response,
              TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  butil::Status status = ValidateTxnGetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }
  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }
  pb::store::TxnResultInfo txn_result_info;

  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckKeys(keys, request->context().isolation_level(), request->start_ts(), resolved_locks,
                        txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            fmt::format("Meet memory lock, please try later"));
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
  *response->mutable_txn_result() = txn_result_info;

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnGet(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnScanRequest(const pb::store::TxnScanRequest* request, store::RegionPtr region,
                                            const pb::common::Range& req_range) {
  if (request->limit() <= 0 && request->stream_meta().limit() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit is invalid");
  }
  if (request->limit() > FLAGS_stream_message_max_limit_size ||
      request->stream_meta().limit() > FLAGS_stream_message_max_limit_size) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit beyond max limit");
  }

  if (request->start_ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param start_ts is invalid");
  }
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScan(StoragePtr storage, google::protobuf::RpcController* controller,
               const dingodb::pb::store::TxnScanRequest* request, dingodb::pb::store::TxnScanResponse* response,
               TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequest(request, region, uniform_range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    }
    return;
  }

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);
  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckRange(correction_range.start_key(), correction_range.end_key(), request->context().isolation_level(),
                         request->start_ts(), resolved_locks, txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            "Meet memory lock, please try later");
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  bool has_more = false;
  std::string end_key{};

  status = storage->TxnScan(ctx, request->stream_meta(), request->start_ts(), correction_range, request->limit(),
                            request->key_only(), request->is_reverse(), resolved_locks, txn_result_info, kvs, has_more,
                            end_key, !request->has_coprocessor(), request->coprocessor());

  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    *response->mutable_txn_result() = txn_result_info;
  }
  response->set_end_key(end_key);
  response->set_has_more(has_more);

  auto stream = ctx->Stream();
  CHECK(stream != nullptr) << fmt::format("[region({})] stream is nullptr.", region_id);

  auto* mut_stream_meta = response->mutable_stream_meta();
  mut_stream_meta->set_stream_id(stream->StreamId());
  mut_stream_meta->set_has_more(has_more);

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnScan(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnPessimisticLockRequest(const dingodb::pb::store::TxnPessimisticLockRequest* request,
                                                       store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("mutations size is too large, max={}", FLAGS_max_prewrite_count));
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->for_update_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "for_update_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());

    if (mutation.value().size() > 8192) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "value size is too large, max=8192");
    }

    if (mutation.op() != pb::store::Op::Lock) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "op is not Lock");
    }
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnPessimisticLock(StoragePtr storage, google::protobuf::RpcController* controller,
                          const dingodb::pb::store::TxnPessimisticLockRequest* request,
                          dingodb::pb::store::TxnPessimisticLockResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnPessimisticLockRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& mutation : request->mutations()) {
    keys_for_lock.push_back(mutation.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  std::vector<pb::common::KeyValue> kvs;

  status = storage->TxnPessimisticLock(ctx, mutations, request->primary_lock(), request->start_ts(),
                                       request->lock_ttl(), request->for_update_ts(), request->return_values(), kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
  if (request->return_values() && !kvs.empty()) {
    for (const auto& kv : kvs) {
      *response->add_kvs() = kv;
    }
  }
}

void StoreServiceImpl::TxnPessimisticLock(google::protobuf::RpcController* controller,
                                          const pb::store::TxnPessimisticLockRequest* request,
                                          pb::store::TxnPessimisticLockResponse* response,
                                          google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPessimisticLock(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnPessimisticRollbackRequest(
    const dingodb::pb::store::TxnPessimisticRollbackRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty");
  }

  if (request->keys_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("keys size is too large, max={}", FLAGS_max_prewrite_count));
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->for_update_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "for_update_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnPessimisticRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                              const dingodb::pb::store::TxnPessimisticRollbackRequest* request,
                              dingodb::pb::store::TxnPessimisticRollbackResponse* response, TrackClosure* done,
                              bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateTxnPessimisticRollbackRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }
  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->TxnPessimisticRollback(ctx, region, request->start_ts(), request->for_update_ts(),
                                           Helper::PbRepeatedToVector(request->keys()));
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnPessimisticRollback(google::protobuf::RpcController* controller,
                                              const pb::store::TxnPessimisticRollbackRequest* request,
                                              pb::store::TxnPessimisticRollbackResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPessimisticRollback(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnPrewriteRequest(const dingodb::pb::store::TxnPrewriteRequest* request,
                                                store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("mutations size is too large, max={}", FLAGS_max_prewrite_count));
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->txn_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "txn_size is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnPrewrite(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnPrewriteRequest* request,
                   dingodb::pb::store::TxnPrewriteResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateTxnPrewriteRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& mutation : request->mutations()) {
    keys_for_lock.push_back(mutation.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  DEFER(region->UnlockKeys(keys_for_lock));
  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  std::map<int64_t, int64_t> for_update_ts_checks;
  for (const auto& for_update_ts_check : request->for_update_ts_checks()) {
    for_update_ts_checks.insert_or_assign(for_update_ts_check.index(), for_update_ts_check.expected_for_update_ts());
  }

  std::map<int64_t, std::string> lock_extra_datas;
  for (const auto& lock_extra_data : request->lock_extra_datas()) {
    lock_extra_datas.insert_or_assign(lock_extra_data.index(), lock_extra_data.extra_data());
  }

  std::vector<int64_t> pessimistic_checks;
  pessimistic_checks.reserve(request->pessimistic_checks_size());
  for (const auto& pessimistic_check : request->pessimistic_checks()) {
    pessimistic_checks.push_back(pessimistic_check);
  }
  std::vector<std::string> secondaries;
  secondaries.reserve(request->secondaries_size());
  if (request->use_async_commit()) {
    for (const auto& secondary : request->secondaries()) {
      secondaries.push_back(secondary);
    }
  }
  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnPrewrite(ctx, region, mutations, request->primary_lock(), request->start_ts(),
                                request->lock_ttl(), request->txn_size(), request->try_one_pc(),
                                request->min_commit_ts(), request->max_commit_ts(), pessimistic_checks,
                                for_update_ts_checks, lock_extra_datas, secondaries);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::store::TxnPrewriteRequest* request,
                                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPrewrite(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnCommitRequest(const dingodb::pb::store::TxnCommitRequest* request,
                                              store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCommit(StoragePtr storage, google::protobuf::RpcController* controller,
                 const dingodb::pb::store::TxnCommitRequest* request, dingodb::pb::store::TxnCommitResponse* response,
                 TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->commit_ts());
  auto status = ValidateTxnCommitRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnCommit(ctx, region, request->start_ts(), request->commit_ts(), keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnCommit(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnCheckTxnStatusRequest(const dingodb::pb::store::TxnCheckTxnStatusRequest* request,
                                                      store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->primary_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_key is empty");
  }

  if (request->lock_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ts is 0");
  }

  if (request->caller_start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "caller_start_ts is 0");
  }

  if (request->current_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "current_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_key());
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCheckTxnStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                         const dingodb::pb::store::TxnCheckTxnStatusRequest* request,
                         dingodb::pb::store::TxnCheckTxnStatusResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  int64_t new_max_ts = request->lock_ts();

  if (request->caller_start_ts() > new_max_ts) {
    new_max_ts = request->caller_start_ts();
  }

  region->SetTxnAccessMaxTs(new_max_ts);
  auto status = ValidateTxnCheckTxnStatusRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  keys_for_lock.push_back(request->primary_key());

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->TxnCheckTxnStatus(ctx, request->primary_key(), request->lock_ts(), request->caller_start_ts(),
                                      request->current_ts(), request->force_sync_commit());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::store::TxnCheckTxnStatusRequest* request,
                                         pb::store::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnCheckTxnStatus(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnCheckSecondaryLocks(const dingodb::pb::store::TxnCheckSecondaryLocksRequest* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0, it's illegal");
  }
  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty, it's illegal");
  }
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    std::vector<std::string_view> keys;
    keys.push_back(key);
    auto status = ServiceHelper::ValidateRegion(region, keys);
    if (!status.ok()) {
      return status;
    }
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCheckSecondaryLocks(StoragePtr storage, google::protobuf::RpcController* controller,
                              const dingodb::pb::store::TxnCheckSecondaryLocksRequest* request,
                              dingodb::pb::store::TxnCheckSecondaryLocksResponse* response, TrackClosure* done,
                              bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  auto status = ValidateTxnCheckSecondaryLocks(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  std::vector<pb::common::KeyValue> kvs;

  status = storage->TxnCheckSecondaryLocks(ctx, region, request->start_ts(), keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::TxnCheckSecondaryLocks(google::protobuf::RpcController* controller,
                                              const pb::store::TxnCheckSecondaryLocksRequest* request,
                                              pb::store::TxnCheckSecondaryLocksResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnCheckSecondaryLocks(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnResolveLockRequest(const dingodb::pb::store::TxnResolveLockRequest* request,
                                                   store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0, it's illegal");
  }

  if (request->commit_ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts < 0, it's illegal");
  }

  if (request->commit_ts() > 0 && request->commit_ts() < request->start_ts()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts < start_ts, it's illegal");
  }

  if (request->keys_size() > 0) {
    for (const auto& key : request->keys()) {
      if (key.empty()) {
        return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
      }
      std::vector<std::string_view> keys;
      keys.push_back(key);
      auto status = ServiceHelper::ValidateRegion(region, keys);
      if (!status.ok()) {
        return status;
      }
    }
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnResolveLock(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::TxnResolveLockRequest* request,
                      dingodb::pb::store::TxnResolveLockResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->commit_ts());
  auto status = ValidateTxnResolveLockRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnResolveLock(ctx, request->start_ts(), request->commit_ts(), keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnResolveLockRequest* request,
                                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnResolveLock(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnBatchGetRequest(const dingodb::pb::store::TxnBatchGetRequest* request,
                                                store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchGet(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnBatchGetRequest* request,
                   dingodb::pb::store::TxnBatchGetResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  butil::Status status = ValidateTxnBatchGetRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckKeys(keys, request->context().isolation_level(), request->start_ts(), resolved_locks,
                        txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            fmt::format("Meet memory lock, please try later"));
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      *response->add_kvs() = kv;
    }
  }
  *response->mutable_txn_result() = txn_result_info;

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::store::TxnBatchGetRequest* request,
                                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnBatchGet(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnBatchRollbackRequest(const dingodb::pb::store::TxnBatchRollbackRequest* request,
                                                     store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                        const dingodb::pb::store::TxnBatchRollbackRequest* request,
                        dingodb::pb::store::TxnBatchRollbackResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnBatchRollbackRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& key : request->keys()) {
    keys_for_lock.push_back(key);
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  status = storage->TxnBatchRollback(ctx, request->start_ts(), keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                        const pb::store::TxnBatchRollbackRequest* request,
                                        pb::store::TxnBatchRollbackResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnBatchRollback(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnScanLockRequest(const dingodb::pb::store::TxnScanLockRequest* request,
                                                store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->max_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_ts is 0");
  }

  if (request->limit() <= 0 && request->stream_meta().limit() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit is invalid");
  }
  if (request->limit() > FLAGS_stream_message_max_limit_size ||
      request->stream_meta().limit() > FLAGS_stream_message_max_limit_size) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit beyond max limit");
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() >= request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key >= end_key");
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScanLock(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::TxnScanLockRequest* request,
                   dingodb::pb::store::TxnScanLockResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnScanLockRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::LockInfo> locks;
  bool has_more = false;
  std::string end_key;
  pb::common::Range range;
  range.set_start_key(request->start_key());
  range.set_end_key(request->end_key());

  status = storage->TxnScanLock(ctx, request->stream_meta(), request->max_ts(), range, request->limit(),
                                txn_result_info, locks, has_more, end_key);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  *response->mutable_txn_result() = txn_result_info;
  for (const auto& lock : locks) {
    *response->add_locks() = lock;
  }

  response->set_has_more(has_more);
  response->set_end_key(end_key);

  auto stream = ctx->Stream();
  CHECK(stream != nullptr) << fmt::format("[region({})] stream is nullptr.", region_id);

  auto* mut_stream_meta = response->mutable_stream_meta();
  mut_stream_meta->set_stream_id(stream->StreamId());
  mut_stream_meta->set_has_more(has_more);

  tracker->SetReadStoreTime();
}

void StoreServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                   const pb::store::TxnScanLockRequest* request,
                                   pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnScanLock(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnHeartBeatRequest(const dingodb::pb::store::TxnHeartBeatRequest* request,
                                                 store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->advise_lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "advise_lock_ttl is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_lock());

  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnHeartBeat(StoragePtr storage, google::protobuf::RpcController* controller,
                    const dingodb::pb::store::TxnHeartBeatRequest* request,
                    dingodb::pb::store::TxnHeartBeatResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnHeartBeatRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  keys_for_lock.push_back(request->primary_lock());

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->TxnHeartBeat(ctx, request->primary_lock(), request->start_ts(), request->advise_lock_ttl());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
    return;
  }
}

void StoreServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                    const pb::store::TxnHeartBeatRequest* request,
                                    pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnHeartBeat(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnGcRequest(const dingodb::pb::store::TxnGcRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGc(StoragePtr storage, google::protobuf::RpcController* controller,
             const dingodb::pb::store::TxnGcRequest* request, dingodb::pb::store::TxnGcResponse* response,
             TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnGcRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->TxnGc(ctx, request->safe_point_ts());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnGc(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnDeleteRangeRequest(const dingodb::pb::store::TxnDeleteRangeRequest* request,
                                                   store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                      const dingodb::pb::store::TxnDeleteRangeRequest* request,
                      dingodb::pb::store::TxnDeleteRangeResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnDeleteRangeRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->TxnDeleteRange(ctx, request->start_key(), request->end_key());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                      const pb::store::TxnDeleteRangeRequest* request,
                                      pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnDeleteRange(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateBackupDataRangeRequest(const dingodb::pb::store::BackupDataRequest* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

static void DoBackupData(StoragePtr storage, google::protobuf::RpcController* controller,
                         const dingodb::pb::store::BackupDataRequest* request,
                         dingodb::pb::store::BackupDataResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();

  auto status = ValidateBackupDataRangeRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check leader if need
  if (request->need_leader()) {
    status = storage->ValidateLeader(region);
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      return;
    }
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->BackupData(ctx, region, request->region_type(), request->backup_ts(), request->backup_tso(),
                               request->storage_path(), request->storage_backend(), request->compression_type(),
                               request->compression_level(), response);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::BackupData(google::protobuf::RpcController* controller,
                                  const dingodb::pb::store::BackupDataRequest* request,
                                  dingodb::pb::store::BackupDataResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoBackupData(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateBackupMetaRangeRequest(const dingodb::pb::store::BackupMetaRequest* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

static void DoBackupMeta(StoragePtr storage, google::protobuf::RpcController* controller,
                         const dingodb::pb::store::BackupMetaRequest* request,
                         dingodb::pb::store::BackupMetaResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();

  auto status = ValidateBackupMetaRangeRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check leader if need
  if (request->need_leader()) {
    status = storage->ValidateLeader(region);
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      return;
    }
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->BackupMeta(ctx, region, request->region_type(), request->backup_ts(), request->backup_tso(),
                               request->storage_path(), request->storage_backend(), request->compression_type(),
                               request->compression_level(), response);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::BackupMeta(google::protobuf::RpcController* controller,
                                  const dingodb::pb::store::BackupMetaRequest* request,
                                  dingodb::pb::store::BackupMetaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoBackupMeta(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static void DoControlConfig(StoragePtr storage, google::protobuf::RpcController* controller,
                            const dingodb::pb::store::ControlConfigRequest* request,
                            dingodb::pb::store::ControlConfigResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetTracker(tracker);

  std::vector<pb::common::ControlConfigVariable> variables;
  for (const auto& variable : request->control_config_variable()) {
    variables.push_back(variable);
  }

  auto status = storage->ControlConfig(ctx, variables, response);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}
void StoreServiceImpl::ControlConfig(google::protobuf::RpcController* controller,
                                     const pb::store::ControlConfigRequest* request,
                                     pb::store::ControlConfigResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure<pb::store::ControlConfigRequest, pb::store::ControlConfigResponse, false>(
      __func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoControlConfig(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateRestoreMetaRequest(const dingodb::pb::store::RestoreMetaRequest* request,
                                                store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidSstMetas(request->storage_backend(), request->sst_metas(), region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoRestoreMeta(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::RestoreMetaRequest* request,
                   dingodb::pb::store::RestoreMetaResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  butil::Status status;

  // check leader if need
  if (request->need_leader()) {
    status = storage->ValidateLeader(region);
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      return;
    }
  }

  status = ValidateRestoreMetaRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& sst_meta : request->sst_metas().backup_data_file_value_sst_metas()) {
    keys_for_lock.push_back(sst_meta.file_name());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->RestoreMeta(ctx, region, request->backup_ts(), request->backup_tso(), request->storage_backend(),
                                request->sst_metas());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::RestoreMeta(google::protobuf::RpcController* controller,
                                   const dingodb::pb::store::RestoreMetaRequest* request,
                                   dingodb::pb::store::RestoreMetaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoRestoreMeta(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateRestoreDataRequest(const dingodb::pb::store::RestoreDataRequest* request,
                                                store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidSstMetas(request->storage_backend(), request->sst_metas(), region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoRestoreData(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::RestoreDataRequest* request,
                   dingodb::pb::store::RestoreDataResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  butil::Status status;

  // check leader if need
  if (request->need_leader()) {
    status = storage->ValidateLeader(region);
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      return;
    }
  }
  status = ValidateRestoreDataRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }
  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& sst_meta : request->sst_metas().backup_data_file_value_sst_metas()) {
    keys_for_lock.push_back(sst_meta.file_name());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, false);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  status = storage->RestoreData(ctx, region, request->backup_ts(), request->backup_tso(), request->storage_backend(),
                                request->sst_metas());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}

void StoreServiceImpl::RestoreData(google::protobuf::RpcController* controller,
                                   const dingodb::pb::store::RestoreDataRequest* request,
                                   dingodb::pb::store::RestoreDataResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoRestoreData(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnDumpRequest(const dingodb::pb::store::TxnDumpRequest* request,
                                            store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() == request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is equal to end_key");
  }

  if (request->start_key().compare(request->end_key()) > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is greater than end_key");
  }

  if (request->end_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_ts is 0");
  }

  return butil::Status();
}

void DoTxnDump(StoragePtr storage, google::protobuf::RpcController* controller,
               const dingodb::pb::store::TxnDumpRequest* request, dingodb::pb::store::TxnDumpResponse* response,
               TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnDumpRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::TxnWriteKey> txn_write_keys;
  std::vector<pb::store::TxnWriteValue> txn_write_values;
  std::vector<pb::store::TxnLockKey> txn_lock_keys;
  std::vector<pb::store::TxnLockValue> txn_lock_values;
  std::vector<pb::store::TxnDataKey> txn_data_keys;
  std::vector<pb::store::TxnDataValue> txn_data_values;
  status = storage->TxnDump(ctx, request->start_key(), request->end_key(), request->start_ts(), request->end_ts(),
                            txn_result_info, txn_write_keys, txn_write_values, txn_lock_keys, txn_lock_values,
                            txn_data_keys, txn_data_values);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  *response->mutable_txn_result() = txn_result_info;
  for (auto& key : txn_write_keys) {
    response->add_write_keys()->Swap(&key);
  }
  for (auto& value : txn_write_values) {
    response->add_write_values()->Swap(&value);
  }
  for (auto& key : txn_lock_keys) {
    response->add_lock_keys()->Swap(&key);
  }
  for (auto& value : txn_lock_values) {
    response->add_lock_values()->Swap(&value);
  }
  for (auto& key : txn_data_keys) {
    response->add_data_keys()->Swap(&key);
  }
  for (auto& value : txn_data_values) {
    response->add_data_values()->Swap(&value);
  }
}

void StoreServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
                               pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnDump(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void DoHello(google::protobuf::RpcController* controller, const dingodb::pb::store::HelloRequest* request,
             dingodb::pb::store::HelloResponse* response, TrackClosure* done, bool is_get_memory_info = false) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  *response->mutable_version_info() = GetVersionInfo();
  if (request->is_just_version_info() && !is_get_memory_info) {
    return;
  }

  auto raft_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_engine == nullptr) {
    return;
  }

  auto regions = Server::GetInstance().GetAllAliveRegion();
  response->set_region_count(regions.size());

  int64_t leader_count = 0;
  for (const auto& region : regions) {
    if (raft_engine->IsLeader(region->Id())) {
      leader_count++;
    }
  }
  response->set_region_leader_count(leader_count);

  if (request->get_region_metrics() || is_get_memory_info) {
    auto store_metrics_manager = Server::GetInstance().GetStoreMetricsManager();
    if (store_metrics_manager == nullptr) {
      return;
    }

    auto store_region_metrics = store_metrics_manager->GetStoreRegionMetrics();
    if (store_region_metrics == nullptr) {
      return;
    }

    auto region_metrics = store_region_metrics->GetAllMetrics();
    for (const auto& region_metrics : region_metrics) {
      auto* new_region_metrics = response->add_region_metrics();
      *new_region_metrics = region_metrics->InnerRegionMetrics();
    }

    auto store_metrics_ptr = store_metrics_manager->GetStoreMetrics();
    if (store_metrics_ptr == nullptr) {
      return;
    }

    auto store_own_metrics = store_metrics_ptr->Metrics();
    *(response->mutable_store_own_metrics()) = store_own_metrics.store_own_metrics();
  }
}

void StoreServiceImpl::Hello(google::protobuf::RpcController* controller, const pb::store::HelloRequest* request,
                             pb::store::HelloResponse* response, google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done =
      new ServiceClosure<pb::store::HelloRequest, pb::store::HelloResponse, false>(__func__, done, request, response);

  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done); });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void StoreServiceImpl::GetMemoryInfo(google::protobuf::RpcController* controller,
                                     const pb::store::HelloRequest* request, pb::store::HelloResponse* response,
                                     google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done =
      new ServiceClosure<pb::store::HelloRequest, pb::store::HelloResponse, false>(__func__, done, request, response);
  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done, true); });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

}  // namespace dingodb
