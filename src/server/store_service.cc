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

#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

StoreServiceImpl::StoreServiceImpl() = default;

void StoreServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::store::AddRegionRequest* request,
                                 dingodb::pb::store::AddRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CREATE);
  command->mutable_create_request()->mutable_region_definition()->CopyFrom(request->region());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                    const pb::store::ChangeRegionRequest* request,
                                    pb::store::ChangeRegionResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CHANGE_PEER);
  command->mutable_change_peer_request()->mutable_region_definition()->CopyFrom(request->region());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::DestroyRegionRequest* request,
                                     dingodb::pb::store::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(INFO) << "DestroyRegion request...";

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_DELETE);
  command->mutable_delete_request()->set_region_id(request->region_id());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void StoreServiceImpl::Snapshot(google::protobuf::RpcController* controller, const pb::store::SnapshotRequest* request,
                                pb::store::SnapshotResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_SNAPSHOT);

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

butil::Status ValidateRegion(uint64_t region_id, const std::vector<std::string_view>& keys) {
  // Todo: use double buffer map replace.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);
  // Check is exist region.
  if (!region) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }
  if (region->State() == pb::common::StoreRegionState::NEW) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is new, waiting later");
  }
  if (region->State() == pb::common::StoreRegionState::STANDBY) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is standby, waiting later");
  }
  if (region->State() == pb::common::StoreRegionState::DELETING) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is deleting");
  }
  if (region->State() == pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is deleted");
  }

  auto range = region->Range();
  for (const auto& key : keys) {
    if (range.start_key().compare(key) > 0 || range.end_key().compare(key) < 0) {
      return butil::Status(pb::error::EKEY_OUT_OF_RANGE, "Key out of range");
    }
  }

  return butil::Status();
}

butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->KvGet(ctx, keys, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }
  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
}

butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(key);
  }

  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage_->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
}

butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPut(ctx, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  status = storage_->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPutIfAbsent(ctx, kvs, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage_->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(key);
  }

  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvBatchDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage_->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status KvDeleteRangeParamCheck(const pb::common::RangeWithOptions& range, std::string* real_start_key,
                                      std::string* real_end_key) {
  if (BAIDU_UNLIKELY(range.range().start_key().empty() || range.range().end_key().empty())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("start_key or end_key empty. not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (BAIDU_UNLIKELY(range.range().start_key() > range.range().end_key())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("start_key > end_key  not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");

  } else if (BAIDU_UNLIKELY(range.range().start_key() == range.range().end_key())) {
    if (!range.with_start() || !range.with_end()) {
      DINGO_LOG(ERROR) << butil::StringPrintf(
          "start_key == end_key with_start != true or with_end != true. not support");
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
    }
  }

  // Design constraints We do not allow tables with all 0xFF because there are too many tables and we cannot handle
  // them
  if (Helper::KeyIsEndOfAllTable(range.range().start_key()) || Helper::KeyIsEndOfAllTable(range.range().start_key())) {
    DINGO_LOG(ERROR) << butil::StringPrintf("real_start_key or real_end_key all 0xFF. not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  std::string internal_real_start_key;
  if (!range.with_start()) {
    internal_real_start_key = Helper::StringIncrement(range.range().start_key());
  } else {
    internal_real_start_key = range.range().start_key();
  }

  std::string internal_real_end_key;
  if (range.with_end()) {
    internal_real_end_key = Helper::StringIncrement(range.range().end_key());
  } else {
    internal_real_end_key = range.range().end_key();
  }

  // parameter check again
  if (BAIDU_UNLIKELY(internal_real_start_key > internal_real_end_key)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("real_start_key > real_end_key  not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");

  } else if (BAIDU_UNLIKELY(internal_real_start_key == internal_real_end_key)) {
    if (!range.with_start() || !range.with_end()) {
      DINGO_LOG(ERROR) << butil::StringPrintf(
          "real_start_key == real_end_key with_start != true or with_end != true. not support");
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
    }
  }

  // Design constraints We do not allow tables with all 0xFF because there are too many tables and we cannot handle
  // them
  if (Helper::KeyIsEndOfAllTable(internal_real_start_key) || Helper::KeyIsEndOfAllTable(internal_real_end_key)) {
    DINGO_LOG(ERROR) << butil::StringPrintf("real_start_key or real_end_key all 0xFF. not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range wrong");
  }

  if (real_start_key) {
    *real_start_key = internal_real_start_key;
  }

  if (real_end_key) {
    *real_end_key = internal_real_end_key;
  }

  return butil::Status();
}

butil::Status ValidateKvDeleteRangeRequest(const dingodb::pb::store::KvDeleteRangeRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  std::string real_start_key;
  std::string real_end_key;
  butil::Status s = KvDeleteRangeParamCheck(request->range(), &real_start_key, &real_end_key);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("Helper::KvDeleteRangeParamCheck failed : %s", s.error_str().c_str());
    return s;
  }

  std::vector<std::string_view> keys = {real_start_key, real_end_key};
  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvDeleteRangeRequest(request);
  if (!status.ok()) {
    // Note: The caller requires that if the parameter is wrong, no error will be reported and it will be returned
    // directly.
    if (pb::error::EILLEGAL_PARAMTETERS == static_cast<pb::error::Errno>(status.error_code())) {
      response->set_delete_count(0);
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::OK);
      err->set_errmsg("");
    } else {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
    }

    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvDeleteRangeRequest*>(request);
  status = storage_->KvDeleteRange(ctx, *mut_request->mutable_range());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
  }
}

butil::Status ValidateKvScanBeginRequest(const dingodb::pb::store::KvScanBeginRequest* request) {
  std::string real_start_key;
  std::string real_end_key;
  butil::Status s = KvDeleteRangeParamCheck(request->range(), &real_start_key, &real_end_key);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("Helper::KvDeleteRangeParamCheck failed : %s", s.error_str().c_str());
    return s;
  }

  std::vector<std::string_view> keys = {real_start_key, real_end_key};
  auto status = ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanBeginRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage_->KvScanBegin(ctx, Constant::kStoreDataCF, request->region_id(), request->range(),
                                 request->max_fetch_cnt(), request->key_only(), request->disable_auto_release(),
                                 &scan_id, &kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;
}

butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanContinue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                                      ::dingodb::pb::store::KvScanContinueResponse* response,
                                      ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanContinueRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs);

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }
}

butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanRelease(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                                     ::dingodb::pb::store::KvScanReleaseResponse* response,
                                     ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanReleaseRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanRelease(ctx, request->scan_id());

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }
}

void StoreServiceImpl::Debug(google::protobuf::RpcController* controller,
                             const ::dingodb::pb::store::DebugRequest* request,
                             ::dingodb::pb::store::DebugResponse* response, ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  if (request->type() == pb::store::DebugType::STORE_REGION_META_STAT) {
    auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
    auto regions = store_region_meta->GetAllRegion();

    std::map<std::string, int32_t> state_counts;
    for (auto& region : regions) {
      std::string name = pb::common::StoreRegionState_Name(region->State());
      if (state_counts.find(name) == state_counts.end()) {
        state_counts[name] = 0;
      }
      ++state_counts[name];
    }

    for (auto [name, count] : state_counts) {
      response->mutable_region_meta_stat()->mutable_state_counts()->insert({name, count});
    }

  } else if (request->type() == pb::store::DebugType::STORE_REGION_META_DETAILS) {
    auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
    std::vector<store::RegionPtr> regions;
    if (request->region_ids().empty()) {
      regions = store_region_meta->GetAllRegion();
    } else {
      for (auto region_id : request->region_ids()) {
        auto region = store_region_meta->GetRegion(region_id);
        if (region != nullptr) {
          regions.push_back(region);
        }
      }
    }

    for (auto& region : regions) {
      response->mutable_region_meta_details()->add_regions()->CopyFrom(region->InnerRegion());
    }
  } else if (request->type() == pb::store::DebugType::STORE_REGION_CONTROL_COMMAND) {
    std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
    if (request->region_ids().empty()) {
      commands = Server::GetInstance()->GetRegionCommandManager()->GetAllCommand();
    } else {
      for (auto region_id : request->region_ids()) {
        auto command = Server::GetInstance()->GetRegionCommandManager()->GetCommand(region_id);
        if (command != nullptr) {
          commands.push_back(command);
        }
      }
    }

    for (auto& command : commands) {
      response->mutable_region_control_command()->add_region_cmds()->CopyFrom(*command);
    }
  } else if (request->type() == pb::store::DebugType::STORE_RAFT_META) {
    auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();

    std::vector<StoreRaftMeta::RaftMetaPtr> raft_metas;
    if (request->region_ids().empty()) {
      raft_metas = store_raft_meta->GetAllRaftMeta();
    } else {
      for (auto region_id : request->region_ids()) {
        raft_metas.push_back(store_raft_meta->GetRaftMeta(region_id));
      }
    }

    for (auto& raft_meta : raft_metas) {
      response->mutable_raft_meta()->add_raft_metas()->CopyFrom(*raft_meta);
    }

  } else if (request->type() == pb::store::DebugType::STORE_REGION_EXECUTOR) {
    auto region_ids = Server::GetInstance()->GetRegionController()->GetAllRegion();

    for (auto region_id : region_ids) {
      response->mutable_region_executor()->add_region_ids(region_id);
    }
  }
}
void StoreServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb
