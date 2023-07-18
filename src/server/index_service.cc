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

#include "server/index_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/context.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_uint64(vector_max_bactch_count, 1024, "vector max batch count in one request");
DEFINE_uint64(vector_max_request_size, 4194304, "vector max batch count in one request");

IndexServiceImpl::IndexServiceImpl() = default;

butil::Status ValidateVectorBatchQueryQequest(const dingodb::pb::index::VectorBatchQueryRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_ids is error");
  }

  if (request->vector_ids().size() > FLAGS_vector_max_bactch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vector_ids size {} is exceed max batch count {}",
                                     request->vector_ids().size(), FLAGS_vector_max_bactch_count));
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorBatchQuery(google::protobuf::RpcController* controller,
                                        const dingodb::pb::index::VectorBatchQueryRequest* request,
                                        dingodb::pb::index::VectorBatchQueryResponse* response,
                                        google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorBatchQuery request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorBatchQueryQequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("VectorBatchQuery request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage_->VectorBatchQuery(ctx, Helper::PbRepeatedToVector(request->vector_ids()),
                                      (!request->without_vector_data()), request->with_scalar_data(),
                                      Helper::PbRepeatedToVector(request->selected_keys()), vector_with_ids);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorBatchQuery request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }
}

butil::Status ValidateVectorSearchRequest(const dingodb::pb::index::VectorSearchRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector().id() == 0) {
    if (request->vector().vector().float_values_size() == 0 && request->vector().vector().binary_values_size() == 0 &&
        request->vector_with_ids().empty()) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  if (request->parameter().top_n() > FLAGS_vector_max_bactch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_bactch_count));
  }

  // we limit the max request size to 4M and max batch count to 1024
  // for response, the limit is 10 times of request, which may be 40M
  // this size is less than the default max message size 64M
  if (request->parameter().top_n() * request->vector_with_ids_size() > FLAGS_vector_max_bactch_count * 10) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_bactch_count));
  }

  if (request->parameter().top_n() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param top_n is error");
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

// vector
void IndexServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorSearchRequest* request,
                                    dingodb::pb::index::VectorSearchResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorSearch request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorSearchRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("VectorSearch request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  if (request->vector_with_ids_size() <= 0) {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(request->vector());

    std::vector<pb::index::VectorWithDistanceResult> vector_results;

    status = storage_->VectorBatchSearch(ctx, vector_with_ids, request->parameter(), vector_results);
    if (!status.ok()) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg("Not leader, please redirect leader.");
        ServiceHelper::RedirectLeader(status.error_str(), response);
      }
      DINGO_LOG(ERROR) << fmt::format("VectorSearch request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
      return;
    }

    for (auto& vector_result : vector_results) {
      response->add_batch_results()->CopyFrom(vector_result);
    }
  } else {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (const auto& vector : request->vector_with_ids()) {
      vector_with_ids.push_back(vector);
    }

    std::vector<pb::index::VectorWithDistanceResult> vector_results;

    status = storage_->VectorBatchSearch(ctx, vector_with_ids, request->parameter(), vector_results);
    if (!status.ok()) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg("Not leader, please redirect leader.");
        ServiceHelper::RedirectLeader(status.error_str(), response);
      }
      DINGO_LOG(ERROR) << fmt::format("VectorSearch request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
      return;
    }

    for (auto& vector_result : vector_results) {
      response->add_batch_results()->CopyFrom(vector_result);
    }
  }
}

butil::Status ValidateVectorAddRequest(const dingodb::pb::index::VectorAddRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vectors().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector quantity is empty");
  }

  if (request->vectors_size() > FLAGS_vector_max_bactch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vectors size {} is exceed max batch count {}", request->vectors_size(),
                                     FLAGS_vector_max_bactch_count));
  }

  if (request->ByteSizeLong() > FLAGS_vector_max_request_size) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param vectors size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_vector_max_request_size));
  }

  for (const auto& vector : request->vectors()) {
    if (vector.vector().float_values().empty()) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorAdd(google::protobuf::RpcController* controller,
                                 const dingodb::pb::index::VectorAddRequest* request,
                                 dingodb::pb::index::VectorAddResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorAdd request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorAddRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::VectorWithId> vectors;
  for (const auto& vector : request->vectors()) {
    vectors.push_back(vector);
  }

  status = storage_->VectorAdd(ctx, vectors);
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

butil::Status ValidateVectorDeleteRequest(const dingodb::pb::index::VectorDeleteRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->ids().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector id quantity is empty");
  }

  if (request->ids_size() > FLAGS_vector_max_bactch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param ids size {} is exceed max batch count {}", request->ids_size(),
                                     FLAGS_vector_max_bactch_count));
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorDelete(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorDeleteRequest* request,
                                    dingodb::pb::index::VectorDeleteResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorDelete request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<uint64_t> ids;
  for (auto id : request->ids()) {
    ids.push_back(id);
  }

  status = storage_->VectorDelete(ctx, ids);
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

butil::Status ValidateVectorGetBorderIdRequest(const dingodb::pb::index::VectorGetBorderIdRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorGetBorderId(google::protobuf::RpcController* controller,
                                         const pb::index::VectorGetBorderIdRequest* request,
                                         pb::index::VectorGetBorderIdResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorGetBorderId request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorGetBorderIdRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("VectorGetBorderId request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  uint64_t border_id = 0;
  status = storage_->VectorGetBorderId(ctx, border_id, request->get_min());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorGetBorderId request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->set_id(border_id);
}

void IndexServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb
