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
DEFINE_uint64(vector_max_request_size, 8388608, "vector max batch count in one request");

IndexServiceImpl::IndexServiceImpl() = default;

void IndexServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

butil::Status IndexServiceImpl::ValidateVectorBatchQueryQequest(
    const dingodb::pb::index::VectorBatchQueryRequest* request, store::RegionPtr region) {
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

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->vector_ids()));
}

void IndexServiceImpl::VectorBatchQuery(google::protobuf::RpcController* controller,
                                        const dingodb::pb::index::VectorBatchQueryRequest* request,
                                        dingodb::pb::index::VectorBatchQueryResponse* response,
                                        google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorBatchQuery request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorBatchQueryQequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_ids = Helper::PbRepeatedToVector(request->vector_ids());
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_vector_data = !request->without_vector_data();
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage_->VectorBatchQuery(ctx, vector_with_ids);
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

butil::Status IndexServiceImpl::ValidateVectorSearchRequest(const dingodb::pb::index::VectorSearchRequest* request,
                                                            store::RegionPtr region) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
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

  if (request->vector_with_ids_size() > 0 && request->vector_with_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  }

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                  fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  std::vector<uint64_t> vector_ids;
  if (request->vector_with_ids_size() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  } else {
    for (const auto& vector : request->vector_with_ids()) {
      if (vector.id() > 0) {
        vector_ids.push_back(vector.id());
      }
    }
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

// vector
void IndexServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorSearchRequest* request,
                                    dingodb::pb::index::VectorSearchResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorSearch request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorSearchRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->RawRange();
  ctx->parameter = request->parameter();

  if (request->vector_with_ids_size() <= 0) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EILLEGAL_PARAMTETERS);
    err->set_errmsg("Param vector_with_ids is empty");
    return;
  } else {
    for (const auto& vector : request->vector_with_ids()) {
      ctx->vector_with_ids.push_back(vector);
    }
  }

  std::vector<pb::index::VectorWithDistanceResult> vector_results;
  status = storage_->VectorBatchSearch(ctx, vector_results);
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
    *(response->add_batch_results()) = vector_result;
  }
}

butil::Status IndexServiceImpl::ValidateVectorAddRequest(const dingodb::pb::index::VectorAddRequest* request,
                                                         store::RegionPtr region) {
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

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (vector_index_wrapper->IsExceedsMaxElements()) {
    return butil::Status(pb::error::EVECTOR_INDEX_EXCEED_MAX_ELEMENTS,
                         fmt::format("Vector index {} exceeds max elements.", region->Id()));
  }

  for (const auto& vector : request->vectors()) {
    if (vector.id() == 0 || vector.id() == UINT64_MAX) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector id is not allowed to be zero or UNINT64_MAX");
    }

    if (vector.vector().float_values().empty()) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& vector : request->vectors()) {
    if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
      if (vector.vector().float_values().size() != dimension) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else {
      if (vector.vector().binary_values().size() != dimension) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector dimension is error, correct dimension is " + std::to_string(dimension));
      }
    }
  }

  status = ServiceHelper::ValidateSystemCapacity();
  if (!status.ok()) {
    return status;
  }

  std::vector<uint64_t> vector_ids;
  for (const auto& vector : request->vectors()) {
    vector_ids.push_back(vector.id());
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void IndexServiceImpl::VectorAdd(google::protobuf::RpcController* controller,
                                 const dingodb::pb::index::VectorAddRequest* request,
                                 dingodb::pb::index::VectorAddResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorAdd request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorAddRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("ValidateRequest failed request: {} {} {} {} response: {}", request->region_id(),
                                    request->vectors().size(), request->replace_deleted(), request->is_update(),
                                    response->ShortDebugString());
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
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("ValidateRequest failed request: {} {} {} {} response: {}", request->region_id(),
                                    request->vectors().size(), request->replace_deleted(), request->is_update(),
                                    response->ShortDebugString());
  }
}

butil::Status IndexServiceImpl::ValidateVectorDeleteRequest(const dingodb::pb::index::VectorDeleteRequest* request,
                                                            store::RegionPtr region) {
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

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->ids()));
}

void IndexServiceImpl::VectorDelete(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorDeleteRequest* request,
                                    dingodb::pb::index::VectorDeleteResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorDelete request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorDeleteRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  status = storage_->VectorDelete(ctx, Helper::PbRepeatedToVector(request->ids()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status IndexServiceImpl::ValidateVectorGetBorderIdRequest(
    const dingodb::pb::index::VectorGetBorderIdRequest* request, store::RegionPtr region) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void IndexServiceImpl::VectorGetBorderId(google::protobuf::RpcController* controller,
                                         const pb::index::VectorGetBorderIdRequest* request,
                                         pb::index::VectorGetBorderIdResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorGetBorderId request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorGetBorderIdRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  uint64_t vector_id = 0;
  status = storage_->VectorGetBorderId(region->Id(), region->RawRange(), request->get_min(), vector_id);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorGetBorderId request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->set_id(vector_id);
}

butil::Status IndexServiceImpl::ValidateVectorScanQueryRequest(
    const dingodb::pb::index::VectorScanQueryRequest* request, store::RegionPtr region) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_id_start() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_id_start is error");
  }

  if (request->max_scan_count() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count cant be 0");
  }

  if (request->max_scan_count() > FLAGS_vector_max_bactch_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count is bigger than %ld",
                         FLAGS_vector_max_bactch_count);
  }

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateIndexRegion(region, {request->vector_id_start()});
}

void IndexServiceImpl::VectorScanQuery(google::protobuf::RpcController* controller,
                                       const pb::index::VectorScanQueryRequest* request,
                                       pb::index::VectorScanQueryResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorScanQuery request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorScanQueryRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->RawRange();
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_vector_data = !request->without_vector_data();
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();
  ctx->start_id = request->vector_id_start();
  ctx->end_id = request->vector_id_end();
  ctx->is_reverse = request->is_reverse_scan();
  ctx->limit = request->max_scan_count();
  ctx->use_scalar_filter = request->use_scalar_filter();
  ctx->scalar_data_for_filter = request->scalar_for_filter();

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage_->VectorScanQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorScanQuery request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }
}

butil::Status IndexServiceImpl::ValidateVectorGetRegionMetricsRequest(
    const dingodb::pb::index::VectorGetRegionMetricsRequest* request, store::RegionPtr region) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void IndexServiceImpl::VectorGetRegionMetrics(google::protobuf::RpcController* controller,
                                              const pb::index::VectorGetRegionMetricsRequest* request,
                                              pb::index::VectorGetRegionMetricsResponse* response,
                                              google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorGetRegionMetrics request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorGetRegionMetricsRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  pb::common::VectorIndexMetrics metrics;
  status = storage_->VectorGetRegionMetrics(region->Id(), region->RawRange(), region->VectorIndexWrapper(), metrics);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorGetRegionMetrics request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  *(response->mutable_metrics()) = metrics;
}

void IndexServiceImpl::VectorCalcDistance(google::protobuf::RpcController* controller,
                                          const ::dingodb::pb::index::VectorCalcDistanceRequest* request,
                                          ::dingodb::pb::index::VectorCalcDistanceResponse* response,
                                          ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorCalcDistance request: " << request->ShortDebugString();

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetCfName(Constant::kStoreDataCF);

  std::vector<std::vector<float>> distances;
  std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
  std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

  butil::Status status =
      storage_->VectorCalcDistance(ctx, 0, *request, distances, result_op_left_vectors, result_op_right_vectors);

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}), please redirect leader({}).", Server::GetInstance()->ServerAddr(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorScanQuery request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  for (const auto& distance : distances) {
    pb::index::VectorDistance dis;
    dis.mutable_internal_distances()->Add(distance.begin(), distance.end());
    response->mutable_distances()->Add(std::move(dis));  // NOLINT
  }

  response->mutable_op_left_vectors()->Add(result_op_left_vectors.begin(), result_op_left_vectors.end());
  response->mutable_op_right_vectors()->Add(result_op_right_vectors.begin(), result_op_right_vectors.end());
}

butil::Status IndexServiceImpl::ValidateVectorSearchDebugRequest(
    const dingodb::pb::index::VectorSearchDebugRequest* request, store::RegionPtr region) {
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

  if (request->vector_with_ids_size() > 0 && request->vector_with_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  }

  auto status = storage_->ValidateLeader(request->region_id());
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  std::vector<uint64_t> vector_ids;
  if (request->vector_with_ids_size() <= 0) {
    if (request->vector().id() > 0) {
      vector_ids.push_back(request->vector().id());
    }
  } else {
    for (const auto& vector : request->vector_with_ids()) {
      if (vector.id() > 0) {
        vector_ids.push_back(vector.id());
      }
    }
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void IndexServiceImpl::VectorSearchDebug(google::protobuf::RpcController* controller,
                                         const pb::index::VectorSearchDebugRequest* request,
                                         pb::index::VectorSearchDebugResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorSearch request: " << request->ShortDebugString();

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->region_id());
  if (region == nullptr) {
    auto* err = response->mutable_error();
    err->set_errcode(pb::error::EREGION_NOT_FOUND);
    err->set_errmsg(fmt::format("Not found region {} at server {}", request->region_id(), Server::GetInstance()->Id()));
    return;
  }

  // Validate request parameter.
  butil::Status status = ValidateVectorSearchDebugRequest(request, region);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(WARNING) << fmt::format("ValidateRequest failed request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->RawRange();
  ctx->parameter = request->parameter();

  if (request->vector_with_ids_size() <= 0) {
    ctx->vector_with_ids.push_back(request->vector());
  } else {
    for (const auto& vector : request->vector_with_ids()) {
      ctx->vector_with_ids.push_back(vector);
    }
  }

  int64_t deserialization_id_time_us = 0;
  int64_t scan_scalar_time_us = 0;
  int64_t search_time_us = 0;
  std::vector<pb::index::VectorWithDistanceResult> vector_results;
  status = storage_->VectorBatchSearchDebug(ctx, vector_results, deserialization_id_time_us, scan_scalar_time_us,
                                            search_time_us);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("VectorSearch request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  for (auto& vector_result : vector_results) {
    *(response->add_batch_results()) = vector_result;
  }
  response->set_deserialization_id_time_us(deserialization_id_time_us);
  response->set_scan_scalar_time_us(scan_scalar_time_us);
  response->set_search_time_us(search_time_us);
}

}  // namespace dingodb
