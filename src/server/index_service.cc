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

#include "butil/status.h"
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
#include "server/util_service.h"
#include "vector/codec.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_uint64(vector_max_batch_count, 1024, "vector max batch count in one request");
DEFINE_uint64(vector_max_request_size, 8388608, "vector max batch count in one request");
DEFINE_bool(enable_async_vector_search, true, "enable async vector search");
DEFINE_bool(enable_async_vector_add, true, "enable async vector add");
DEFINE_bool(enable_async_vector_delete, true, "enable async vector delete");
DEFINE_bool(enable_async_vector_count, true, "enable async vector count");
DEFINE_bool(enable_async_vector_operation, true, "enable async vector operation");

static void IndexRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

DEFINE_uint32(max_prewrite_count, 1024, "max prewrite count");
DECLARE_uint32(max_scan_lock_limit);

IndexServiceImpl::IndexServiceImpl() = default;

static butil::Status ValidateVectorBatchQueryRequest(const pb::index::VectorBatchQueryRequest* request,
                                                     store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_ids is error");
  }

  if (request->vector_ids().size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vector_ids size {} is exceed max batch count {}",
                                     request->vector_ids().size(), FLAGS_vector_max_batch_count));
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->vector_ids()));
}

void DoVectorBatchQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                        const pb::index::VectorBatchQueryRequest* request,
                        pb::index::VectorBatchQueryResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorBatchQueryRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_ids = Helper::PbRepeatedToVector(request->vector_ids());
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_vector_data = !request->without_vector_data();
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage->VectorBatchQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }
}

void IndexServiceImpl::VectorBatchQuery(google::protobuf::RpcController* controller,
                                        const pb::index::VectorBatchQueryRequest* request,
                                        pb::index::VectorBatchQueryResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorBatchQuery", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorBatchQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorBatchQuery(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorSearchRequest(const pb::index::VectorSearchRequest* request,
                                                 store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->parameter().top_n() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_batch_count));
  }

  // we limit the max request size to 4M and max batch count to 1024
  // for response, the limit is 10 times of request, which may be 40M
  // this size is less than the default max message size 64M
  if (request->parameter().top_n() * request->vector_with_ids_size() > FLAGS_vector_max_batch_count * 10) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_batch_count));
  }

  if (request->parameter().top_n() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param top_n is error");
  }

  if (request->vector_with_ids_size() > 0 && request->vector_with_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  std::vector<int64_t> vector_ids;
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

void DoVectorSearch(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorSearchRequest* request, pb::index::VectorSearchResponse* response,
                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorSearchRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range();
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
  status = storage->VectorBatchSearch(ctx, vector_results);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  for (auto& vector_result : vector_results) {
    *(response->add_batch_results()) = vector_result;
  }
}

void IndexServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                    const pb::index::VectorSearchRequest* request,
                                    pb::index::VectorSearchResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorSearch", done, request, response);

  if (!FLAGS_enable_async_vector_search) {
    return DoVectorSearch(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorSearch(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorAddRequest(const pb::index::VectorAddRequest* request, store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vectors().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector quantity is empty");
  }

  if (request->vectors_size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vectors size {} is exceed max batch count {}", request->vectors_size(),
                                     FLAGS_vector_max_batch_count));
  }

  if (request->ByteSizeLong() > FLAGS_vector_max_request_size) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param vectors size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_vector_max_request_size));
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (vector_index_wrapper->IsExceedsMaxElements()) {
    return butil::Status(pb::error::EVECTOR_INDEX_EXCEED_MAX_ELEMENTS,
                         fmt::format("Vector index {} exceeds max elements.", region->Id()));
  }

  for (const auto& vector : request->vectors()) {
    if (vector.id() == 0 || vector.id() == INT64_MAX || vector.id() < 0) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param vector id is not allowed to be zero or NINT64_MAX or netative");
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

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> vector_ids;
  for (const auto& vector : request->vectors()) {
    vector_ids.push_back(vector.id());
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoVectorAdd(StoragePtr storage, google::protobuf::RpcController* controller,
                 const pb::index::VectorAddRequest* request, pb::index::VectorAddResponse* response,
                 google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto status = ValidateVectorAddRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  std::vector<pb::common::VectorWithId> vectors;
  for (const auto& vector : request->vectors()) {
    vectors.push_back(vector);
  }

  status = storage->VectorAdd(ctx, vectors);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::VectorAdd(google::protobuf::RpcController* controller,
                                 const pb::index::VectorAddRequest* request, pb::index::VectorAddResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorAdd", done, request, response);

  if (!FLAGS_enable_async_vector_add) {
    return DoVectorAdd(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorAdd(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorDeleteRequest(const pb::index::VectorDeleteRequest* request,
                                                 store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->ids().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector id quantity is empty");
  }

  if (request->ids_size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param ids size {} is exceed max batch count {}", request->ids_size(),
                                     FLAGS_vector_max_batch_count));
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->ids()));
}

void DoVectorDelete(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorDeleteRequest* request, pb::index::VectorDeleteResponse* response,
                    google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto status = ValidateVectorDeleteRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  if (is_sync) ctx->EnableSyncMode();

  status = storage->VectorDelete(ctx, Helper::PbRepeatedToVector(request->ids()));
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::VectorDelete(google::protobuf::RpcController* controller,
                                    const pb::index::VectorDeleteRequest* request,
                                    pb::index::VectorDeleteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorDelete", done, request, response);

  if (!FLAGS_enable_async_vector_delete) {
    return DoVectorDelete(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorDelete(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorGetBorderIdRequest(const pb::index::VectorGetBorderIdRequest* request,
                                                      store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorGetBorderId(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::VectorGetBorderIdRequest* request,
                         pb::index::VectorGetBorderIdResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorGetBorderIdRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  int64_t vector_id = 0;
  status = storage->VectorGetBorderId(region->Id(), region->Range(), request->get_min(), vector_id);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  response->set_id(vector_id);
}

void IndexServiceImpl::VectorGetBorderId(google::protobuf::RpcController* controller,
                                         const pb::index::VectorGetBorderIdRequest* request,
                                         pb::index::VectorGetBorderIdResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorGetBorderId", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorGetBorderId(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorGetBorderId(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorScanQueryRequest(const pb::index::VectorScanQueryRequest* request,
                                                    store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_id_start() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_id_start is error");
  }

  if (request->max_scan_count() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count cant be 0");
  }

  if (request->max_scan_count() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count is bigger than %ld",
                         FLAGS_vector_max_batch_count);
  }

  // for VectorScanQuery, client can do scan from any id, so we don't need to check vector id
  // sdk will merge, sort, limit_cut of all the results for user.
  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorScanQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                       const pb::index::VectorScanQueryRequest* request, pb::index::VectorScanQueryResponse* response,
                       google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorScanQueryRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->Range();
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
  status = storage->VectorScanQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }
}

void IndexServiceImpl::VectorScanQuery(google::protobuf::RpcController* controller,
                                       const pb::index::VectorScanQueryRequest* request,
                                       pb::index::VectorScanQueryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorScanQuery", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorScanQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorScanQuery(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorGetRegionMetricsRequest(const pb::index::VectorGetRegionMetricsRequest* request,
                                                           store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorGetRegionMetrics(StoragePtr storage, google::protobuf::RpcController* controller,
                              const pb::index::VectorGetRegionMetricsRequest* request,
                              pb::index::VectorGetRegionMetricsResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorGetRegionMetricsRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  pb::common::VectorIndexMetrics metrics;
  status = storage->VectorGetRegionMetrics(region->Id(), region->Range(), region->VectorIndexWrapper(), metrics);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  *(response->mutable_metrics()) = metrics;
}

void IndexServiceImpl::VectorGetRegionMetrics(google::protobuf::RpcController* controller,
                                              const pb::index::VectorGetRegionMetricsRequest* request,
                                              pb::index::VectorGetRegionMetricsResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorGetRegionMetrics", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorGetRegionMetrics(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoVectorGetRegionMetrics(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorCountRequest(const pb::index::VectorCountRequest* request, store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_id_start() > request->vector_id_end()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_id_start/vector_id_end range is error");
  }

  std::vector<int64_t> vector_ids;
  if (request->vector_id_start() != 0) {
    vector_ids.push_back(request->vector_id_start());
  }
  if (request->vector_id_end() != 0) {
    vector_ids.push_back(request->vector_id_end() - 1);
  }
  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

static pb::common::Range GenCountRange(store::RegionPtr region, int64_t start_vector_id,  // NOLINT
                                       int64_t end_vector_id) {                           // NOLINT
  pb::common::Range range;

  if (start_vector_id == 0) {
    range.set_start_key(region->Range().start_key());
  } else {
    std::string key;
    VectorCodec::EncodeVectorKey(region->PartitionId(), start_vector_id, key);
    range.set_start_key(key);
  }

  if (end_vector_id == 0) {
    range.set_end_key(region->Range().end_key());
  } else {
    std::string key;
    VectorCodec::EncodeVectorKey(region->PartitionId(), end_vector_id, key);
    range.set_end_key(key);
  }

  return range;
}

void DoVectorCount(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::VectorCountRequest* request, pb::index::VectorCountResponse* response,
                   google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorCountRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  int64_t count = 0;
  status = storage->VectorCount(region->Id(),
                                GenCountRange(region, request->vector_id_start(), request->vector_id_end()), count);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  response->set_count(count);
}

void IndexServiceImpl::VectorCount(google::protobuf::RpcController* controller,
                                   const pb::index::VectorCountRequest* request,
                                   pb::index::VectorCountResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorCount", done, request, response);

  if (!FLAGS_enable_async_vector_count) {
    return DoVectorCount(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoVectorCount(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateVectorSearchDebugRequest(const pb::index::VectorSearchDebugRequest* request,
                                                      store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector().id() == 0) {
    if (request->vector().vector().float_values_size() == 0 && request->vector().vector().binary_values_size() == 0 &&
        request->vector_with_ids().empty()) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  if (request->parameter().top_n() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_batch_count));
  }

  // we limit the max request size to 4M and max batch count to 1024
  // for response, the limit is 10 times of request, which may be 40M
  // this size is less than the default max message size 64M
  if (request->parameter().top_n() * request->vector_with_ids_size() > FLAGS_vector_max_batch_count * 10) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_vector_max_batch_count));
  }

  if (request->parameter().top_n() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param top_n is error");
  }

  if (request->vector_with_ids_size() > 0 && request->vector_with_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (!vector_index_wrapper->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  std::vector<int64_t> vector_ids;
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

void DoVectorSearchDebug(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::VectorSearchDebugRequest* request,
                         pb::index::VectorSearchDebugResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  butil::Status status = ValidateVectorSearchDebugRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range();
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
  status = storage->VectorBatchSearchDebug(ctx, vector_results, deserialization_id_time_us, scan_scalar_time_us,
                                           search_time_us);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  for (auto& vector_result : vector_results) {
    *(response->add_batch_results()) = vector_result;
  }
  response->set_deserialization_id_time_us(deserialization_id_time_us);
  response->set_scan_scalar_time_us(scan_scalar_time_us);
  response->set_search_time_us(search_time_us);
}

void IndexServiceImpl::VectorSearchDebug(google::protobuf::RpcController* controller,
                                         const pb::index::VectorSearchDebugRequest* request,
                                         pb::index::VectorSearchDebugResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("VectorSearchDebug", done, request, response);

  if (!FLAGS_enable_async_vector_search) {
    return DoVectorSearchDebug(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorSearchDebug(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

// txn
static butil::Status ValidateTxnGetRequest(const pb::index::TxnGetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGet(StoragePtr storage, google::protobuf::RpcController* controller, const pb::index::TxnGetRequest* request,
              pb::index::TxnGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  auto* mut_request = const_cast<pb::index::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));
  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      if (kv.value().empty()) {
        DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {} has empty value", request->ShortDebugString(),
                                        response->ShortDebugString());
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("empty value of vector kv value");
        return;
      }
      pb::common::VectorWithId vector_with_id;
      auto parse_ret = vector_with_id.ParseFromString(kv.value());
      if (!parse_ret) {
        DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {} parse vector_with_id failed",
                                        request->ShortDebugString(), response->ShortDebugString());
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("parse vector_with_id failed");
        return;
      }
      response->mutable_vector()->Swap(&vector_with_id);
    }
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void IndexServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::index::TxnGetRequest* request,
                              pb::index::TxnGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnGet", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnScanRequestIndex(const pb::index::TxnScanRequest* request, store::RegionPtr region,
                                                 const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance()->Id()));
  }

  // check if limit is valid
  if (request->limit() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is exceed vector max batch count");
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!status.ok()) {
    return status;
  }

  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(), req_range);
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
               const pb::index::TxnScanRequest* request, pb::index::TxnScanResponse* response,
               google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequestIndex(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID == static_cast<pb::error::Errno>(status.error_code())) {
      return;
    }
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more;
  std::string end_key;

  auto correction_range = Helper::IntersectRange(region->Range(), uniform_range);
  status = storage->TxnScan(ctx, request->start_ts(), correction_range, request->limit(), request->key_only(),
                            request->is_reverse(), request->disable_coprocessor(), request->coprocessor(),
                            txn_result_info, kvs, has_more, end_key);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      if (kv.value().empty()) {
        DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {} has empty value",
                                        request->ShortDebugString(), response->ShortDebugString());
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("empty value of vector kv value");
        return;
      }
      pb::common::VectorWithId vector_with_id;
      auto parse_ret = vector_with_id.ParseFromString(kv.value());
      if (!parse_ret) {
        DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {} parse vector_with_id failed",
                                        request->ShortDebugString(), response->ShortDebugString());
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("parse vector_with_id failed");
        return;
      }
      response->add_vectors()->Swap(&vector_with_id);
    }
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    response->mutable_txn_result()->CopyFrom(txn_result_info);
  }
  response->set_end_key(end_key);
  response->set_has_more(has_more);
}

void IndexServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::index::TxnScanRequest* request,
                               pb::index::TxnScanResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnScan", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnScan(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnScan(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnPrewriteRequest(const pb::index::TxnPrewriteRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations size is too large, max=1024");
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

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  // Validate region exist.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(request->context().region_id());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (request->mutations_size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vectors size {} is exceed max batch count {}", request->mutations_size(),
                                     FLAGS_vector_max_batch_count));
  }

  if (request->ByteSizeLong() > FLAGS_vector_max_request_size) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param vectors size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_vector_max_request_size));
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

  for (const auto& mutation : request->mutations()) {
    const auto& vector = mutation.vector();
    if (mutation.op() == pb::store::Op::Put) {
      if (vector.id() == 0 || vector.id() == INT64_MAX || vector.id() < 0) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector id is not allowed to be zero or UNINT64_MAX");
      }

      if (vector.vector().float_values().empty()) {
        return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
      }
    } else if (mutation.op() == pb::store::Op::Delete) {
      continue;
    } else {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param op is error");
    }
  }

  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& mutation : request->mutations()) {
    if (mutation.op() == pb::store::Op::Put) {
      const auto& vector = mutation.vector();
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
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> vector_ids;
  for (const auto& mutation : request->mutations()) {
    int64_t vector_id = Helper::DecodeVectorId(mutation.key());
    if (vector_id == 0) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector id is error");
    }
    vector_ids.push_back(vector_id);
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoTxnPrewrite(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::TxnPrewriteRequest* request, pb::index::TxnPrewriteResponse* response,
                   google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnPrewriteRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<pb::index::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  pb::store::TxnResultInfo txn_result_info;
  std::vector<std::string> already_exists;
  int64_t one_pc_commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnPrewrite(ctx, mutations, request->primary_lock(), request->start_ts(), request->lock_ttl(),
                                request->txn_size(), request->try_one_pc(), request->max_commit_ts(), txn_result_info,
                                already_exists, one_pc_commit_ts);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::index::TxnPrewriteRequest* request,
                                   pb::index::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnPrewrite", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnPrewrite(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnPrewrite(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnCommitRequest(const pb::index::TxnCommitRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCommit(StoragePtr storage, google::protobuf::RpcController* controller,
                 const pb::index::TxnCommitRequest* request, pb::index::TxnCommitResponse* response,
                 google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnCommitRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;
  int64_t commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnCommit(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info, commit_ts);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_commit_ts(commit_ts);
}

void IndexServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::index::TxnCommitRequest* request, pb::index::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnCommit", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnCommit(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnCommit(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnCheckTxnStatusRequest(const pb::index::TxnCheckTxnStatusRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_key());
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnCheckTxnStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::TxnCheckTxnStatusRequest* request,
                         pb::index::TxnCheckTxnStatusResponse* response, google::protobuf::Closure* done,
                         bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnCheckTxnStatusRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;
  int64_t commit_ts = 0;
  pb::store::Action action;
  pb::store::LockInfo lock_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnCheckTxnStatus(ctx, request->primary_key(), request->lock_ts(), request->caller_start_ts(),
                                      request->current_ts(), txn_result_info, lock_ttl, commit_ts, action, lock_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::index::TxnCheckTxnStatusRequest* request,
                                         pb::index::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnCheckTxnStatus", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnCheckTxnStatus(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnCheckTxnStatus(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnResolveLockRequest(const pb::index::TxnResolveLockRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys_size() > 0) {
    for (const auto& key : request->keys()) {
      if (key.empty()) {
        return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
      }
      std::vector<std::string_view> keys;
      keys.push_back(key);
      auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
      if (!status.ok()) {
        return status;
      }
    }
  }

  return butil::Status();
}

void DoTxnResolveLock(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::index::TxnResolveLockRequest* request, pb::index::TxnResolveLockResponse* response,
                      google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnResolveLockRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnResolveLock(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void IndexServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::index::TxnResolveLockRequest* request,
                                      pb::index::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnResolveLock", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnResolveLock(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnResolveLock(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnBatchGetRequest(const pb::index::TxnBatchGetRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchGet(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::TxnBatchGetRequest* request, pb::index::TxnBatchGetResponse* response,
                   google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnBatchGetRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      if (kv.value().empty()) {
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("empty value of vector kv value");
        return;
      }
      pb::common::VectorWithId vector_with_id;
      auto parse_ret = vector_with_id.ParseFromString(kv.value());
      if (!parse_ret) {
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
        err->set_errmsg("parse vector_with_id failed");
        return;
      }
      response->add_vectors()->Swap(&vector_with_id);
    }
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void IndexServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::index::TxnBatchGetRequest* request,
                                   pb::index::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnBatchGet", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnBatchGet(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnBatchGet(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnBatchRollbackRequest(const pb::index::TxnBatchRollbackRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                        const pb::index::TxnBatchRollbackRequest* request,
                        pb::index::TxnBatchRollbackResponse* response, google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnBatchRollbackRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchRollback(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void IndexServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                        const pb::index::TxnBatchRollbackRequest* request,
                                        pb::index::TxnBatchRollbackResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnBatchRollback", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnBatchRollback(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnBatchRollback(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnScanLockRequest(const pb::index::TxnScanLockRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->max_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_ts is 0");
  }

  if (request->limit() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is 0");
  }

  if (request->limit() > FLAGS_max_scan_lock_limit) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is too large, max=1024");
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->start_key());

  std::string str(1, '\x01');
  std::string end_key = Helper::StringSubtractRightAlign(request->end_key(), str);

  keys.push_back(end_key);

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScanLock(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::TxnScanLockRequest* request, pb::index::TxnScanLockResponse* response,
                   google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnScanLockRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::LockInfo> locks;

  status = storage->TxnScanLock(ctx, request->max_ts(), request->start_key(), request->limit(), request->end_key(),
                                txn_result_info, locks);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  for (const auto& lock : locks) {
    response->add_locks()->CopyFrom(lock);
  }
}

void IndexServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                   const pb::index::TxnScanLockRequest* request,
                                   pb::index::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnScanLock", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnScanLock(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnScanLock(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnHeartBeatRequest(const pb::index::TxnHeartBeatRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_lock());

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnHeartBeat(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::TxnHeartBeatRequest* request, pb::index::TxnHeartBeatResponse* response,
                    google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnHeartBeatRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage->TxnHeartBeat(ctx, request->primary_lock(), request->start_ts(), request->advise_lock_ttl(),
                                 txn_result_info, lock_ttl);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_lock_ttl(lock_ttl);
}

void IndexServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                    const pb::index::TxnHeartBeatRequest* request,
                                    pb::index::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnHeartBeat", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnHeartBeat(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoTxnHeartBeat(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnGcRequest(const pb::index::TxnGcRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  return butil::Status();
}

void DoTxnGc(StoragePtr storage, google::protobuf::RpcController* controller, const pb::index::TxnGcRequest* request,
             pb::index::TxnGcResponse* response, google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnGcRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  pb::store::TxnResultInfo txn_result_info;
  int64_t lock_ttl = 0;

  status = storage->TxnGc(ctx, request->safe_point_ts(), txn_result_info);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
}

void IndexServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::index::TxnGcRequest* request,
                             pb::index::TxnGcResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnGc", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnGc(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnGc(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnDeleteRangeRequest(const pb::index::TxnDeleteRangeRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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

  return butil::Status();
}

void DoTxnDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::index::TxnDeleteRangeRequest* request, pb::index::TxnDeleteRangeResponse* response,
                      google::protobuf::Closure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  auto status = ValidateTxnDeleteRangeRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  if (is_sync) ctx->EnableSyncMode();

  status = storage->TxnDeleteRange(ctx, request->start_key(), request->end_key());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                      const pb::index::TxnDeleteRangeRequest* request,
                                      pb::index::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnDeleteRange", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnDeleteRange(storage_, controller, request, response, svr_done, false);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>(
      [=]() { DoTxnDeleteRange(storage, controller, request, response, svr_done, true); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

static butil::Status ValidateTxnDumpRequest(const pb::index::TxnDumpRequest* request) {
  // check if region_epoch is match
  auto epoch_ret =
      ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), request->context().region_id());
  if (!epoch_ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("ValidateRegionEpoch failed request: {} ", request->ShortDebugString());
    return epoch_ret;
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
               const pb::index::TxnDumpRequest* request, pb::index::TxnDumpResponse* response,
               google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateTxnDumpRequest(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region_id, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());

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
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      response->mutable_error()->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                                        Server::GetInstance()->ServerAddr(), region_id,
                                                        status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
  }
}

void IndexServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::index::TxnDumpRequest* request,
                               pb::index::TxnDumpResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure("TxnDump", done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoTxnDump(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task = std::make_shared<ServiceTask>([=]() { DoTxnDump(storage, controller, request, response, svr_done); });
  bool ret = worker_set_->ExecuteHashByRegionId(request->context().region_id(), task);
  if (!ret) {
    brpc::ClosureGuard done_guard(done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "Commit execute queue failed");
  }
}

}  // namespace dingodb
