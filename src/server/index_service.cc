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

#include <climits>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "common/version.h"
#include "engine/storage.h"
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
#include "vector/codec.h"
#include "vector/vector_index_utils.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_int64(vector_max_batch_count, 4096, "vector max batch count in one request");
DEFINE_int64(vector_max_request_size, 33554432, "vector max batch count in one request");
DEFINE_bool(enable_async_vector_search, true, "enable async vector search");
DEFINE_bool(enable_async_vector_count, true, "enable async vector count");
DEFINE_bool(enable_async_vector_operation, true, "enable async vector operation");
DEFINE_bool(enable_async_vector_build, true, "enable async vector build");
DEFINE_bool(enable_async_vector_load, true, "enable async vector load");
DEFINE_bool(enable_async_vector_status, true, "enable async vector status");
DEFINE_bool(enable_async_vector_reset, true, "enable async vector reset");
DEFINE_bool(enable_async_vector_dump, true, "enable async vector dump");

static void IndexRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

DECLARE_int64(max_prewrite_count);
DECLARE_int64(stream_message_max_limit_size);
DECLARE_int64(vector_max_background_task_count);

DECLARE_bool(dingo_log_switch_scalar_speed_up_detail);

IndexServiceImpl::IndexServiceImpl() = default;

bool IndexServiceImpl::IsBackgroundPendingTaskCountExceed() {
  return vector_index_manager_->GetBackgroundPendingTaskCount() > FLAGS_vector_max_background_task_count;
}

static butil::Status ValidateVectorBatchQueryRequest(StoragePtr storage,
                                                     const pb::index::VectorBatchQueryRequest* request,
                                                     store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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
  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->vector_ids()));
}

void DoVectorBatchQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                        const pb::index::VectorBatchQueryRequest* request,
                        pb::index::VectorBatchQueryResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorBatchQueryRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->Range(false);
  ctx->vector_ids = Helper::PbRepeatedToVector(request->vector_ids());
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_vector_data = !request->without_vector_data();
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();
  ctx->ts = request->ts();

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage->VectorBatchQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::VectorBatchQuery(google::protobuf::RpcController* controller,
                                        const pb::index::VectorBatchQueryRequest* request,
                                        pb::index::VectorBatchQueryResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorBatchQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorBatchQuery(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorSearchRequest(StoragePtr storage, const pb::index::VectorSearchRequest* request,
                                                 store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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
    return butil::Status(
        pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
        fmt::format("Param top_n {} * vector_with_ids_size {} is exceed max batch count {} * 10",
                    request->parameter().top_n(), request->vector_with_ids_size(), FLAGS_vector_max_batch_count));
  }

  if (request->parameter().top_n() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param top_n is error");
  }

  if (request->vector_with_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_with_ids is empty");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  auto vector_index_wrapper = region->VectorIndexWrapper();
  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& vector : request->vector_with_ids()) {
    if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
      if (vector.vector().value_type() != pb::common::ValueType::FLOAT) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::FLOAT));
      }
      if (vector.vector().float_values().size() != dimension) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector float dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT ||
               vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
      if (vector.vector().value_type() != pb::common::ValueType::UINT8) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::UINT8));
      }
      if (vector.vector().binary_values().size() != dimension / CHAR_BIT) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector binary dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else {
      return butil::Status(
          pb::error::EILLEGAL_PARAMTETERS,
          "not support vector index type " + pb::common::VectorIndexType_Name(vector_index_wrapper->Type()));
    }
  }
  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoVectorSearch(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorSearchRequest* request, pb::index::VectorSearchResponse* response,
                    TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorSearchRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }
  if (request->parameter().top_n() == 0) {
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorSearchRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->parameter.Swap(mut_request->mutable_parameter());
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  auto scalar_schema = region->ScalarSchema();
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("vector search scalar schema: {}", scalar_schema.ShortDebugString());
  if (0 != scalar_schema.fields_size()) {
    ctx->scalar_schema = scalar_schema;
  }

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
    return;
  }

  for (auto& vector_result : vector_results) {
    *(response->add_batch_results()) = vector_result;
  }
}

void IndexServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                    const pb::index::VectorSearchRequest* request,
                                    pb::index::VectorSearchResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_search) {
    return DoVectorSearch(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorSearch(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteLeastQueue(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorAddRequest(StoragePtr storage, const pb::index::VectorAddRequest* request,
                                              store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s =
        fmt::format("Vector index {}, type is  DISKANN, can not support add. use import instead.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  for (const auto& vector : request->vectors()) {
    if (BAIDU_UNLIKELY(!VectorCodec::IsLegalVectorId(vector.id()))) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param vector id is not allowed to be zero, INT64_MAX or negative");
    }
    if (vector.vector().value_type() == pb::common::ValueType::FLOAT) {
      if (BAIDU_UNLIKELY(vector.vector().float_values().empty())) {
        return butil::Status(pb::error::EVECTOR_EMPTY, "Float Vector is empty");
      }
    } else if (vector.vector().value_type() == pb::common::ValueType::UINT8) {
      if (BAIDU_UNLIKELY(vector.vector().binary_values().empty())) {
        return butil::Status(pb::error::EVECTOR_EMPTY, "Binary Vector is empty");
      }
    } else {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "not support value type " + pb::common::ValueType_Name(vector.vector().value_type()));
    }
  }

  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& vector : request->vectors()) {
    if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
      if (vector.vector().value_type() != pb::common::ValueType::FLOAT) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::FLOAT));
      }
      if (vector.vector().float_values().size() != dimension) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector float dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT ||
               vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
      if (vector.vector().value_type() != pb::common::ValueType::UINT8) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::UINT8));
      }
      if (vector.vector().binary_values().size() != dimension / CHAR_BIT) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector binary dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else {
      return butil::Status(
          pb::error::EILLEGAL_PARAMTETERS,
          "not support vector index type " + pb::common::VectorIndexType_Name(vector_index_wrapper->Type()));
    }
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  auto scalar_schema = region->ScalarSchema();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("vector add scalar schema: {}", scalar_schema.ShortDebugString());

  std::vector<int64_t> vector_ids;
  for (const auto& vector : request->vectors()) {
    if (0 != scalar_schema.fields_size()) {
      status = VectorIndexUtils::ValidateVectorScalarData(scalar_schema, vector.scalar_data());
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    }
    vector_ids.push_back(vector.id());
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoVectorAdd(StoragePtr storage, google::protobuf::RpcController* controller,
                 const pb::index::VectorAddRequest* request, pb::index::VectorAddResponse* response, TrackClosure* done,
                 bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateVectorAddRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<pb::common::VectorWithId> vectors;
  for (const auto& vector : request->vectors()) {
    vectors.push_back(vector);
  }

  status = storage->VectorAdd(ctx, is_sync, vectors, request->is_update());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::VectorAdd(google::protobuf::RpcController* controller,
                                 const pb::index::VectorAddRequest* request, pb::index::VectorAddResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorAdd(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorDeleteRequest(StoragePtr storage, const pb::index::VectorDeleteRequest* request,
                                                 store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s =
        fmt::format("Vector index {}, type is  DISKANN, can not support delete. use import instead.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->ids()));
}

void DoVectorDelete(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorDeleteRequest* request, pb::index::VectorDeleteResponse* response,
                    TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateVectorDeleteRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->VectorDelete(ctx, is_sync, region, Helper::PbRepeatedToVector(request->ids()));
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::VectorDelete(google::protobuf::RpcController* controller,
                                    const pb::index::VectorDeleteRequest* request,
                                    pb::index::VectorDeleteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorDelete(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorGetBorderIdRequest(StoragePtr storage,
                                                      const pb::index::VectorGetBorderIdRequest* request,
                                                      store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }
  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorGetBorderId(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::VectorGetBorderIdRequest* request,
                         pb::index::VectorGetBorderIdResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorGetBorderIdRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  int64_t vector_id = 0;
  status = storage->VectorGetBorderId(region, request->get_min(), request->ts(), vector_id);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->set_id(vector_id);

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::VectorGetBorderId(google::protobuf::RpcController* controller,
                                         const pb::index::VectorGetBorderIdRequest* request,
                                         pb::index::VectorGetBorderIdResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorGetBorderId(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorGetBorderId(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorScanQueryRequest(StoragePtr storage,
                                                    const pb::index::VectorScanQueryRequest* request,
                                                    store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  // for VectorScanQuery, client can do scan from any id, so we don't need to check vector id
  // sdk will merge, sort, limit_cut of all the results for user.
  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorScanQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                       const pb::index::VectorScanQueryRequest* request, pb::index::VectorScanQueryResponse* response,
                       TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorScanQueryRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->Range(false);
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
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();
  ctx->ts = request->ts();

  std::vector<pb::common::VectorWithId> vector_with_ids;
  status = storage->VectorScanQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  for (auto& vector_with_id : vector_with_ids) {
    response->add_vectors()->Swap(&vector_with_id);
  }

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::VectorScanQuery(google::protobuf::RpcController* controller,
                                       const pb::index::VectorScanQueryRequest* request,
                                       pb::index::VectorScanQueryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorScanQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorScanQuery(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorGetRegionMetricsRequest(StoragePtr storage,
                                                           const pb::index::VectorGetRegionMetricsRequest* request,
                                                           store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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
                              pb::index::VectorGetRegionMetricsResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorGetRegionMetricsRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  pb::common::VectorIndexMetrics metrics;
  status = storage->VectorGetRegionMetrics(region, region->VectorIndexWrapper(), metrics);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  *(response->mutable_metrics()) = metrics;
}

void IndexServiceImpl::VectorGetRegionMetrics(google::protobuf::RpcController* controller,
                                              const pb::index::VectorGetRegionMetricsRequest* request,
                                              pb::index::VectorGetRegionMetricsResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorGetRegionMetrics(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorGetRegionMetrics(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorCountRequest(StoragePtr storage, const pb::index::VectorCountRequest* request,
                                                store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector_id_start() > request->vector_id_end()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector_id_start/vector_id_end range is error");
  }
  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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
  pb::common::Range result;

  auto range = region->Range(false);
  auto prefix = region->GetKeyPrefix();
  auto partition_id = region->PartitionId();
  if (start_vector_id == 0) {
    result.set_start_key(range.start_key());
  } else {
    std::string key = VectorCodec::PackageVectorKey(prefix, partition_id, start_vector_id);
    result.set_start_key(key);
  }

  if (end_vector_id == 0) {
    result.set_end_key(range.end_key());
  } else {
    std::string key = VectorCodec::PackageVectorKey(prefix, partition_id, end_vector_id);
    result.set_end_key(key);
  }

  return result;
}

void DoVectorCount(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::VectorCountRequest* request, pb::index::VectorCountResponse* response,
                   TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorCountRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  int64_t count = 0;
  status = storage->VectorCount(region, GenCountRange(region, request->vector_id_start(), request->vector_id_end()),
                                request->ts(), count);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->set_count(count);

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::VectorCount(google::protobuf::RpcController* controller,
                                   const pb::index::VectorCountRequest* request,
                                   pb::index::VectorCountResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_count) {
    return DoVectorCount(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorCount(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorCountMemoryRequest(StoragePtr storage,
                                                      const pb::index::VectorCountMemoryRequest* request,
                                                      store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support count memory.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorCountMemory(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::VectorCountMemoryRequest* request,
                         pb::index::VectorCountMemoryResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorCountMemoryRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorCountMemoryRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  int64_t count = 0;
  status = storage->VectorCountMemory(ctx, count);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->set_count(count);
}

void IndexServiceImpl::VectorCountMemory(google::protobuf::RpcController* controller,
                                         const pb::index::VectorCountMemoryRequest* request,
                                         pb::index::VectorCountMemoryResponse* response,
                                         ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_count) {
    return DoVectorCountMemory(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorCountMemory(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorImportRequestForAdd(StoragePtr storage,
                                                       const pb::index::VectorImportRequest* request,
                                                       store::RegionPtr region) {
  butil::Status status;
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
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  if (vector_index_wrapper->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support import.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  for (const auto& vector : request->vectors()) {
    if (BAIDU_UNLIKELY(!VectorCodec::IsLegalVectorId(vector.id()))) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param vector id is not allowed to be zero, INT64_MAX or negative");
    }

    if (BAIDU_UNLIKELY(vector.vector().float_values().empty())) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& vector : request->vectors()) {
    if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
      if (vector.vector().float_values().size() != dimension) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector float dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else {
      if (vector.vector().binary_values().size() != dimension) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector binary dimension is error, correct dimension is " + std::to_string(dimension));
      }
    }
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  auto scalar_schema = region->ScalarSchema();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("vector add scalar schema: {}", scalar_schema.ShortDebugString());

  std::vector<int64_t> vector_ids;
  for (const auto& vector : request->vectors()) {
    if (0 != scalar_schema.fields_size()) {
      status = VectorIndexUtils::ValidateVectorScalarData(scalar_schema, vector.scalar_data());
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    }
    vector_ids.push_back(vector.id());
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

static butil::Status ValidateVectorImportRequestForDelete(StoragePtr storage,
                                                          const pb::index::VectorImportRequest* request,
                                                          store::RegionPtr region) {
  butil::Status status;

  if (request->delete_ids_size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param ids size {} is exceed max batch count {}", request->delete_ids_size(),
                                     FLAGS_vector_max_batch_count));
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  if (vector_index_wrapper->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support import.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, Helper::PbRepeatedToVector(request->delete_ids()));
}

static butil::Status ValidateVectorImportRequest(StoragePtr storage, const pb::index::VectorImportRequest* request,
                                                 store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vectors_size() == 0 && request->delete_ids_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Vector quantity is empty");
  }

  if (request->vectors_size() > 0 && request->delete_ids_size() > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vectors and delete_ids can not be both set");
  }

  if (request->vectors_size() > 0) {
    return ValidateVectorImportRequestForAdd(storage, request, region);
  } else {
    return ValidateVectorImportRequestForDelete(storage, request, region);
  }
}

void DoVectorImport(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorImportRequest* request, pb::index::VectorImportResponse* response,
                    TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateVectorImportRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<pb::common::VectorWithId> vectors;
  for (const auto& vector : request->vectors()) {
    vectors.push_back(vector);
  }

  std::vector<int64_t> delete_ids;
  for (const auto& id : request->delete_ids()) {
    delete_ids.push_back(id);
  }

  status = storage->VectorImport(ctx, is_sync, vectors, delete_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::VectorImport(google::protobuf::RpcController* controller,
                                    const pb::index::VectorImportRequest* request,
                                    pb::index::VectorImportResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorImport(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorBuildRequest(StoragePtr storage, const pb::index::VectorBuildRequest* request,
                                                store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support build.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorBuild(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::VectorBuildRequest* request, pb::index::VectorBuildResponse* response,
                   TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorBuildRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorBuildRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  pb::common::VectorStateParameter vector_state_parameter;
  const pb::common::VectorBuildParameter& parameter = request->parameter();
  status = storage->VectorBuild(ctx, parameter, request->ts(), vector_state_parameter);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  response->mutable_state()->Swap(&vector_state_parameter);
}

void IndexServiceImpl::VectorBuild(google::protobuf::RpcController* controller,
                                   const pb::index::VectorBuildRequest* request,
                                   pb::index::VectorBuildResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_build) {
    return DoVectorBuild(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorBuild(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorLoadRequest(StoragePtr storage, const pb::index::VectorLoadRequest* request,
                                               store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (!request->parameter().has_diskann()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param diskann not found");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support load.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorLoad(StoragePtr storage, google::protobuf::RpcController* controller,
                  const pb::index::VectorLoadRequest* request, pb::index::VectorLoadResponse* response,
                  TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorLoadRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorLoadRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  pb::common::VectorStateParameter vector_state_parameter;
  const pb::common::VectorLoadParameter& parameter = request->parameter();
  status = storage->VectorLoad(ctx, parameter, vector_state_parameter);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  response->mutable_state()->Swap(&vector_state_parameter);
}

void IndexServiceImpl::VectorLoad(google::protobuf::RpcController* controller,
                                  const pb::index::VectorLoadRequest* request, pb::index::VectorLoadResponse* response,
                                  ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_load) {
    return DoVectorLoad(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorLoad(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorStatusRequest(StoragePtr storage, const pb::index::VectorStatusRequest* request,
                                                 store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support status.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::index::VectorStatusRequest* request, pb::index::VectorStatusResponse* response,
                    TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorStatusRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorStatusRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  pb::common::VectorStateParameter vector_state_parameter;
  pb::error::Error internal_error;
  status = storage->VectorStatus(ctx, vector_state_parameter, internal_error);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    response->mutable_internal_error()->Swap(&internal_error);
    return;
  }

  response->mutable_internal_error()->Swap(&internal_error);
  response->mutable_state()->Swap(&vector_state_parameter);
}

void IndexServiceImpl::VectorStatus(google::protobuf::RpcController* controller,
                                    const pb::index::VectorStatusRequest* request,
                                    pb::index::VectorStatusResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_status) {
    return DoVectorStatus(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorStatus(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorResetRequest(StoragePtr storage, const pb::index::VectorResetRequest* request,
                                                store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support reset.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorReset(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::index::VectorResetRequest* request, pb::index::VectorResetResponse* response,
                   TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorResetRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorResetRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  pb::common::VectorStateParameter vector_state_parameter;
  status = storage->VectorReset(ctx, request->delete_data_file(), vector_state_parameter);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->mutable_state()->Swap(&vector_state_parameter);
}

void IndexServiceImpl::VectorReset(google::protobuf::RpcController* controller,
                                   const pb::index::VectorResetRequest* request,
                                   pb::index::VectorResetResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_reset) {
    return DoVectorReset(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorReset(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorDumpRequest(StoragePtr storage, const pb::index::VectorDumpRequest* request,
                                               store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->Type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = fmt::format("Vector index {}, type is not DISKANN, can not support dump.", region->Id());
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  return ServiceHelper::ValidateIndexRegion(region, {});
}

void DoVectorDump(StoragePtr storage, google::protobuf::RpcController* controller,
                  const pb::index::VectorDumpRequest* request, pb::index::VectorDumpResponse* response,
                  TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorDumpRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::index::VectorDumpRequest*>(request);
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  std::vector<std::string> dump_datas;
  status = storage->VectorDump(ctx, request->dump_all(), dump_datas);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  for (auto& dump_data : dump_datas) {
    response->add_dump_datas(std::move(dump_data));
  }
}

void IndexServiceImpl::VectorDump(google::protobuf::RpcController* controller,
                                  const pb::index::VectorDumpRequest* request, pb::index::VectorDumpResponse* response,
                                  ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (svr_done->GetRegion() == nullptr) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_dump) {
    return DoVectorDump(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorDump(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateVectorSearchDebugRequest(StoragePtr storage,
                                                      const pb::index::VectorSearchDebugRequest* request,
                                                      store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
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

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
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

  auto dimension = vector_index_wrapper->GetDimension();
  for (const auto& vector : request->vector_with_ids()) {
    if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ ||
        vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
      if (vector.vector().value_type() != pb::common::ValueType::FLOAT) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::FLOAT));
      }
      if (vector.vector().float_values().size() != dimension) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector float dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT ||
               vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
      if (vector.vector().value_type() != pb::common::ValueType::UINT8) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector value type is error, correct value type is " +
                                 pb::common::ValueType_Name(pb::common::ValueType::UINT8));
      }
      if (vector.vector().binary_values().size() != dimension / CHAR_BIT) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param vector binary dimension is error, correct dimension is " + std::to_string(dimension));
      }
    } else {
      return butil::Status(
          pb::error::EILLEGAL_PARAMTETERS,
          "not support vector index type " + pb::common::VectorIndexType_Name(vector_index_wrapper->Type()));
    }
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoVectorSearchDebug(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::index::VectorSearchDebugRequest* request,
                         pb::index::VectorSearchDebugResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateVectorSearchDebugRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // Set vector reader context.
  auto ctx = std::make_shared<Engine::VectorReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->vector_index = region->VectorIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->parameter = request->parameter();
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

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
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_vector_search) {
    return DoVectorSearchDebug(storage_, controller, request, response, svr_done);
  }
  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoVectorSearchDebug(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// txn
static butil::Status ValidateTxnGetRequest(const pb::store::TxnGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGetVector(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::store::TxnGetRequest* request, pb::store::TxnGetResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAppliedMaxTs(request->start_ts());
  butil::Status status = ValidateTxnGetRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<std::string> keys;
  auto* mut_request = const_cast<pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::VectorWithId vector_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = vector_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse vector_with_id failed");
          return;
        }
      }

      response->mutable_vector()->Swap(&vector_with_id);
    }
  }
  *response->mutable_txn_result() = txn_result_info;

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnGetVector(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnScanRequestIndex(const pb::store::TxnScanRequest* request, store::RegionPtr region,
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

  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
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

  if (request->has_coprocessor()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Not support scan vector with coprocessor");
  }

  return butil::Status();
}

void DoTxnScanVector(StoragePtr storage, google::protobuf::RpcController* controller,
                     const pb::store::TxnScanRequest* request, pb::store::TxnScanResponse* response,
                     TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAppliedMaxTs(request->start_ts());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequestIndex(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID == static_cast<pb::error::Errno>(status.error_code())) {
      return;
    }
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more = false;
  std::string end_key{};

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);
  status = storage->TxnScan(ctx, request->stream_meta(), request->start_ts(), correction_range, request->limit(),
                            request->key_only(), request->is_reverse(), resolved_locks, txn_result_info, kvs, has_more,
                            end_key, !request->has_coprocessor(), request->coprocessor());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::VectorWithId vector_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = vector_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse vector_with_id failed");
          return;
        }
      }

      response->add_vectors()->Swap(&vector_with_id);
    }
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

void IndexServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnScanVector(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateIndexTxnPessimisticLockRequest(
    StoragePtr storage, const dingodb::pb::store::TxnPessimisticLockRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
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

  if (request->for_update_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "for_update_ts is 0");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
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

void DoIndexTxnPessimisticLock(StoragePtr storage, google::protobuf::RpcController* controller,
                               const dingodb::pb::store::TxnPessimisticLockRequest* request,
                               dingodb::pb::store::TxnPessimisticLockResponse* response, TrackClosure* done,
                               bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateIndexTxnPessimisticLockRequest(storage, request, region);
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
    for (auto& kv : kvs) {
      pb::common::VectorWithId vector_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = vector_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse vector_with_id failed");
          return;
        }
      }

      response->add_vector()->Swap(&vector_with_id);
    }
  }
}

void IndexServiceImpl::TxnPessimisticLock(google::protobuf::RpcController* controller,
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
    DoIndexTxnPessimisticLock(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

butil::Status ValidateTxnPessimisticRollbackRequest(const dingodb::pb::store::TxnPessimisticRollbackRequest* request);

void DoTxnPessimisticRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                              const dingodb::pb::store::TxnPessimisticRollbackRequest* request,
                              dingodb::pb::store::TxnPessimisticRollbackResponse* response, TrackClosure* done,
                              bool is_sync);

void IndexServiceImpl::TxnPessimisticRollback(google::protobuf::RpcController* controller,
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

static butil::Status ValidateIndexTxnPrewriteRequest(StoragePtr storage, const pb::store::TxnPrewriteRequest* request,
                                                     store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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
    if (BAIDU_UNLIKELY(mutation.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  if (BAIDU_UNLIKELY(request->mutations_size() > FLAGS_vector_max_batch_count)) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vectors size {} is exceed max batch count {}", request->mutations_size(),
                                     FLAGS_vector_max_batch_count));
  }

  if (BAIDU_UNLIKELY(request->ByteSizeLong() > FLAGS_vector_max_request_size)) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param vectors size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_vector_max_request_size));
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();

  std::vector<int64_t> vector_ids;
  auto dimension = vector_index_wrapper->GetDimension();

  for (const auto& mutation : request->mutations()) {
    // check vector_id is correctly encoded in key of mutation
    int64_t vector_id = VectorCodec::UnPackageVectorId(mutation.key());

    if (BAIDU_UNLIKELY(!VectorCodec::IsLegalVectorId(vector_id))) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param vector id is not allowed to be zero, INT64_MAX or negative, please check the "
                           "vector_id encoded in mutation key");
    }

    vector_ids.push_back(vector_id);

    // check if vector_id is legal
    const auto& vector = mutation.vector();
    if (mutation.op() == pb::store::Op::Put || mutation.op() == pb::store::PutIfAbsent) {
      if (vector_index_wrapper->IsExceedsMaxElements()) {
        return butil::Status(pb::error::EVECTOR_INDEX_EXCEED_MAX_ELEMENTS,
                             fmt::format("Vector index {} exceeds max elements.", region->Id()));
      }

      if (BAIDU_UNLIKELY(!VectorCodec::IsLegalVectorId(vector_id))) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param  ector id is not allowed to be zero, INT64_MAX or negative, please check the "
                             "vector_id in VectorWithId");
      }

      if (BAIDU_UNLIKELY(vector.id() != vector_id)) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector id in VectorWithId is not equal to vector_id in mutation key, please check "
                             "the mutation key and VectorWithId");
      }

      if (vector.vector().value_type() == pb::common::ValueType::FLOAT) {
        if (BAIDU_UNLIKELY(vector.vector().float_values().empty())) {
          return butil::Status(pb::error::EVECTOR_EMPTY, "Float Vector is empty");
        }
      } else if (vector.vector().value_type() == pb::common::ValueType::UINT8) {
        if (BAIDU_UNLIKELY(vector.vector().binary_values().empty())) {
          return butil::Status(pb::error::EVECTOR_EMPTY, "Binary Vector is empty");
        }
      } else {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "not support value type " + pb::common::ValueType_Name(vector.vector().value_type()));
      }

      // check vector dimension
      if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW ||
          vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT ||
          vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE ||
          vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT ||
          vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ ||
          vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
        if (vector.vector().value_type() != pb::common::ValueType::FLOAT) {
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                               "Param vector value type is error, correct value type is " +
                                   pb::common::ValueType_Name(pb::common::ValueType::FLOAT));
        }
        if (vector.vector().float_values().size() != dimension) {
          return butil::Status(
              pb::error::EILLEGAL_PARAMTETERS,
              "Param vector float dimension is error, correct dimension is " + std::to_string(dimension));
        }
      } else if (vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT ||
                 vector_index_wrapper->Type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
        if (vector.vector().value_type() != pb::common::ValueType::UINT8) {
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                               "Param vector value type is error, correct value type is " +
                                   pb::common::ValueType_Name(pb::common::ValueType::UINT8));
        }
        if (vector.vector().binary_values().size() != dimension / CHAR_BIT) {
          return butil::Status(
              pb::error::EILLEGAL_PARAMTETERS,
              "Param vector binary dimension is error, correct dimension is " + std::to_string(dimension));
        }
      } else {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "not support vector index type " + pb::common::VectorIndexType_Name(vector_index_wrapper->Type()));
      }

      auto scalar_schema = region->ScalarSchema();
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
          << fmt::format("vector txn prewrite scalar schema: {}", scalar_schema.ShortDebugString());
      if (0 != scalar_schema.fields_size()) {
        status = VectorIndexUtils::ValidateVectorScalarData(scalar_schema, vector.scalar_data());
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }
      }

    } else if (mutation.op() == pb::store::Op::Delete || mutation.op() == pb::store::Op::CheckNotExists) {
      if (BAIDU_UNLIKELY(!VectorCodec::IsLegalVectorId(vector_id))) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param vector id is not allowed to be zero, INT64_MAX or negative, please check the "
                             "vector_id encoded in mutation key");
      }

      continue;
    } else {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param op of mutation is error");
    }
  }

  return ServiceHelper::ValidateIndexRegion(region, vector_ids);
}

void DoTxnPrewriteVector(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::store::TxnPrewriteRequest* request, pb::store::TxnPrewriteResponse* response,
                         TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateIndexTxnPrewriteRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& mutation : request->mutations()) {
    keys_for_lock.push_back(std::to_string(mutation.vector().id()));
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::store::Mutation> mutations;
  mutations.reserve(request->mutations_size());
  for (const auto& mutation : request->mutations()) {
    pb::store::Mutation store_mutation;
    store_mutation.set_op(mutation.op());
    store_mutation.set_key(mutation.key());
    store_mutation.set_value(mutation.vector().SerializeAsString());
    mutations.push_back(store_mutation);
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

  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void IndexServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::store::TxnPrewriteRequest* request,
                                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPrewriteVector(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnCommitRequest(const pb::store::TxnCommitRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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

  if (request->keys_size() > FLAGS_vector_max_batch_count) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param vectors size {} is exceed max batch count {}", request->keys_size(),
                                     FLAGS_vector_max_batch_count));
  }

  if (request->ByteSizeLong() > FLAGS_vector_max_request_size) {
    return butil::Status(pb::error::EVECTOR_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param vectors size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_vector_max_request_size));
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  if (region->VectorIndexWrapper()->IsExceedsMaxElements()) {
    return butil::Status(pb::error::EVECTOR_INDEX_EXCEED_MAX_ELEMENTS,
                         fmt::format("Vector index {} exceeds max elements.", region->Id()));
  }

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> vector_ids;
  for (const auto& key : request->keys()) {
    int64_t vector_id = VectorCodec::UnPackageVectorId(key);
    if (vector_id == 0) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param vector id is error");
    }
    vector_ids.push_back(vector_id);
  }

  auto ret1 = ServiceHelper::ValidateIndexRegion(region, vector_ids);
  if (!ret1.ok()) {
    return ret1;
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

  return butil::Status::OK();
}

void DoTxnCommit(StoragePtr storage, google::protobuf::RpcController* controller,
                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response, TrackClosure* done,
                 bool is_sync);

void IndexServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
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

static butil::Status VectorValidateTxnCheckTxnStatusRequest(const pb::store::TxnCheckTxnStatusRequest* request,
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

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return butil::Status();
}

void DoTxnCheckTxnStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::store::TxnCheckTxnStatusRequest* request,
                         pb::store::TxnCheckTxnStatusResponse* response, TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::store::TxnCheckTxnStatusRequest* request,
                                         pb::store::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = VectorValidateTxnCheckTxnStatusRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

static butil::Status VectorValidateTxnResolveLockRequest(const pb::store::TxnResolveLockRequest* request,
                                                         store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
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

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnResolveLock(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::store::TxnResolveLockRequest* request, pb::store::TxnResolveLockResponse* response,
                      TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnResolveLockRequest* request,
                                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = VectorValidateTxnResolveLockRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

static butil::Status ValidateTxnBatchGetRequest(const pb::store::TxnBatchGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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
  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchGetVector(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::store::TxnBatchGetRequest* request, pb::store::TxnBatchGetResponse* response,
                         TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAppliedMaxTs(request->start_ts());
  butil::Status status = ValidateTxnBatchGetRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
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

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::VectorWithId vector_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = vector_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse vector_with_id failed");
          return;
        }
      }

      response->add_vectors()->Swap(&vector_with_id);
    }
  }
  *response->mutable_txn_result() = txn_result_info;

  tracker->SetReadStoreTime();
}

void IndexServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::store::TxnBatchGetRequest* request,
                                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnBatchGetVector(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnBatchRollbackRequest(const pb::store::TxnBatchRollbackRequest* request,
                                                     store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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
  auto status = ServiceHelper::ValidateRegion(region, keys);
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
                        const pb::store::TxnBatchRollbackRequest* request,
                        pb::store::TxnBatchRollbackResponse* response, TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
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

static butil::Status ValidateTxnScanLockRequest(const pb::store::TxnScanLockRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
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

  std::vector<std::string_view> keys;
  keys.push_back(request->start_key());
  keys.push_back(request->end_key());

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScanLock(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::store::TxnScanLockRequest* request, pb::store::TxnScanLockResponse* response,
                   TrackClosure* done);

void IndexServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
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

static butil::Status ValidateTxnHeartBeatRequest(const pb::store::TxnHeartBeatRequest* request,
                                                 store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnHeartBeat(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::store::TxnHeartBeatRequest* request, pb::store::TxnHeartBeatResponse* response,
                    TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
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

static butil::Status VectorValidateTxnGcRequest(const pb::store::TxnGcRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  if (!region->VectorIndexWrapper()->IsReady()) {
    if (region->VectorIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EVECTOR_INDEX_BUILD_ERROR,
                           fmt::format("Vector index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_READY,
                         fmt::format("Vector index {} not ready, please retry.", region->Id()));
  }

  return butil::Status();
}

void DoTxnGc(StoragePtr storage, google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
             pb::store::TxnGcResponse* response, TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = VectorValidateTxnGcRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

static butil::Status ValidateTxnDeleteRangeRequest(const pb::store::TxnDeleteRangeRequest* request,
                                                   store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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
                      const pb::store::TxnDeleteRangeRequest* request, pb::store::TxnDeleteRangeResponse* response,
                      TrackClosure* done, bool is_sync);

void IndexServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
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

void IndexServiceImpl::BackupData(google::protobuf::RpcController* controller,
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
void IndexServiceImpl::ControlConfig(google::protobuf::RpcController* controller,
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

void DoRestoreMeta(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::RestoreMetaRequest* request,
                   dingodb::pb::store::RestoreMetaResponse* response, TrackClosure* done, bool is_sync);

void IndexServiceImpl::RestoreMeta(google::protobuf::RpcController* controller,
                                   const dingodb::pb::store::RestoreMetaRequest* request,
                                   dingodb::pb::store::RestoreMetaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

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

void DoRestoreData(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::RestoreDataRequest* request,
                   dingodb::pb::store::RestoreDataResponse* response, TrackClosure* done, bool is_sync);

void IndexServiceImpl::RestoreData(google::protobuf::RpcController* controller,
                                   const dingodb::pb::store::RestoreDataRequest* request,
                                   dingodb::pb::store::RestoreDataResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

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

static butil::Status ValidateTxnDumpRequest(const pb::store::TxnDumpRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
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
               const pb::store::TxnDumpRequest* request, pb::store::TxnDumpResponse* response, TrackClosure* done);

void IndexServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
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

void DoHello(google::protobuf::RpcController* controller, const dingodb::pb::index::HelloRequest* request,
             dingodb::pb::index::HelloResponse* response, TrackClosure* done, bool is_get_memory_info = false) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

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

void IndexServiceImpl::Hello(google::protobuf::RpcController* controller, const pb::index::HelloRequest* request,
                             pb::index::HelloResponse* response, google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done =
      new ServiceClosure<pb::index::HelloRequest, pb::index::HelloResponse, false>(__func__, done, request, response);

  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done); });

  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void IndexServiceImpl::GetMemoryInfo(google::protobuf::RpcController* controller,
                                     const pb::index::HelloRequest* request, pb::index::HelloResponse* response,
                                     google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done =
      new ServiceClosure<pb::index::HelloRequest, pb::index::HelloResponse, false>(__func__, done, request, response);

  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done, true); });

  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

}  // namespace dingodb
