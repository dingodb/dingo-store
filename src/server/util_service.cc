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

#include "server/util_service.h"

#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "server/service_helper.h"

namespace dingodb {

DECLARE_int64(vector_max_batch_count);
DECLARE_int64(vector_max_request_size);
DECLARE_bool(enable_async_vector_operation);

static butil::Status ValidateVectorCalcDistance(const pb::index::VectorCalcDistanceRequest* request) {
  if (request->op_left_vectors_size() * request->op_right_vectors_size() > FLAGS_vector_max_batch_count ||
      request->op_left_vectors_size() == 0 || request->op_right_vectors_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         "op_left_vectors_size or op_right_vectors_size exceed max limit");
  }

  return butil::Status();
}

void DoVectorCalcDistance(StoragePtr storage, google::protobuf::RpcController* controller,
                          const pb::index::VectorCalcDistanceRequest* request,
                          pb::index::VectorCalcDistanceResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  butil::Status status = ValidateVectorCalcDistance(request);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  std::vector<std::vector<float>> distances;
  std::vector<pb::common::Vector> result_op_left_vectors;
  std::vector<pb::common::Vector> result_op_right_vectors;

  status = storage->VectorCalcDistance(*request, distances, result_op_left_vectors, result_op_right_vectors);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  for (const auto& distance : distances) {
    response->add_distances()->mutable_internal_distances()->Add(distance.begin(), distance.end());
  }

  response->mutable_op_left_vectors()->Add(result_op_left_vectors.begin(), result_op_left_vectors.end());
  response->mutable_op_right_vectors()->Add(result_op_right_vectors.begin(), result_op_right_vectors.end());
}

void UtilServiceImpl::VectorCalcDistance(google::protobuf::RpcController* controller,
                                         const ::dingodb::pb::index::VectorCalcDistanceRequest* request,
                                         ::dingodb::pb::index::VectorCalcDistanceResponse* response,
                                         ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (!FLAGS_enable_async_vector_operation) {
    return DoVectorCalcDistance(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  StoragePtr storage = storage_;
  auto task =
      std::make_shared<ServiceTask>([=]() { DoVectorCalcDistance(storage, controller, request, response, svr_done); });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

}  // namespace dingodb
