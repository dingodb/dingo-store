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

DECLARE_uint64(vector_max_batch_count);
DECLARE_uint64(vector_max_request_size);
DECLARE_bool(enable_async_vector_operation);

UtilServiceImpl::UtilServiceImpl() = default;

void UtilServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

class VectorCalcDistanceTask : public TaskRunnable {
 public:
  VectorCalcDistanceTask(std::shared_ptr<Storage> storage, brpc::Controller* cntl,
                         const dingodb::pb::index::VectorCalcDistanceRequest* request,
                         dingodb::pb::index::VectorCalcDistanceResponse* response, google::protobuf::Closure* done)
      : storage_(storage), cntl_(cntl), request_(request), response_(response), done_(done) {}
  ~VectorCalcDistanceTask() override = default;

  std::string Type() override { return "VECTOR_COUNT"; }

  void Run() override {
    brpc::ClosureGuard done_guard(done_);

    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    butil::Status status =
        storage_->VectorCalcDistance(*request_, distances, result_op_left_vectors, result_op_right_vectors);

    if (!status.ok()) {
      auto* err = response_->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        err->set_errmsg(fmt::format("Not leader({}), please redirect leader({}).", Server::GetInstance()->ServerAddr(),
                                    status.error_str()));
        ServiceHelper::RedirectLeader(status.error_str(), response_);
      }
      DINGO_LOG(ERROR) << fmt::format("VectorScanQuery request_: {} response_: {}", request_->ShortDebugString(),
                                      response_->ShortDebugString());
      return;
    }

    for (const auto& distance : distances) {
      pb::index::VectorDistance dis;
      dis.mutable_internal_distances()->Add(distance.begin(), distance.end());
      response_->mutable_distances()->Add(std::move(dis));  // NOLINT
    }

    response_->mutable_op_left_vectors()->Add(result_op_left_vectors.begin(), result_op_left_vectors.end());
    response_->mutable_op_right_vectors()->Add(result_op_right_vectors.begin(), result_op_right_vectors.end());
  }

 private:
  std::shared_ptr<Storage> storage_;
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const dingodb::pb::index::VectorCalcDistanceRequest* request_;
  dingodb::pb::index::VectorCalcDistanceResponse* response_;
};

void UtilServiceImpl::VectorCalcDistance(google::protobuf::RpcController* controller,
                                         const ::dingodb::pb::index::VectorCalcDistanceRequest* request,
                                         ::dingodb::pb::index::VectorCalcDistanceResponse* response,
                                         ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorCalcDistance request: " << request->ShortDebugString();

  if (request->op_left_vectors_size() > FLAGS_vector_max_batch_count ||
      request->op_right_vectors_size() > FLAGS_vector_max_batch_count || request->op_left_vectors_size() == 0 ||
      request->op_right_vectors_size() == 0) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(pb::error::EILLEGAL_PARAMTETERS));
    err->set_errmsg("op_left_vectors_size or op_right_vectors_size exceed max limit");
    DINGO_LOG(ERROR) << fmt::format("VectorCalcDistance request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (FLAGS_enable_async_vector_operation) {
    auto task = std::make_shared<VectorCalcDistanceTask>(storage_, cntl, request, response, done_guard.release());
    auto ret = storage_->Execute(0, task);
    if (!ret) {
      DINGO_LOG(ERROR) << "VectorCalcDistance execute failed, request: " << request->ShortDebugString();
      auto* err = response->mutable_error();
      err->set_errcode(pb::error::EINTERNAL);
      err->set_errmsg("VectorCalcDistance execute failed");
      return;
    } else {
      std::vector<std::vector<float>> distances;
      std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
      std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

      butil::Status status =
          storage_->VectorCalcDistance(*request, distances, result_op_left_vectors, result_op_right_vectors);

      if (!status.ok()) {
        auto* err = response->mutable_error();
        err->set_errcode(static_cast<Errno>(status.error_code()));
        err->set_errmsg(status.error_str());
        if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
          err->set_errmsg(fmt::format("Not leader({}), please redirect leader({}).",
                                      Server::GetInstance()->ServerAddr(), status.error_str()));
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
  }
}

}  // namespace dingodb
