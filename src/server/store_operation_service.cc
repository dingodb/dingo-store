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

#include "server/store_operation_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

void StoreOperationImpl::default_method(google::protobuf::RpcController* controller,
                                        const pb::cluster::StoreOperationRequest* /*request*/,
                                        pb::cluster::StoreOperationResponse* /*response*/,
                                        google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /store_operation/<StoreId>\n";  // << butil::describe_resources<Socket>() << '\n';

    pb::common::StoreMap store_map;
    controller_->GetStoreMap(store_map);

    for (const auto& store : store_map.stores()) {
      pb::coordinator::StoreOperation store_operation;
      controller_->GetStoreOperation(store.id(), store_operation);

      os << "================ StoreOperation: ================" << '\n';
      os << store_operation.DebugString() << '\n';
    }

  } else {
    char* endptr = nullptr;
    int64_t store_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "StoreId=" << store_id << '\n';

      pb::coordinator::StoreOperation store_operation;
      controller_->GetStoreOperation(store_id, store_operation);

      if (store_operation.id() == 0) {
        os << "StoreOperation is not found" << '\n';
      } else {
        os << "================ StoreOperation: ================" << '\n';
        os << store_operation.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a StoreOperationId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
