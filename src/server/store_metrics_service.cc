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

#include "server/store_metrics_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"

namespace dingodb {

void StoreMetricsImpl::default_method(google::protobuf::RpcController* controller,
                                      const pb::cluster::StoreMetricsRequest* /*request*/,
                                      pb::cluster::StoreMetricsResponse* /*response*/,
                                      google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /store_metrics/<StoreId>\n";  // << butil::describe_resources<Socket>() << '\n';
  } else {
    char* endptr = nullptr;
    int64_t store_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "StoreMetricsId=" << store_id << '\n';

      std::vector<pb::common::StoreMetrics> store_metrics;
      controller_->GetStoreRegionMetrics(store_id, store_metrics);

      if (store_metrics.empty()) {
        os << "StoreMetrics is not found" << '\n';
      } else {
        os << "================ StoreMetrics: ================" << '\n';
        for (const auto& store_metric : store_metrics) {
          os << store_metric.DebugString() << '\n';
        }
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a StoreMetricsId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
