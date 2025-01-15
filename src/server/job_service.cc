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

#include "server/job_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"

namespace dingodb {

void JobImpl::default_method(google::protobuf::RpcController* controller,
                             const pb::cluster::JobListRequest* /*request*/, pb::cluster::JobListResponse* /*response*/,
                             google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /job/<JobId>\n";  // << butil::describe_resources<Socket>() << '\n';

    butil::FlatMap<int64_t, pb::coordinator::Job> job;
    job.init(100);
    controller_->GetJobAll(job);

    for (auto& [id, job] : job) {
      for (int i = 0; i < job.tasks_size(); ++i) {
        job.mutable_tasks(i)->set_step(i);
      }
    }

    if (job.empty()) {
      os << "No Job now" << '\n';
    } else {
      os << "================ Job: ================" << '\n';
      for (const auto& job : job) {
        os << job.second.DebugString() << '\n';
      }
    }

  } else {
    char* endptr = nullptr;
    int64_t job_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "JobId=" << job_id << '\n';

      pb::coordinator::Job job;
      controller_->GetJob(job_id, job);

      for (int i = 0; i < job.tasks_size(); ++i) {
        job.mutable_tasks(i)->set_step(i);
      }

      if (job.id() == 0) {
        os << "Job is not found" << '\n';
      } else {
        os << "================ Job: ================" << '\n';
        os << job.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a JobId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
