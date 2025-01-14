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

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "server/job_list_service.h"

namespace dingodb {

void JobListImpl::default_method(google::protobuf::RpcController* controller,
                                 const pb::cluster::JobListRequest* /*request*/,
                                 pb::cluster::JobListResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /job_list/<JobListId>\n";  // << butil::describe_resources<Socket>() << '\n';

    butil::FlatMap<int64_t, pb::coordinator::JobList> job_lists;
    job_lists.init(100);
    controller_->GetJobListAll(job_lists);

    for (auto& [id, job_list] : job_lists) {
      for (int i = 0; i < job_list.tasks_size(); ++i) {
        job_list.mutable_tasks(i)->set_step(i);
      }
    }

    if (job_lists.empty()) {
      os << "No JobList now" << '\n';
    } else {
      os << "================ JobList: ================" << '\n';
      for (const auto& job_list : job_lists) {
        os << job_list.second.DebugString() << '\n';
      }
    }

  } else {
    char* endptr = nullptr;
    int64_t job_list_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "JobListId=" << job_list_id << '\n';

      pb::coordinator::JobList job_list;
      controller_->GetJobList(job_list_id, job_list);

      for (int i = 0; i < job_list.tasks_size(); ++i) {
        job_list.mutable_tasks(i)->set_step(i);
      }

      if (job_list.id() == 0) {
        os << "JobList is not found" << '\n';
      } else {
        os << "================ JobList: ================" << '\n';
        os << job_list.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a JobListId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
