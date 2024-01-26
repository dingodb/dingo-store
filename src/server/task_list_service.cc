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

#include "server/task_list_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"

namespace dingodb {

void TaskListImpl::default_method(google::protobuf::RpcController* controller,
                                  const pb::cluster::TaskListRequest* /*request*/,
                                  pb::cluster::TaskListResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /task_list/<TaskListId>\n";  // << butil::describe_resources<Socket>() << '\n';

    butil::FlatMap<int64_t, pb::coordinator::TaskList> task_lists;
    task_lists.init(100);
    controller_->GetTaskListAll(task_lists);

    for (auto& [id, task_list] : task_lists) {
      for (int i = 0; i < task_list.tasks_size(); ++i) {
        task_list.mutable_tasks(i)->set_step(i);
      }
    }

    if (task_lists.empty()) {
      os << "No TaskList now" << '\n';
    } else {
      os << "================ TaskList: ================" << '\n';
      for (const auto& task_list : task_lists) {
        os << task_list.second.DebugString() << '\n';
      }
    }

  } else {
    char* endptr = nullptr;
    int64_t task_list_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "TaskListId=" << task_list_id << '\n';

      pb::coordinator::TaskList task_list;
      controller_->GetTaskList(task_list_id, task_list);

      for (int i = 0; i < task_list.tasks_size(); ++i) {
        task_list.mutable_tasks(i)->set_step(i);
      }

      if (task_list.id() == 0) {
        os << "TaskList is not found" << '\n';
      } else {
        os << "================ TaskList: ================" << '\n';
        os << task_list.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a TaskListId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
