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

#include "server/table_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"

namespace dingodb {

void TableImpl::default_method(google::protobuf::RpcController* controller,
                               const pb::cluster::TableRequest* /*request*/, pb::cluster::TableResponse* /*response*/,
                               google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();

  if (constraint.empty()) {
    os << "# Use /table/<TableId>\n";  // << butil::describe_resources<Socket>() << '\n';
  } else {
    char* endptr = nullptr;
    int64_t table_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "TableId=" << table_id << '\n';

      int64_t schema_id = 2;

      pb::meta::TableDefinitionWithId table_definition;
      controller_->GetTable(2, table_id, table_definition);

      bool is_index = false;

      if (table_definition.table_id().entity_id() == 0) {
        controller_->GetIndex(schema_id, table_id, false, table_definition);
        is_index = true;
        os << "(Index) TableDefinition: " << '\n';
        os << table_definition.DebugString() << '\n';
      } else {
        os << "(Table) TableDefinition: " << '\n';
        os << table_definition.DebugString() << '\n';
      }

      if (table_definition.table_id().entity_id() > 0) {
        if (!is_index) {
          pb::meta::TableRange table_range;
          controller_->GetTableRange(schema_id, table_id, table_range);

          os << "TableRange: " << '\n';
          os << table_range.DebugString() << '\n';
        } else {
          pb::meta::IndexRange index_range;
          controller_->GetIndexRange(schema_id, table_id, index_range);

          os << "IndexRange: " << '\n';
          os << index_range.DebugString() << '\n';
        }
      } else {
        os << "Table is not found" << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a TableId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

}  // namespace dingodb
