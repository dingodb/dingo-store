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
#include "brpc/server.h"

namespace dingodb {

void TableImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "table";
  info->path = "/table";
}

void TableImpl::default_method(google::protobuf::RpcController* controller,
                               const pb::cluster::TableRequest* /*request*/, pb::cluster::TableResponse* /*response*/,
                               google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();
  const bool use_html = brpc::UseHTML(cntl->http_request());

  if (constraint.empty()) {
    cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");

    if (use_html) {
      os << "<!DOCTYPE html><html><head>\n"
         << brpc::gridtable_style() << "<script src=\"/js/sorttable\"></script>\n"
         << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
         << brpc::TabsHead();

      os << "<meta charset=\"UTF-8\">\n"
         << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
         << "<style>\n"
         << "  /* Define styles for different colors */\n"
         << "  .red-text {\n"
         << "    color: red;\n"
         << "  }\n"
         << "  .blue-text {\n"
         << "    color: blue;\n"
         << "  }\n"
         << "  .green-text {\n"
         << "    color: green;\n"
         << "  }\n"
         << "  .bold-text {"
         << "    font-weight: bold;"
         << "  }"
         << "</style>\n";

      os << brpc::TabsHead() << "</head><body>";
      server->PrintTabsBody(os, "table");
    } else {
      os << "# Use /table/<TableId>\n";  // << butil::describe_resources<Socket>() << '\n';
    }

    int64_t epoch = 0;
    pb::common::Location coordinator_leader_location;
    std::vector<pb::common::Location> locations;
    pb::common::CoordinatorMap coordinator_map;
    coordinator_controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations, coordinator_map);

    os << "DINGO_STORE VERSION: " << std::string(GIT_VERSION) << '\n';

    if (coordinator_controller_->IsLeader()) {
      os << (use_html ? "<br>\n" : "\n");
      os << "Coordinator role: <span class=\"blue-text bold-text\">LEADER</span>" << '\n';

      // add url for task_list
      os << (use_html ? "<br>\n" : "\n");
      os << "<a href=\"/task_list/"
         << "\">"
         << "GET_TASK_LIST"
         << "</a>" << '\n';
      os << "<a href=\"/store_operation/"
         << "\">"
         << "GET_OPERATION"
         << "</a>" << '\n';

      os << (use_html ? "<br>CoordinatorMap:\n" : "\n");
      for (const auto& location : locations) {
        os << "<a href=http://" + location.host() + ":" + std::to_string(location.port()) + "/dingo>" +
                  location.host() + ":" + std::to_string(location.port()) + "</a>"
           << '\n';
      }
    } else {
      os << (use_html ? "<br>\n" : "\n");
      os << "Coordinator role: <span class=\"red-text bold-text\">FOLLOWER</span>" << '\n';

      os << (use_html ? "<br>\n" : "\n");
      os << "Coordinator Leader is <a class=\"red-text bold-text\" href=http://" + coordinator_leader_location.host() +
                ":" + std::to_string(coordinator_leader_location.port()) + "/dingo>" +
                coordinator_leader_location.host() + ":" + std::to_string(coordinator_leader_location.port()) + "</a>"
         << '\n';
    }

    os << (use_html ? "<br>\n" : "\n");
    os << (use_html ? "<br>\n" : "\n");
    PrintSchemaTables(os, use_html);

  } else {
    char* endptr = nullptr;
    int64_t table_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "TableId=" << table_id << '\n';

      int64_t schema_id = 2;

      pb::meta::TableDefinitionWithId table_definition;
      coordinator_controller_->GetTable(2, table_id, table_definition);

      bool is_index = false;

      if (table_definition.table_id().entity_id() == 0) {
        coordinator_controller_->GetIndex(schema_id, table_id, false, table_definition);
        is_index = true;
        os << "================ (Index) TableDefinition: ================" << '\n';
        os << table_definition.DebugString() << '\n';
      } else {
        os << "================ (Table) TableDefinition: ================" << '\n';
        os << table_definition.DebugString() << '\n';
      }

      if (table_definition.table_id().entity_id() > 0) {
        if (!is_index) {
          pb::meta::TableRange table_range;
          coordinator_controller_->GetTableRange(schema_id, table_id, table_range);

          os << "================ TableRange: ================" << '\n';
          os << table_range.DebugString() << '\n';
        } else {
          pb::meta::IndexRange index_range;
          coordinator_controller_->GetIndexRange(schema_id, table_id, index_range);

          os << "================ IndexRange: ================" << '\n';
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

void TableImpl::PrintSchemaTables(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("SCHEMA");
  table_header.push_back("TABLE_ID");
  table_header.push_back("TABLE_NAME");
  table_header.push_back("ENGINE");
  table_header.push_back("PARTITIONS");
  table_header.push_back("REGIONS");
  table_header.push_back("COLUMNS");
  table_header.push_back("AUTO_INCR");
  table_header.push_back("REPLICA");
  table_header.push_back("TYPE");
  table_header.push_back("TABLE_ID");
  table_header.push_back("VECTOR_INDEX_TYPE");
  table_header.push_back("DIMENSION");
  table_header.push_back("METRIC_TYPE");
  table_header.push_back("CHARSET");
  table_header.push_back("COLLATE");
  table_header.push_back("TABLE_TYPE");
  table_header.push_back("ROW_FORMAT");
  table_header.push_back("CREATE_TIME");
  table_header.push_back("UPDATE_TIME");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // SCHMEA_NAME
  min_widths.push_back(10);  // TABLE_ID
  min_widths.push_back(20);  // TABLE_NAME
  min_widths.push_back(6);   // ENGINE
  min_widths.push_back(2);   // PARTITIONS
  min_widths.push_back(2);   // REGIONS
  min_widths.push_back(3);   // COLUMNS
  min_widths.push_back(3);   // AUTO_INCR
  min_widths.push_back(3);   // REPLICA
  min_widths.push_back(6);   // TYPE
  min_widths.push_back(10);  // TABLE_ID
  min_widths.push_back(30);  // VECTOR_INDEX_TYPE
  min_widths.push_back(5);   // DIMENSION
  min_widths.push_back(20);  // METRIC_TYPE
  min_widths.push_back(10);  // CHARSET
  min_widths.push_back(10);  // COLLATE
  min_widths.push_back(10);  // TABLE_TYPE
  min_widths.push_back(10);  // ROW_FORMAT
  min_widths.push_back(20);  // CREATE_TIME
  min_widths.push_back(20);  // UPDATE_TIME

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  // 1. Get All Schema
  std::vector<pb::meta::Schema> schemas;
  coordinator_controller_->GetSchemas(-1, schemas);

  if (schemas.empty()) {
    DINGO_LOG(ERROR) << "Get Schemas Failed";
  }

  for (const auto& schema : schemas) {
    int64_t const schema_id = schema.id().entity_id();
    if (schema_id == 0 || schema.name() == Constant::kRootSchemaName) {
      continue;
    }

    for (const auto& table_entry : schema.table_ids()) {
      std::vector<std::string> line;
      std::vector<std::string> url_line;

      line.push_back(schema.name());  // SCHEMA
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_entry.entity_id()));  // TABLE_ID
      url_line.push_back(std::string("/table/"));

      // Start TableID
      auto table_id = table_entry.entity_id();

      pb::meta::TableDefinitionWithId table_definition;
      coordinator_controller_->GetTable(schema_id, table_id, table_definition);

      line.push_back(table_definition.table_definition().name());  // TABLE_NAME
      url_line.push_back(std::string());

      line.push_back(pb::common::Engine_Name(table_definition.table_definition().engine()));  // ENGINE
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(table_definition.table_definition().table_partition().partitions_size()));  // PARTITIONS
      url_line.push_back(std::string());

      pb::meta::TableRange table_range;
      coordinator_controller_->GetTableRange(schema_id, table_id, table_range);

      line.push_back(std::to_string(table_range.range_distribution_size()));  // REGIONS
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().columns_size()));  // COLUMNS
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().auto_increment()));  // AUTO_INCR
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().replica()));  // REPLICA
      url_line.push_back(std::string());

      line.push_back("TABLE");  // TYPE
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_entry.entity_id()));  // TABLE_ID
      url_line.push_back(std::string("/table/"));

      line.push_back("N/A");  // VECTOR_INDEX_TYPE
      url_line.push_back(std::string());

      line.push_back("N/A");  // DIMENSION
      url_line.push_back(std::string());

      line.push_back("N/A");  // METRIC_TYPE
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().charset());  // CHARSET
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().collate());  // COLLATE
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().table_type());  // TABLE_TYPE
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().row_format());  // ROW_FORMAT
      url_line.push_back(std::string());

      line.push_back(Helper::FormatMsTime(table_definition.table_definition().create_timestamp(),
                                          "%Y-%m-%d %H:%M:%S"));  // CREATE_TIME
      url_line.push_back(std::string());

      line.push_back(Helper::FormatMsTime(table_definition.table_definition().update_timestamp(),
                                          "%Y-%m-%d %H:%M:%S"));  // UPDATE_TIME
      url_line.push_back(std::string());

      table_contents.push_back(line);
      table_urls.push_back(url_line);
    }

    for (const auto& index_entry : schema.index_ids()) {
      std::vector<std::string> line;
      std::vector<std::string> url_line;

      line.push_back(schema.name());  // SCHEMA
      url_line.push_back(std::string());
      line.push_back(std::to_string(index_entry.entity_id()));  // INDEX_ID
      url_line.push_back(std::string("/table/"));

      // Start TableID
      auto table_id = index_entry.entity_id();

      pb::meta::TableDefinitionWithId table_definition;
      coordinator_controller_->GetIndex(schema_id, table_id, false, table_definition);

      line.push_back(table_definition.table_definition().name());  // TABLE_NAME
      url_line.push_back(std::string());

      line.push_back(pb::common::Engine_Name(table_definition.table_definition().engine()));  // ENGINE
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(table_definition.table_definition().table_partition().partitions_size()));  // PARTITIONS
      url_line.push_back(std::string());

      pb::meta::IndexRange table_range;
      coordinator_controller_->GetIndexRange(schema_id, table_id, table_range);

      line.push_back(std::to_string(table_range.range_distribution_size()));  // REGIONS
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().columns_size()));  // COLUMNS
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().auto_increment()));  // AUTO_INCR
      url_line.push_back(std::string());

      line.push_back(std::to_string(table_definition.table_definition().replica()));  // REPLICA
      url_line.push_back(std::string());

      line.push_back("INDEX");  // TYPE
      url_line.push_back(std::string());

      line.push_back(std::to_string(index_entry.entity_id()));  // TABLE_ID
      url_line.push_back(std::string("/table/"));

      if (table_definition.table_definition().index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
        const auto& vector_index_parameter =
            table_definition.table_definition().index_parameter().vector_index_parameter();

        line.push_back(
            pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type()));  // VECTOR_INDEX_TYPE
        url_line.push_back(std::string());

        if (vector_index_parameter.has_flat_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.flat_parameter().dimension()));  // DIMENSION
          url_line.push_back(std::string());
          line.push_back(
              pb::common::MetricType_Name(vector_index_parameter.flat_parameter().metric_type()));  // METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_bruteforce_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.bruteforce_parameter().dimension()));  // DIMENSION
          url_line.push_back(std::string());
          line.push_back(
              pb::common::MetricType_Name(vector_index_parameter.bruteforce_parameter().metric_type()));  // METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_ivf_flat_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.ivf_flat_parameter().dimension()));  // DIMENSION
          url_line.push_back(std::string());
          line.push_back(
              pb::common::MetricType_Name(vector_index_parameter.ivf_flat_parameter().metric_type()));  // METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_ivf_pq_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.ivf_pq_parameter().dimension()));  // DIMENSION
          url_line.push_back(std::string());
          line.push_back(
              pb::common::MetricType_Name(vector_index_parameter.ivf_pq_parameter().metric_type()));  // METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_hnsw_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.hnsw_parameter().dimension()));  // DIMENSION
          url_line.push_back(std::string());
          line.push_back(
              pb::common::MetricType_Name(vector_index_parameter.hnsw_parameter().metric_type()));  // METRIC_TYPE
          url_line.push_back(std::string());
        } else {
          line.push_back("N/A");  // DIMENSION
          url_line.push_back(std::string());
          line.push_back("N/A");  // METRIC_TYPE
          url_line.push_back(std::string());
        }

      } else {
        line.push_back("N/A");  // VECTOR_INDEX_TYPE
        url_line.push_back(std::string());
        line.push_back("N/A");  // DIMENSION
        url_line.push_back(std::string());
        line.push_back("N/A");  // METRIC_TYPE
        url_line.push_back(std::string());
      }

      line.push_back(table_definition.table_definition().charset());  // CHARSET
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().collate());  // COLLATE
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().table_type());  // TABLE_TYPE
      url_line.push_back(std::string());

      line.push_back(table_definition.table_definition().row_format());  // ROW_FORMAT
      url_line.push_back(std::string());

      line.push_back(Helper::FormatMsTime(table_definition.table_definition().create_timestamp(),
                                          "%Y-%m-%d %H:%M:%S"));  // CREATE_TIME
      url_line.push_back(std::string());

      line.push_back(Helper::FormatMsTime(table_definition.table_definition().update_timestamp(),
                                          "%Y-%m-%d %H:%M:%S"));  // UPDATE_TIME
      url_line.push_back(std::string());

      table_contents.push_back(line);
      table_urls.push_back(url_line);
    }
  }

  if (use_html) {
    os << "<span class=\"bold-text\">TABLE: " << table_contents.size() << "</span>" << '\n';
  }

  Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

}  // namespace dingodb
