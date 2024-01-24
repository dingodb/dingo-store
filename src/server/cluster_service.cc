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

#include "server/cluster_service.h"

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "vector/codec.h"

namespace dingodb {

void ClusterStatImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "dingo";
  info->path = "/dingo";
}

// Print a table in HTML or plain text format.
// @param os Output stream
// @param use_html Whether to use HTML format
// @param table_header Header of the table
// @param min_widths Minimum width of each column
// @param table_contents Contents of the table
// @param table_urls Urls of the table
void ClusterStatImpl::PrintHtmlTable(std::ostream& os, bool use_html, const std::vector<std::string>& table_header,
                                     const std::vector<int32_t>& min_widths,
                                     const std::vector<std::vector<std::string>>& table_contents,
                                     const std::vector<std::vector<std::string>>& table_urls) {
  if (use_html) {
    // os << "<table class=\"gridtable sortable\" border=\"1\"><tr>";
    os << R"(<table class="gridtable sortable" border="1"><tr>)";
    for (const auto& header : table_header) {
      os << "<th>" << header << "</th>";
    }
    os << "</tr>\n";
  } else {
    for (const auto& header : table_header) {
      os << header << "|";
    }
  }

  for (int i = 0; i < table_contents.size(); i++) {
    const auto& line = table_contents[i];
    const std::vector<std::string>& url_line = table_urls.empty() ? std::vector<std::string>() : table_urls[i];

    if (use_html) {
      os << "<tr>";
    }
    for (size_t i = 0; i < line.size(); ++i) {
      if (use_html) {
        os << "<td>";
      }

      if (use_html) {
        if (!url_line.empty() && !url_line[i].empty()) {
          if (url_line[i].substr(0, 4) == "http") {
            os << "<a href=\"" << url_line[i] << "\">" << line[i] << "</a>";
          } else {
            os << "<a href=\"" << url_line[i] << line[i] << "\">" << line[i] << "</a>";
          }
        } else {
          os << brpc::min_width(line[i], min_widths[i]);
        }
      } else {
        os << brpc::min_width(line[i], min_widths[i]);
      }
      if (use_html) {
        os << "</td>";
      } else {
        os << "|";
      }
    }
    if (use_html) {
      os << "</tr>";
    }
    os << '\n';
  }

  if (use_html) {
    os << "</table>\n";
  }
}

void ClusterStatImpl::PrintStores(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("ID");
  table_header.push_back("TYPE");
  table_header.push_back("STATE");
  table_header.push_back("IN_STATE");
  table_header.push_back("SERVER_LOCATION");
  table_header.push_back("RAFT_LOCATION");
  table_header.push_back("RESOURCE_TAG");
  table_header.push_back("CREATE_TIME");
  table_header.push_back("UPDATE_TIME");
  table_header.push_back("STORE_METRICS");
  table_header.push_back("STORE_OPERATION");
  table_header.push_back("INFO");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // ID
  min_widths.push_back(16);  // TYPE
  min_widths.push_back(16);  // STATE
  min_widths.push_back(10);  // IN_STATE
  min_widths.push_back(30);  // SERVER_LOCATION
  min_widths.push_back(30);  // RAFT_LOCATION
  min_widths.push_back(15);  // RESOURCE_TAG
  min_widths.push_back(30);  // CREATE_TIME
  min_widths.push_back(30);  // UPDATE_TIME
  min_widths.push_back(10);  // STORE_METRICS
  min_widths.push_back(10);  // STORE_OPERATION
  min_widths.push_back(5);   // INFO

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  pb::common::StoreMap store_map;
  controller_->GetStoreMap(store_map);

  for (const auto& store : store_map.stores()) {
    std::vector<std::string> line;
    std::vector<std::string> url_line;

    line.push_back(std::to_string(store.id()));  // ID
    url_line.push_back(std::string());
    line.push_back(pb::common::StoreType_Name(store.store_type()));  // TYPE
    url_line.push_back(std::string());
    line.push_back(pb::common::StoreState_Name(store.state()));  // STATE
    url_line.push_back(std::string());
    line.push_back(pb::common::StoreInState_Name(store.in_state()));  //
    url_line.push_back(std::string());
    line.push_back(store.server_location().host() + ":" +
                   std::to_string(store.server_location().port()));  // SERVER_LOCATION
    url_line.push_back("http://" + store.server_location().host() + ":" +
                       std::to_string(store.server_location().port()));
    line.push_back(store.raft_location().host() + ":" + std::to_string(store.raft_location().port()));  // RAFT_LOCATION
    url_line.push_back("http://" + store.raft_location().host() + ":" + std::to_string(store.raft_location().port()) +
                       "/raft_stat");
    if (store.resource_tag().empty()) {
      line.push_back("N/A");  // RESOURCE_TAG
    } else {
      line.push_back(store.resource_tag());  // RESOURCE_TAG
    }
    url_line.push_back(std::string());
    line.push_back(Helper::FormatMsTime(store.create_timestamp(), "%Y-%m-%d %H:%M:%S"));  // CREATE_TIME
    url_line.push_back(std::string());
    line.push_back(Helper::FormatMsTime(store.last_seen_timestamp(), "%Y-%m-%d %H:%M:%S"));  // UPDATE_TIME
    url_line.push_back(std::string());
    line.push_back(std::to_string(store.id()));  // STORE_METRICS
    url_line.push_back("/store_metrics/");
    line.push_back(std::to_string(store.id()));  // STORE_OPERATION
    url_line.push_back("/store_operation/");
    line.push_back("Hello");  // INFO
    if (store.store_type() == pb::common::StoreType::NODE_TYPE_STORE) {
      url_line.push_back("http://" + store.server_location().host() + ":" +
                         std::to_string(store.server_location().port()) + "/StoreService/Hello/");
    } else if (store.store_type() == pb::common::StoreType::NODE_TYPE_INDEX) {
      url_line.push_back("http://" + store.server_location().host() + ":" +
                         std::to_string(store.server_location().port()) + "/IndexService/Hello/");
    } else {
      url_line.push_back(std::string());
    }

    table_contents.push_back(line);
    table_urls.push_back(url_line);
  }

  PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

void ClusterStatImpl::PrintExecutors(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("ID");
  table_header.push_back("USER");
  table_header.push_back("STATUS");
  table_header.push_back("SERVER_LOCATION");
  table_header.push_back("RESOURCE_TAG");
  table_header.push_back("CREATE_TIME");
  table_header.push_back("UPDATE_TIME");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // ID
  min_widths.push_back(16);  // USER
  min_widths.push_back(16);  // STATUS
  min_widths.push_back(30);  // SERVER_LOCATION
  min_widths.push_back(15);  // RESOURCE_TAG
  min_widths.push_back(30);  // CREATE_TIME
  min_widths.push_back(30);  // UPDATE_TIME

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  pb::common::ExecutorMap executor_map;
  controller_->GetExecutorMap(executor_map);

  for (const auto& executor : executor_map.executors()) {
    std::vector<std::string> line;

    line.push_back(executor.id());
    line.push_back(executor.executor_user().user());
    line.push_back(pb::common::ExecutorState_Name(executor.state()));
    line.push_back(executor.server_location().host() + ":" + std::to_string(executor.server_location().port()));
    if (executor.resource_tag().empty()) {
      line.push_back("N/A");
    } else {
      line.push_back(executor.resource_tag());
    }
    line.push_back(Helper::FormatMsTime(executor.create_timestamp(), "%Y-%m-%d %H:%M:%S"));
    line.push_back(Helper::FormatMsTime(executor.last_seen_timestamp(), "%Y-%m-%d %H:%M:%S"));

    table_contents.push_back(line);
  }

  PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

void ClusterStatImpl::PrintRegions(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("REGION_ID");
  table_header.push_back("REGION_NAME");
  table_header.push_back("EPOCH");
  table_header.push_back("REGION_TYPE");
  table_header.push_back("REGION_STATE");
  table_header.push_back("BRAFT_STATUS");
  table_header.push_back("REPLICA_STATUS");
  table_header.push_back("STORE_REGION_STATE");
  table_header.push_back("LEADER_ID");
  table_header.push_back("REPLICA");
  table_header.push_back("START_KEY");
  table_header.push_back("END_KEY");
  table_header.push_back("SCHEMA_ID");
  table_header.push_back("TABLE_ID");
  table_header.push_back("INDEX_ID");
  table_header.push_back("PART_ID");
  table_header.push_back("ENGINE");
  table_header.push_back("CREATE_TIME");
  table_header.push_back("UPDATE_TIME");
  table_header.push_back("APPLIED_INDEX");
  table_header.push_back("VECTOR_TYPE");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(20);  // REGION_NAME
  min_widths.push_back(6);   // EPOCH
  min_widths.push_back(10);  // REGION_TYPE
  min_widths.push_back(10);  // REGION_STATE
  min_widths.push_back(10);  // BRAFT_STATUS
  min_widths.push_back(10);  // REPLICA_STATUS
  min_widths.push_back(10);  // STORE_REGION_STATE
  min_widths.push_back(10);  // LEADER_ID
  min_widths.push_back(10);  // REPLICA
  min_widths.push_back(20);  // START_KEY
  min_widths.push_back(20);  // END_KEY
  min_widths.push_back(10);  // SCHEMA_ID
  min_widths.push_back(10);  // TABLE_ID
  min_widths.push_back(10);  // INDEX_ID
  min_widths.push_back(10);  // PART_ID
  min_widths.push_back(10);  // ENGINE
  min_widths.push_back(20);  // CREATE_TIME
  min_widths.push_back(20);  // UPDATE_TIME
  min_widths.push_back(10);  // APPLIED_INDEX
  min_widths.push_back(10);  // VECTOR_TYPE

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  pb::common::RegionMap region_map;
  controller_->GetRegionMapFull(region_map);

  for (const auto& region : region_map.regions()) {
    std::vector<std::string> line;
    std::vector<std::string> url_line;

    line.push_back(std::to_string(region.id()));
    url_line.push_back(std::string("/region/"));

    line.push_back(region.definition().name());
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().epoch().conf_version()) + "-" +
                   std::to_string(region.definition().epoch().version()));
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionType_Name(region.region_type()));
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionState_Name(region.state()));
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionRaftStatus_Name(region.status().raft_status()));
    url_line.push_back(std::string());

    line.push_back(pb::common::ReplicaStatus_Name(region.status().replica_status()));
    url_line.push_back(std::string());

    line.push_back(pb::common::StoreRegionState_Name(region.metrics().store_region_state()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.leader_store_id()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().peers_size()));
    url_line.push_back(std::string());

    line.push_back(Helper::StringToHex(region.definition().range().start_key()));
    url_line.push_back(std::string());

    line.push_back(Helper::StringToHex(region.definition().range().end_key()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().schema_id()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().table_id()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().index_id()));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().part_id()));
    url_line.push_back(std::string());

    line.push_back(pb::common::RawEngine_Name(region.definition().raw_engine()));
    url_line.push_back(std::string());

    line.push_back(Helper::FormatMsTime(region.create_timestamp(), "%Y-%m-%d %H:%M:%S"));
    url_line.push_back(std::string());

    line.push_back(Helper::FormatMsTime(region.metrics().last_update_metrics_timestamp(), "%Y-%m-%d %H:%M:%S"));
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().last_update_metrics_log_index()));
    url_line.push_back(std::string());

    auto vector_index_type = region.definition().index_parameter().vector_index_parameter().vector_index_type();
    if (vector_index_type != pb::common::VECTOR_INDEX_TYPE_NONE) {
      line.push_back(pb::common::VectorIndexType_Name(vector_index_type));
    } else {
      line.push_back("N/A");
    }
    url_line.push_back(std::string());

    table_contents.push_back(line);
    table_urls.push_back(url_line);
  }

  PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

void ClusterStatImpl::PrintSchemaTables(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("SCHEMA_NAME");
  table_header.push_back("TABLE_ID");
  table_header.push_back("TABLE_NAME");
  table_header.push_back("ENGINE");
  table_header.push_back("TYPE");
  table_header.push_back("PARTITIONS");
  table_header.push_back("REGIONS");
  table_header.push_back("VECTOR_INDEX_TYPE");
  table_header.push_back("VECTOR_DIMENSION");
  table_header.push_back("VECTOR_METRIC_TYPE");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // SCHMEA_NAME
  min_widths.push_back(10);  // TABLE_ID
  min_widths.push_back(20);  // TABLE_NAME
  min_widths.push_back(6);   // ENGINE
  min_widths.push_back(6);   // TYPE
  min_widths.push_back(2);   // PARTITIONS
  min_widths.push_back(2);   // REGIONS
  min_widths.push_back(30);  // VECTOR_INDEX_TYPE
  min_widths.push_back(5);   // VECTOR_DIMENSION
  min_widths.push_back(20);  // VECTOR_METRIC_TYPE

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  // 1. Get All Schema
  std::vector<pb::meta::Schema> schemas;
  controller_->GetSchemas(0, schemas);

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

      line.push_back(schema.name());  // SCHEMA_NAME
      url_line.push_back(std::string());
      line.push_back(std::to_string(table_entry.entity_id()));  // TABLE_ID
      url_line.push_back(std::string("/table/"));

      // Start TableID
      auto table_id = table_entry.entity_id();

      pb::meta::TableDefinitionWithId table_definition;
      controller_->GetTable(schema_id, table_id, table_definition);

      line.push_back(table_definition.table_definition().name());  // TABLE_NAME
      url_line.push_back(std::string());
      line.push_back(pb::common::Engine_Name(table_definition.table_definition().engine()));  // ENGINE
      url_line.push_back(std::string());
      line.push_back("TABLE");  // TYPE
      url_line.push_back(std::string());
      line.push_back(
          std::to_string(table_definition.table_definition().table_partition().partitions_size()));  // PARTITIONS
      url_line.push_back(std::string());

      pb::meta::TableRange table_range;
      controller_->GetTableRange(schema_id, table_id, table_range);

      line.push_back(std::to_string(table_range.range_distribution_size()));  // REGIONS
      url_line.push_back(std::string());

      line.push_back("N/A");  // VECTOR_INDEX_TYPE
      url_line.push_back(std::string());
      line.push_back("N/A");  // VECTOR_DIMENSION
      url_line.push_back(std::string());
      line.push_back("N/A");  // VECTOR_METRIC_TYPE
      url_line.push_back(std::string());

      table_contents.push_back(line);
      table_urls.push_back(url_line);
    }

    for (const auto& index_entry : schema.index_ids()) {
      std::vector<std::string> line;
      std::vector<std::string> url_line;

      line.push_back(schema.name());  // SCHEMA_NAME
      url_line.push_back(std::string());
      line.push_back(std::to_string(index_entry.entity_id()));  // INDEX_ID
      url_line.push_back(std::string("/table/"));

      // Start TableID
      auto table_id = index_entry.entity_id();

      pb::meta::TableDefinitionWithId table_definition;
      controller_->GetIndex(schema_id, table_id, false, table_definition);

      line.push_back(table_definition.table_definition().name());  // TABLE_NAME
      url_line.push_back(std::string());
      line.push_back(pb::common::Engine_Name(table_definition.table_definition().engine()));  // ENGINE
      url_line.push_back(std::string());
      line.push_back("INDEX");  // TYPE
      url_line.push_back(std::string());
      line.push_back(
          std::to_string(table_definition.table_definition().table_partition().partitions_size()));  // PARTITIONS
      url_line.push_back(std::string());

      pb::meta::IndexRange table_range;
      controller_->GetIndexRange(schema_id, table_id, table_range);

      line.push_back(std::to_string(table_range.range_distribution_size()));  // REGIONS
      url_line.push_back(std::string());

      if (table_definition.table_definition().index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
        const auto& vector_index_parameter =
            table_definition.table_definition().index_parameter().vector_index_parameter();

        line.push_back(pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type()));
        url_line.push_back(std::string());

        if (vector_index_parameter.has_flat_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.flat_parameter().dimension()));  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back(pb::common::MetricType_Name(
              vector_index_parameter.flat_parameter().metric_type()));  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_bruteforce_parameter()) {
          line.push_back(
              std::to_string(vector_index_parameter.bruteforce_parameter().dimension()));  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back(pb::common::MetricType_Name(
              vector_index_parameter.bruteforce_parameter().metric_type()));  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_ivf_flat_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.ivf_flat_parameter().dimension()));  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back(pb::common::MetricType_Name(
              vector_index_parameter.ivf_flat_parameter().metric_type()));  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_ivf_pq_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.ivf_pq_parameter().dimension()));  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back(pb::common::MetricType_Name(
              vector_index_parameter.ivf_pq_parameter().metric_type()));  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        } else if (vector_index_parameter.has_hnsw_parameter()) {
          line.push_back(std::to_string(vector_index_parameter.hnsw_parameter().dimension()));  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back(pb::common::MetricType_Name(
              vector_index_parameter.hnsw_parameter().metric_type()));  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        } else {
          line.push_back("N/A");  // VECTOR_DIMENSION
          url_line.push_back(std::string());
          line.push_back("N/A");  // VECTOR_METRIC_TYPE
          url_line.push_back(std::string());
        }

      } else {
        line.push_back("N/A");  // VECTOR_INDEX_TYPE
        url_line.push_back(std::string());
        line.push_back("N/A");  // VECTOR_DIMENSION
        url_line.push_back(std::string());
        line.push_back("N/A");  // VECTOR_METRIC_TYPE
        url_line.push_back(std::string());
      }

      table_contents.push_back(line);
      table_urls.push_back(url_line);
    }
  }

  PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

void ClusterStatImpl::default_method(::google::protobuf::RpcController* controller,
                                     const pb::cluster::ClusterStatRequest* /*request*/,
                                     pb::cluster::ClusterStatResponse* /*response*/,
                                     ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");

  if (use_html) {
    os << "<!DOCTYPE html><html><head>\n"
       << brpc::gridtable_style() << "<script src=\"/js/sorttable\"></script>\n"
       << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
       << brpc::TabsHead() << "</head><body>";
    server->PrintTabsBody(os, "dingo");
  }

  int64_t cluster_id = 0;
  int64_t epoch = 0;
  pb::common::Location coordinator_leader_location;
  std::vector<pb::common::Location> locations;
  controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations);

  os << "dingo-store version: " << std::string(GIT_VERSION) << '\n';
  if (controller_->IsLeader()) {
    os << (use_html ? "<br>\n" : "\n");
    os << "dingo-store role: " << '\n';
    os << "<a href=\"/CoordinatorService/Hello/"
       << "\">"
       << "LEADER"
       << "</a>" << '\n';

    // add url for task_list
    os << (use_html ? "<br>\n" : "\n");
    os << "<a href=\"/task_list/"
       << "\">"
       << "GET_TASK_LIST"
       << "</a>" << '\n';

    for (const auto& location : locations) {
      os << (use_html ? "<br>\n" : "\n");
      os << "Follower is <a href=http://" + location.host() + ":" + std::to_string(location.port()) + "/dingo>" +
                location.host() + ":" + std::to_string(location.port()) + "</a>"
         << '\n';
    }
  } else {
    os << (use_html ? "<br>\n" : "\n");
    os << "dingo-store role: " << '\n';
    os << "<a href=\"/CoordinatorService/Hello/"
       << "\">"
       << "FOLLOWER"
       << "</a>" << '\n';

    os << (use_html ? "<br>\n" : "\n");
    os << "Leader is <a href=http://" + coordinator_leader_location.host() + ":" +
              std::to_string(coordinator_leader_location.port()) + "/dingo>" + coordinator_leader_location.host() +
              ":" + std::to_string(coordinator_leader_location.port()) + "</a>"
       << '\n';
  }

  os << (use_html ? "<br>\n" : "\n");
  os << (use_html ? "<br>\n" : "\n");
  os << "STORE AND INDEX: " << '\n';
  PrintStores(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "EXECUTOR: " << '\n';
  PrintExecutors(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "TABLE AND INDEX: " << '\n';
  PrintSchemaTables(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "REGION: " << '\n';
  PrintRegions(os, use_html);

  if (use_html) {
    os << "</body></html>\n";
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

bool ClusterStatImpl::GetRegionInfo(int64_t region_id, const pb::common::RegionMap& region_map,
                                    pb::common::Region& result) {
  bool is_found = false;
  for (const auto& region : region_map.regions()) {
    if (region.id() == region_id) {
      is_found = true;
      result = region;
    }
  }
  DINGO_LOG(WARNING) << "Get Region Id From RegionMap Failed. Input RegionID:" << region_id;
  return is_found;
}

void ClusterStatImpl::PrintSchema(std::ostream& os, const std::string& schema_name) {
  os << "<p style=\"text-align: center;\">Schema:" + schema_name + "</p>";
}

void ClusterStatImpl::PrintRegionNode(std::ostream& os, const pb::common::Region& region) {
  DINGO_LOG(INFO) << "Region:" << region.ShortDebugString();
  std::string epoch_info = fmt::format("<li>epoch: {}-{}</li>", region.definition().epoch().conf_version(),
                                       region.definition().epoch().version());
  std::string create_time_info =
      fmt::format("<li>create_time: {}", Helper::FormatMsTime(region.create_timestamp(), "%Y-%m-%d %H:%M:%S"));
  std::string leader_info = "<li>Leader:";
  std::string follower_info = "<li>Follower:";
  int64_t const leader_store_id = region.leader_store_id();
  for (const auto& peer : region.definition().peers()) {
    std::string ip_port;
    ip_port += peer.raft_location().host();
    ip_port += ":";
    ip_port += std::to_string(peer.raft_location().port());
    if (peer.store_id() == leader_store_id) {
      leader_info += "<a href=\"http://";
      leader_info += ip_port;
      leader_info += "/status\"";
      leader_info += ">";
      leader_info += ip_port;
      leader_info += "</a>";
    } else {
      follower_info += "<a href=\"http://";
      follower_info += ip_port;
      follower_info += "/status\"";
      follower_info += ">";
      follower_info += ip_port;
      follower_info += "</a>";
      follower_info += " ";
    }
  }
  leader_info += "</li>";
  follower_info += "</li>";

  std::string range_info;
  if (region.definition().index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
    range_info = fmt::format("<li>Range:[{}, {}); [{}/{}, {}/{})</li>",
                             Helper::StringToHex(region.definition().range().start_key()),
                             Helper::StringToHex(region.definition().range().end_key()),
                             VectorCodec::DecodePartitionId(region.definition().range().start_key()),
                             VectorCodec::DecodeVectorId(region.definition().range().start_key()),
                             VectorCodec::DecodePartitionId(region.definition().range().end_key()),
                             VectorCodec::DecodeVectorId(region.definition().range().end_key()));
  } else {
    range_info = fmt::format("<li>Range:[{}, {})</li>", Helper::StringToHex(region.definition().range().start_key()),
                             Helper::StringToHex(region.definition().range().end_key()));
  }

  os << "<li>RegionID:";
  os << std::to_string(region.id());
  os << "<ul>";
  os << epoch_info;
  os << create_time_info;
  os << leader_info;
  os << follower_info;
  os << range_info;
  os << "</ul>";
  os << "</li>";
}

void ClusterStatImpl::PrintTableRegions(std::ostream& os, const pb::common::RegionMap& region_map,
                                        const pb::meta::TableRange& table_range) {
  os << "<li> Regions: ";
  os << "<ul>";
  for (const auto& range_distribution : table_range.range_distribution()) {
    int64_t const region_id = range_distribution.id().entity_id();
    DINGO_LOG(INFO) << "RangeID:" << region_id << "," << range_distribution.ShortDebugString();
    pb::common::Region found_region;
    bool const is_found = GetRegionInfo(region_id, region_map, found_region);
    if (!is_found) {
      DINGO_LOG(WARNING) << "Cannot Found Region:" << region_id << " on RegionMap";
      continue;
    }

    // Append Region Info to HTML
    PrintRegionNode(os, found_region);
  }
  // End Append Region Information
  os << "</ul>";
  os << "</li>";
}

std::string ClusterStatImpl::GetTabHead() {
  std::string source_str(brpc::TabsHead());
  const std::string substr_to_replace = "ol,ul { list-style:none; }\n";
  const std::string replace_str = "ul { font-size: 18px; }";
  size_t pos = 0;
  while ((pos = source_str.find(substr_to_replace, pos)) != std::string::npos) {
    source_str.replace(pos, substr_to_replace.length(), replace_str);
    break;
  }
  return source_str;
}

void ClusterStatImpl::PrintTableDefinition(std::ostream& os, const pb::meta::TableDefinition& table_definition) {
  /*
   * <li>Columns
   *  <ul>
   *   <li>ID:INT</li>
   *   <li>NAME:varchar</li>
   *   <li>AGE:int</li>
   *   </ul>
   * </li>
   * */
  os << "<li> Columns:";
  os << "<ul>";
  for (const auto& column : table_definition.columns()) {
    os << "<li>" << column.name() << ":" << column.sql_type() << "</li>";
  }
  os << "</ul>";
  os << "</li>";

  os << "<li> Engine:";
  os << "<ul>";
  os << "<li>" << pb::common::Engine_Name(table_definition.engine()) << "</li>";
  os << "</ul>";
  os << "</li>";
}

void ClusterStatImpl::PrintIndexDefinition(std::ostream& os, const pb::meta::TableDefinition& table_definition) {
  /*
   * <li>Columns
   *  <ul>
   *   <li>ID:INT</li>
   *   <li>NAME:varchar</li>
   *   <li>AGE:int</li>
   *   </ul>
   * </li>
   * */
  os << "<li> IndexDefinition:";
  os << "<ul>";
  os << "<li>" << table_definition.ShortDebugString() << "</li>";
  os << "</ul>";
  os << "</li>";

  os << "<li> Engine:";
  os << "<ul>";
  os << "<li>" << pb::common::Engine_Name(table_definition.engine()) << "</li>";
  os << "</ul>";
  os << "</li>";
}

void ClusterStatImpl::PrintIndexRegions(std::ostream& os, const pb::common::RegionMap& region_map,
                                        const pb::meta::IndexRange& index_range) {
  os << "<li> Regions: ";
  os << "<ul>";
  for (const auto& range_distribution : index_range.range_distribution()) {
    int64_t const region_id = range_distribution.id().entity_id();
    DINGO_LOG(INFO) << "RangeID:" << region_id << "," << range_distribution.ShortDebugString();
    pb::common::Region found_region;
    bool const is_found = GetRegionInfo(region_id, region_map, found_region);
    if (!is_found) {
      DINGO_LOG(WARNING) << "Cannot Found Region:" << region_id << " on RegionMap";
      continue;
    }

    // Append Region Info to HTML
    PrintRegionNode(os, found_region);
  }
  // End Append Region Information
  os << "</ul>";
  os << "</li>";
}

}  // namespace dingodb
