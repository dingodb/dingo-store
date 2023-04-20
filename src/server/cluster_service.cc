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

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/http_status_code.h"
#include "brpc/server.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

void ClusterStatImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "Cluster";
  info->path = "/ClusterStat";
}

void ClusterStatImpl::default_method(::google::protobuf::RpcController* controller,
                                     const pb::cluster::ClusterStatRequest* /*request*/,
                                     pb::cluster::ClusterStatResponse* /*response*/,
                                     ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const bool is_html = brpc::UseHTML(cntl->http_request());

  cntl->http_response().set_content_type("text/html");

  butil::IOBufBuilder os;
  const std::string header_in_str = "Table and Regions In DingoStore Cluster";
  // If Current Request is HTML mode, then construct HTML HEADER
  os << "<!DOCTYPE html><html><head>\n"
     << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
     << "<title>DingoDB Cluster Stat Information</title>\n"
     << GetTabHead() << "</head><body>";
  cntl->server()->PrintTabsBody(os, "Cluster");

  os << "<h1 style=\"text-align: center;\">" << header_in_str << "</h1>";

  // 1. Get All Schema
  std::vector<pb::meta::Schema> schemas;
  controller_->GetSchemas(0, schemas);

  if (schemas.empty()) {
    DINGO_LOG(ERROR) << "Get Schemas Failed";
  }

  for (const auto& schema : schemas) {
    uint64_t const schema_id = schema.id().entity_id();
    if (schema_id == 0 || schema.name() != "dingo") {
      continue;
    }

    PrintSchema(os, schema.name());
    os << "<hr>";

    for (const auto& table_entry : schema.table_ids()) {
      // Start TableID
      os << "<ul>";
      auto table_id = table_entry.entity_id();
      os << "<li> TableID: " << std::to_string(table_id);
      // Start Append Table Information
      os << "<ul>";

      pb::meta::TableDefinitionWithId table_definition;
      controller_->GetTable(schema_id, table_id, table_definition);
      os << "<li> TableName:" << table_definition.table_definition().name() << "</li>";
      pb::meta::TableDefinition table_def = table_definition.table_definition();
      PrintTableDefinition(os, table_definition.table_definition());

      pb::common::RegionMap region_map;
      controller_->GetRegionMap(region_map);

      pb::meta::TableRange table_range;
      controller_->GetTableRange(schema_id, table_id, table_range);

      PrintTableRegions(os, region_map, table_range);
      // End Append Table Information
      os << "</ul>";
      os << "</li>";
      // End of TableID
      os << "</ul>";
    }
  }

  os << "</body></html>";
  os.move_to(cntl->response_attachment());
}

bool ClusterStatImpl::GetRegionInfo(uint64_t region_id, const pb::common::RegionMap& region_map,
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
  os << "<p style=\"text-align: center; font-size: 25px;\"><strong>Schema:" + schema_name + "</strong></p>";
}

void ClusterStatImpl::PrintRegionNode(std::ostream& os, const pb::common::Region& region) {
  std::string leader_info = "<li>Leader:";
  std::string follower_info = "<li>Follower:";
  std::string range_info = "<li>Range:[";
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

  range_info += region.definition().range().ShortDebugString();
  range_info += "] </li>";

  os << "<li>RegionID:";
  os << std::to_string(region.id());
  os << "<ul>";
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
    uint64_t const region_id = range_distribution.id().entity_id();
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
    os << "<li>" << column.name() << ":" << SqlType_Name(column.sql_type()) << "</li>";
  }

  os << "</ul>";
  os << "</li>";
}

}  // namespace dingodb
