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
#include "brpc/http_status_code.h"
#include "brpc/server.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "vector/codec.h"

namespace dingodb {

void ClusterStatImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "dingo";
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

  uint64_t cluster_id = 0;
  uint64_t epoch = 0;
  pb::common::Location coordinator_leader_location;
  std::vector<pb::common::Location> locations;
  controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations);

  pb::common::StoreMap store_map;
  controller_->GetStoreMap(store_map);

  butil::IOBufBuilder os;
  const std::string header_in_str = "Cluster Information (" + std::string(GIT_VERSION) + ")";
  // If Current Request is HTML mode, then construct HTML HEADER
  os << "<!DOCTYPE html><html><head>\n"
     << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
     << "<title>DingoDB Cluster Stat Information</title>\n"
     << GetTabHead() << "</head><body>";
  cntl->server()->PrintTabsBody(os, "Cluster");

  os << "<h1 style=\"text-align: center;\">" << header_in_str << "</h1>";

  os << "<p style=\"text-align: center; \">Coordinator Leader:<a href=http://" + coordinator_leader_location.host() +
            ":" + std::to_string(coordinator_leader_location.port()) + "/ClusterStat>" +
            coordinator_leader_location.host() + ":" + std::to_string(coordinator_leader_location.port()) + "</a></p>";

  for (const auto& store : store_map.stores()) {
    os << "<p style=\"text-align: center; \">" + pb::common::StoreType_Name(store.store_type()) + ":" +
              std::to_string(store.id()) + " " + pb::common::StoreState_Name(store.state()) + " <a href=http://" +
              store.server_location().host() + ":" + std::to_string(store.server_location().port()) +
              "> server:" + store.server_location().host() + ":" + std::to_string(store.server_location().port()) +
              "</a> <a href=http://" + store.raft_location().host() + ":" +
              std::to_string(store.raft_location().port()) + "> raft:" + store.raft_location().host() + ":" +
              std::to_string(store.raft_location().port()) + "</a></p>";
  }

  // 1. Get All Schema
  std::vector<pb::meta::Schema> schemas;
  controller_->GetSchemas(0, schemas);

  if (schemas.empty()) {
    DINGO_LOG(ERROR) << "Get Schemas Failed";
  }

  for (const auto& schema : schemas) {
    uint64_t const schema_id = schema.id().entity_id();
    if (schema_id == 0 || schema.name() != Constant::kDingoSchemaName) {
      continue;
    }

    PrintSchema(os, schema.name());
    os << "<hr>";

    pb::common::RegionMap region_map;
    controller_->GetRegionMapFull(region_map);

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

      pb::meta::TableRange table_range;
      controller_->GetTableRange(schema_id, table_id, table_range);

      PrintTableRegions(os, region_map, table_range);
      // End Append Table Information
      os << "</ul>";
      os << "</li>";
      // End of TableID
      os << "</ul>";
    }

    for (const auto& index_entry : schema.index_ids()) {
      // Start IndexID
      os << "<ul>";
      auto index_id = index_entry.entity_id();
      os << "<li> IndexID: " << std::to_string(index_id);
      // Start Append Index Information
      os << "<ul>";

      pb::meta::TableDefinitionWithId table_definition;
      controller_->GetIndex(schema_id, index_id, false, table_definition);
      os << "<li> IndexName:" << table_definition.table_definition().name() << "</li>";
      PrintIndexDefinition(os, table_definition.table_definition());

      pb::meta::IndexRange index_range;
      controller_->GetIndexRange(schema_id, index_id, index_range);

      PrintIndexRegions(os, region_map, index_range);
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

  std::string raw_range_info;
  if (region.definition().index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
    raw_range_info = fmt::format("<li>RawRange:[{}, {}); [{}/{}, {}/{})</li>",
                                 Helper::StringToHex(region.definition().raw_range().start_key()),
                                 Helper::StringToHex(region.definition().raw_range().end_key()),
                                 VectorCodec::DecodePartitionId(region.definition().raw_range().start_key()),
                                 VectorCodec::DecodeVectorId(region.definition().raw_range().start_key()),
                                 VectorCodec::DecodePartitionId(region.definition().raw_range().end_key()),
                                 VectorCodec::DecodeVectorId(region.definition().raw_range().end_key()));
  } else {
    raw_range_info =
        fmt::format("<li>RawRange:[{}, {})</li>", Helper::StringToHex(region.definition().raw_range().start_key()),
                    Helper::StringToHex(region.definition().raw_range().end_key()));
  }

  os << "<li>RegionID:";
  os << std::to_string(region.id());
  os << "<ul>";
  os << epoch_info;
  os << create_time_info;
  os << leader_info;
  os << follower_info;
  os << range_info;
  os << raw_range_info;
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
    os << "<li>" << column.name() << ":" << column.sql_type() << "</li>";
  }

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
}

void ClusterStatImpl::PrintIndexRegions(std::ostream& os, const pb::common::RegionMap& region_map,
                                        const pb::meta::IndexRange& index_range) {
  os << "<li> Regions: ";
  os << "<ul>";
  for (const auto& range_distribution : index_range.range_distribution()) {
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

}  // namespace dingodb
