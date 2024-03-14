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

#include "server/region_service.h"

#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"

namespace dingodb {

void RegionImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "region";
  info->path = "/region";
}

void RegionImpl::default_method(google::protobuf::RpcController* controller,
                                const pb::cluster::RegionRequest* /*request*/,
                                pb::cluster::RegionResponse* /*response*/, google::protobuf::Closure* done) {
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
      server->PrintTabsBody(os, "region");
    } else {
      os << "# Use /region/<RegionId>\n";  // << butil::describe_resources<Socket>() << '\n';
    }

    os << "DINGO_STORE VERSION: " << std::string(GIT_VERSION) << '\n';

    int64_t epoch = 0;
    pb::common::Location coordinator_leader_location;
    std::vector<pb::common::Location> locations;
    pb::common::CoordinatorMap coordinator_map;
    coordinator_controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations, coordinator_map);

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
    PrintRegions(os, use_html);
  } else {
    char* endptr = nullptr;
    int64_t region_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "RegionId=" << region_id << '\n';

      pb::common::Region region;
      auto ret = coordinator_controller_->QueryRegion(region_id, region);

      if (region.id() == 0) {
        os << "Region is not found" << '\n';
      } else {
        os << "================ Region: ================" << '\n';
        os << region.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a RegionId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

void RegionImpl::PrintRegions(std::ostream& os, bool use_html) {
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
  table_header.push_back("REGION_ID");
  table_header.push_back("RAFT_STATE");
  table_header.push_back("READONLY");
  table_header.push_back("TERM");
  table_header.push_back("APPLIED_INDEX");
  table_header.push_back("COMITTED_INDEX");
  table_header.push_back("FIRST_INDEX");
  table_header.push_back("LAST_INDEX");
  table_header.push_back("DISK_INDEX");
  table_header.push_back("PENDING_INDEX");
  table_header.push_back("PENDING_QUEUE_SIZE");
  table_header.push_back("STABLE_FOLLOWERS");
  table_header.push_back("UNSTABLE_FOLLOWERS");
  table_header.push_back("REGION_ID");
  table_header.push_back("METIRCS_INDEX");
  table_header.push_back("REGION_SIZE");
  table_header.push_back("VECTOR_TYPE");
  table_header.push_back("VECTOR_SNAPSHOT_LOG_ID");
  table_header.push_back("VECTOR_APPLY_LOG_ID");
  table_header.push_back("VECTOR_BUILD_EPOCH");
  table_header.push_back("IS_STOP");
  table_header.push_back("IS_READY");
  table_header.push_back("IS_OWN_READY");
  table_header.push_back("IS_SWITCHING");
  table_header.push_back("BUILD_ERROR");
  table_header.push_back("REBUILD_ERROR");

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
  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(10);  // RAFT_STATE
  min_widths.push_back(10);  // READONLY
  min_widths.push_back(10);  // TERM
  min_widths.push_back(10);  // APPLIED_INDEX
  min_widths.push_back(10);  // COMITTED_INDEX
  min_widths.push_back(10);  // FIRST_INDEX
  min_widths.push_back(10);  // LAST_INDEX
  min_widths.push_back(10);  // DISK_INDEX
  min_widths.push_back(10);  // PENDING_INDEX
  min_widths.push_back(10);  // PENDING_QUEUE_SIZE
  min_widths.push_back(10);  // STABLE_FOLLOWERS
  min_widths.push_back(10);  // UNSTABLE_FOLLOWERS
  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(10);  // METIRCS_INDEX
  min_widths.push_back(10);  // REGION_SIZE
  min_widths.push_back(10);  // VECTOR_TYPE
  min_widths.push_back(10);  // VECTOR_SNAPSHOT_LOG_ID
  min_widths.push_back(10);  // VECTOR_APPLY_LOG_ID
  min_widths.push_back(10);  // VECTOR_BUILD_EPOCH
  min_widths.push_back(10);  // IS_STOP
  min_widths.push_back(10);  // IS_READY
  min_widths.push_back(10);  // IS_OWN_READY
  min_widths.push_back(10);  // IS_SWITCHING
  min_widths.push_back(10);  // BUILD_ERROR
  min_widths.push_back(10);  // REBUILD_ERROR

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  pb::common::RegionMap region_map;
  coordinator_controller_->GetRegionMapFull(region_map);

  for (const auto& region : region_map.regions()) {
    std::vector<std::string> line;
    std::vector<std::string> url_line;

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    line.push_back(region.definition().name());  // REGION_NAME
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().epoch().conf_version()) + "-" +
                   std::to_string(region.definition().epoch().version()));  // EPOCH
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionType_Name(region.region_type()));  // REGION_TYPE
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionState_Name(region.state()));  // REGION_STATE
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionRaftStatus_Name(region.status().raft_status()));  // BRAFT_STATUS
    url_line.push_back(std::string());

    line.push_back(pb::common::ReplicaStatus_Name(region.status().replica_status()));  // REPLICA_STATUS
    url_line.push_back(std::string());

    line.push_back(pb::common::StoreRegionState_Name(region.metrics().store_region_state()));  // STORE_REGION_STATE
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.leader_store_id()));  // LEADER_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().peers_size()));  // REPLICA
    url_line.push_back(std::string());

    line.push_back(Helper::StringToHex(region.definition().range().start_key()));  // START_KEY
    url_line.push_back(std::string());

    line.push_back(Helper::StringToHex(region.definition().range().end_key()));  // END_KEY
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().schema_id()));  // SCHEMA_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().table_id()));  // TABLE_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().index_id()));  // INDEX_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().part_id()));  // PART_ID
    url_line.push_back(std::string());

    line.push_back(pb::common::RawEngine_Name(region.definition().raw_engine()));  // ENGINE
    url_line.push_back(std::string());

    line.push_back(Helper::FormatMsTime(region.create_timestamp(), "%Y-%m-%d %H:%M:%S"));  // CREATE_TIME
    url_line.push_back(std::string());

    line.push_back(
        Helper::FormatMsTime(region.metrics().last_update_metrics_timestamp(), "%Y-%m-%d %H:%M:%S"));  // UPDATE_TIME
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    line.push_back(pb::common::RaftNodeState_Name(region.metrics().braft_status().raft_state()));  // RAFT_STATE
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().readonly()));  // READONLY
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().term()));  // TERM
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().known_applied_index()));  // APPLIED_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().committed_index()));  // COMITTED_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().first_index()));  // FIRST_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().last_index()));  // LAST_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().disk_index()));  // DISK_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().pending_index()));  // PENDING_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().pending_queue_size()));  // PENDING_QUEUE_SIZE
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().stable_followers().size()));  // STABLE_FOLLOWERS
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().braft_status().unstable_followers().size()));  // UNSTABLE_FOLLOWERS
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    line.push_back(std::to_string(region.metrics().last_update_metrics_log_index()));  // METIRCS_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().region_size()));  // REGION_SIZE
    url_line.push_back(std::string());

    auto vector_index_type = region.definition().index_parameter().vector_index_parameter().vector_index_type();
    if (vector_index_type != pb::common::VECTOR_INDEX_TYPE_NONE) {
      line.push_back(pb::common::VectorIndexType_Name(vector_index_type));  // VECTOR_TYPE
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().vector_index_status().snapshot_log_id()));  // VECTOR_SNAPSHOT_LOG_ID
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().apply_log_id()));  // VECTOR_APPLY_LOG_ID
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().vector_index_status().last_build_epoch_version()));  // VECTOR_BUILD_EPOCH
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_stop()));  // IS_STOP
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_ready()));  // IS_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_own_ready()));  // IS_OWN_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_switching()));  // IS_SWITCHING
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_build_error()));  // BUILD_ERROR
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_rebuild_error()));  // REBUILD_ERROR
      url_line.push_back(std::string());

    } else {
      line.push_back("N/A");  // VECTOR_TYPE
      url_line.push_back(std::string());

      line.push_back("N/A");  // VECTOR_SNAPSHOT_LOG_ID
      url_line.push_back(std::string());

      line.push_back("N/A");  // VECTOR_APPLY_LOG_ID
      url_line.push_back(std::string());

      line.push_back("N/A");  // VECTOR_BUILD_EPOCH
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_STOP
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_READY
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_OWN_READY
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_SWITCHING
      url_line.push_back(std::string());

      line.push_back("N/A");  // BUILD_ERROR
      url_line.push_back(std::string());

      line.push_back("N/A");  // REBUILD_ERROR
      url_line.push_back(std::string());
    }

    table_contents.push_back(line);
    table_urls.push_back(url_line);
  }

  if (use_html) {
    os << "<span class=\"bold-text\">REGION: " << table_contents.size() << "</span>" << '\n';
  }

  Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
}

}  // namespace dingodb
