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
#include "butil/compiler_specific.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"

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
  if (BAIDU_UNLIKELY(table_header.size() != min_widths.size())) {
    os << "! table_header.size(" << table_header.size() << ") == min_widths.size(" << min_widths.size() << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_contents.empty() && table_header.size() != table_contents[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_contents[0].size(" << table_contents[0].size()
       << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_urls.empty() && table_header.size() != table_urls[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_urls[0].size(" << table_urls[0].size() << ")";
    return;
  }

  if (BAIDU_LIKELY(use_html)) {
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
  coordinator_controller_->GetStoreMap(store_map);

  std::map<std::string, pb::common::Store> store_map_by_type_id;
  for (const auto& store : store_map.stores()) {
    store_map_by_type_id[std::to_string(store.store_type()) + "-" + std::to_string(store.id())] = store;
  }

  for (const auto& [key, store] : store_map_by_type_id) {
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
  coordinator_controller_->GetExecutorMap(executor_map);

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
  coordinator_controller_->GetSchemas(0, schemas);

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
      coordinator_controller_->GetTable(schema_id, table_id, table_definition);

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
      coordinator_controller_->GetTableRange(schema_id, table_id, table_range);

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
      coordinator_controller_->GetIndex(schema_id, table_id, false, table_definition);

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
      coordinator_controller_->GetIndexRange(schema_id, table_id, table_range);

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
    server->PrintTabsBody(os, "dingo");
  }

  int64_t cluster_id = 0;
  int64_t epoch = 0;
  pb::common::Location coordinator_leader_location;
  std::vector<pb::common::Location> locations;
  coordinator_controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations);

  pb::common::Location kv_leader_location;
  kv_controller_->GetLeaderLocation(kv_leader_location);

  pb::common::Location tso_leader_location;
  tso_controller_->GetLeaderLocation(tso_leader_location);

  pb::common::Location auto_increment_leader_location;
  auto_increment_controller_->GetLeaderLocation(auto_increment_leader_location);

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

    os << (use_html ? "<br>CoordinatorMap:\n" : "\n");
    for (const auto& location : locations) {
      os << "<a href=http://" + location.host() + ":" + std::to_string(location.port()) + "/dingo>" + location.host() +
                ":" + std::to_string(location.port()) + "</a>"
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

  std::vector<std::string> table_header;
  table_header.push_back("CONTROLLER");
  table_header.push_back("URL");
  std::vector<int32_t> min_widths;
  min_widths.push_back(10);
  min_widths.push_back(10);
  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  std::vector<std::string> line;
  std::vector<std::string> url_line;

  line.push_back("coordinator_control");
  url_line.push_back(std::string());
  line.push_back(coordinator_leader_location.host() + ":" + std::to_string(coordinator_leader_location.port()));
  url_line.push_back("http://" + coordinator_leader_location.host() + ":" +
                     std::to_string(coordinator_leader_location.port()) + "/CoordinatorService/GetMemoryInfo/");
  table_contents.push_back(line);
  table_urls.push_back(url_line);

  line.clear();
  url_line.clear();
  line.push_back("kv_control");
  url_line.push_back(std::string());
  line.push_back(kv_leader_location.host() + ":" + std::to_string(kv_leader_location.port()));
  url_line.push_back("http://" + kv_leader_location.host() + ":" + std::to_string(kv_leader_location.port()) +
                     "/VersionService/GetMemoryInfo/");
  table_contents.push_back(line);
  table_urls.push_back(url_line);

  line.clear();
  url_line.clear();
  line.push_back("tso_control");
  url_line.push_back(std::string());
  line.push_back(tso_leader_location.host() + ":" + std::to_string(tso_leader_location.port()));
  url_line.push_back("http://" + tso_leader_location.host() + ":" + std::to_string(tso_leader_location.port()) +
                     "/MetaService/GetTsoInfo/");
  table_contents.push_back(line);
  table_urls.push_back(url_line);

  line.clear();
  url_line.clear();
  line.push_back("auto_increment_control");
  url_line.push_back(std::string());
  line.push_back(auto_increment_leader_location.host() + ":" + std::to_string(auto_increment_leader_location.port()));
  url_line.push_back("http://" + auto_increment_leader_location.host() + ":" +
                     std::to_string(auto_increment_leader_location.port()) + "/MetaService/GetMemoryInfo/");
  table_contents.push_back(line);
  table_urls.push_back(url_line);

  PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);

  os << (use_html ? "<br>\n" : "\n");
  os << (use_html ? "<br>\n" : "\n");
  os << "<span class=\"bold-text\">STORE: </span>" << '\n';
  PrintStores(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "<span class=\"bold-text\">EXECUTOR: </span>" << '\n';
  PrintExecutors(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "<span class=\"bold-text\">TABLE: </span>" << '\n';
  PrintSchemaTables(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  os << "<span class=\"bold-text\">REGION: </span>" << '\n';
  PrintRegions(os, use_html);

  if (use_html) {
    os << "</body></html>\n";
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void ClusterStatImpl::SetControl(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                 std::shared_ptr<KvControl> kv_controller, std::shared_ptr<TsoControl> tso_controller,
                                 std::shared_ptr<AutoIncrementControl> auto_increment_controller) {
  coordinator_controller_ = coordinator_controller;
  kv_controller_ = kv_controller;
  tso_controller_ = tso_controller;
  auto_increment_controller_ = auto_increment_controller;
}

}  // namespace dingodb
