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
#include "common/helper.h"
#include "proto/common.pb.h"

namespace dingodb {

void ClusterStatImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "dingo";
  info->path = "/dingo";
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
  table_header.push_back("METRICS_C");
  table_header.push_back("OPERATION");
  table_header.push_back("METRICS_S");
  table_header.push_back("MEMORY");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // ID
  min_widths.push_back(16);  // TYPE
  min_widths.push_back(16);  // STATE
  min_widths.push_back(10);  // IN_STATE
  min_widths.push_back(30);  // SERVER_LOCATION
  min_widths.push_back(30);  // RAFT_LOCATION
  min_widths.push_back(15);  // RESOURCE_TAG
  min_widths.push_back(20);  // CREATE_TIME
  min_widths.push_back(30);  // UPDATE_TIME
  min_widths.push_back(10);  // METRICS_C
  min_widths.push_back(10);  // OPERATION
  min_widths.push_back(5);   // METRICS_S
  min_widths.push_back(10);  // MEMORY

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
    line.push_back(std::to_string(store.id()));  // METRICS_C
    url_line.push_back("/store_metrics/");
    line.push_back(std::to_string(store.id()));  // OPERATION
    url_line.push_back("/store_operation/");
    line.push_back(std::to_string(store.id()) + "/metrics");  // METRICS_S
    if (store.store_type() == pb::common::StoreType::NODE_TYPE_STORE) {
      url_line.push_back("http://" + store.server_location().host() + ":" +
                         std::to_string(store.server_location().port()) + "/StoreService/GetMemoryInfo/");
    } else if (store.store_type() == pb::common::StoreType::NODE_TYPE_INDEX) {
      url_line.push_back("http://" + store.server_location().host() + ":" +
                         std::to_string(store.server_location().port()) + "/IndexService/GetMemoryInfo/");
    } else {
      url_line.push_back(std::string());
    }

    line.push_back(std::to_string(store.id()) + "/memory");  // MEMORY
    url_line.push_back("http://" + store.server_location().host() + ":" +
                       std::to_string(store.server_location().port()) + "/memory/");

    table_contents.push_back(line);
    table_urls.push_back(url_line);
  }

  os << "<span class=\"bold-text\">STORE: " << table_contents.size() << "</span>" << '\n';
  Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
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
  min_widths.push_back(20);  // CREATE_TIME
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

  os << "<span class=\"bold-text\">EXECUTOR: " << table_contents.size() << "</span>" << '\n';
  Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
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
  pb::common::CoordinatorMap coordinator_map;
  coordinator_controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations, coordinator_map);

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
    os << "<a href=\"/store_operation/"
       << "\">"
       << "GET_OPERATION"
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

  {
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

    Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
  }

  os << (use_html ? "<br>\n" : "\n");
  os << (use_html ? "<br>\n" : "\n");
  PrintStores(os, use_html);

  os << (use_html ? "<br>\n" : "\n");
  PrintExecutors(os, use_html);

  {
    os << (use_html ? "<br>\n" : "\n");
    std::vector<std::string> table_header;
    table_header.push_back("META");
    table_header.push_back("URL");
    std::vector<int32_t> min_widths;
    min_widths.push_back(10);
    min_widths.push_back(10);
    std::vector<std::vector<std::string>> table_contents;
    std::vector<std::vector<std::string>> table_urls;

    std::vector<std::string> line;
    std::vector<std::string> url_line;

    int64_t schema_count = 0;
    int64_t table_count = 0;
    int64_t index_count = 0;
    int64_t region_count = 0;

    coordinator_controller_->GetMetaCount(schema_count, table_count, index_count, region_count);

    line.clear();
    url_line.clear();
    line.push_back("SCHEMA: " + std::to_string(schema_count));
    url_line.push_back(std::string());
    line.push_back("table");
    url_line.push_back("http://" + coordinator_leader_location.host() + ":" +
                       std::to_string(coordinator_leader_location.port()) + "/table");
    table_contents.push_back(line);
    table_urls.push_back(url_line);

    line.clear();
    url_line.clear();
    line.push_back("TABLE: " + std::to_string(table_count));
    url_line.push_back(std::string());
    line.push_back("table");
    url_line.push_back("http://" + coordinator_leader_location.host() + ":" +
                       std::to_string(coordinator_leader_location.port()) + "/table");
    table_contents.push_back(line);
    table_urls.push_back(url_line);

    line.clear();
    url_line.clear();
    line.push_back(" INDEX: " + std::to_string(index_count));
    url_line.push_back(std::string());
    line.push_back("table");
    url_line.push_back("http://" + coordinator_leader_location.host() + ":" +
                       std::to_string(coordinator_leader_location.port()) + "/table");
    table_contents.push_back(line);
    table_urls.push_back(url_line);

    line.clear();
    url_line.clear();
    line.push_back("REGION: " + std::to_string(region_count));
    url_line.push_back(std::string());
    line.push_back("region");
    url_line.push_back("http://" + coordinator_leader_location.host() + ":" +
                       std::to_string(coordinator_leader_location.port()) + "/region");
    table_contents.push_back(line);
    table_urls.push_back(url_line);

    Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
  }

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
