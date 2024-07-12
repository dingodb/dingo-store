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

#include "client_v2/pretty.h"

#include <iostream>
#include <string>
#include <vector>

#include "client_v2/helper.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "ftxui/dom/deprecated.hpp"
#include "ftxui/dom/elements.hpp"
#include "ftxui/dom/node.hpp"
#include "ftxui/dom/table.hpp"
#include "ftxui/screen/color.hpp"
#include "ftxui/screen/screen.hpp"

namespace client_v2 {

static bool PrintError(const dingodb::pb::error::Error& error) {
  if (error.errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << fmt::format("Error: {} {}",
                             dingodb::pb::error::Errno_descriptor()->FindValueByNumber(error.errcode())->name(),
                             error.errmsg())
              << std::endl;
    return true;
  }

  return false;
}

static void PrintTable(const std::vector<std::vector<std::string>>& rows) {
  if (rows.empty()) {
    return;
  }

  int clounm_num = rows[0].size();

  auto table = ftxui::Table(rows);

  table.SelectAll().Border(ftxui::LIGHT);
  table.SelectAll().Separator(ftxui::LIGHT);

  // Make first row bold with a double border.
  table.SelectRow(0).Decorate(ftxui::bold);
  table.SelectRow(0).SeparatorVertical(ftxui::LIGHT);
  table.SelectRow(0).Border(ftxui::DOUBLE);

  auto document = table.Render();
  auto screen = ftxui::Screen::Create(ftxui::Dimension::Fit(document));
  ftxui::Render(screen, document);
  screen.Print();

  std::cout << std::endl;
}

static void PrintTable(const std::vector<std::vector<ftxui::Element>>& rows) {
  if (rows.empty()) {
    return;
  }

  int clounm_num = rows[0].size();

  auto table = ftxui::Table(rows);

  table.SelectAll().Border(ftxui::LIGHT);
  table.SelectAll().Separator(ftxui::LIGHT);

  // Make first row bold with a double border.
  table.SelectRow(0).Decorate(ftxui::bold);
  table.SelectRow(0).SeparatorVertical(ftxui::LIGHT);
  table.SelectRow(0).Border(ftxui::DOUBLE);

  auto document = table.Render();
  auto screen = ftxui::Screen::Create(ftxui::Dimension::Fit(document));
  ftxui::Render(screen, document);
  screen.Print();

  std::cout << std::endl;
}

void Pretty::Show(dingodb::pb::coordinator::GetCoordinatorMapResponse& response) {
  if (PrintError(response.error())) {
    return;
  }

  std::vector<std::vector<std::string>> rows = {
      {"Type", "Address", "ID", "State"},
      {"CoorLeader", dingodb::Helper::LocationToString(response.leader_location()), "", ""},
      {"KvLeader", dingodb::Helper::LocationToString(response.kv_leader_location()), "", ""},
      {"TsoLeader", dingodb::Helper::LocationToString(response.tso_leader_location()), "", ""},
      {"AutoIncLeader", dingodb::Helper::LocationToString(response.auto_increment_leader_location()), "", ""}};

  for (const auto& coor : response.coordinator_map().coordinators()) {
    std::vector<std::string> row = {
        "Coordinator",
        dingodb::Helper::LocationToString(coor.location()),
        std::to_string(coor.id()),
        dingodb::pb::common::CoordinatorState_Name(coor.state()),
    };
    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::coordinator::GetStoreMapResponse& response) {
  if (PrintError(response.error())) {
    return;
  }

  std::vector<std::vector<std::string>> rows = {
      {"ID", "Type", "Address", "State", "InState", "CreateTime", "LastSeenTime"}};

  std::map<dingodb::pb::common::StoreType, int> counts;
  for (const auto& store : response.storemap().stores()) {
    std::vector<std::string> row = {
        std::to_string(store.id()),
        dingodb::pb::common::StoreType_Name(store.store_type()),
        dingodb::Helper::LocationToString(store.server_location()),
        dingodb::pb::common::StoreState_Name(store.state()),
        dingodb::pb::common::StoreInState_Name(store.in_state()),
        dingodb::Helper::FormatMsTime(store.create_timestamp()),
        dingodb::Helper::FormatMsTime(store.last_seen_timestamp()),
    };

    if (counts.find(store.store_type()) == counts.end()) {
      counts.insert_or_assign(store.store_type(), 0);
    }

    ++counts[store.store_type()];

    rows.push_back(row);
  }

  PrintTable(rows);

  // print summary
  std::string summary = "Summary:";
  for (auto& [type, count] : counts) {
    summary += fmt::format(" {}({})", dingodb::pb::common::StoreType_Name(type), count);
  }
  std::cout << summary << std::endl;
}

void Pretty::Show(dingodb::pb::debug::DumpRegionResponse& response) {
  if (PrintError(response.error())) {
    return;
  }

  if (!response.data().kvs().empty()) {
    std::vector<std::vector<std::string>> rows;
    rows = {{"Key", "Ts", "Flag", "Ttl", "Value"}};

    for (const auto& kv : response.data().kvs()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(kv.flag());

      rows.push_back({dingodb::Helper::StringToHex(kv.key()), std::to_string(kv.ts()), flag, std::to_string(kv.ttl()),
                      kv.value().substr(0, 32)

      });
    }

    PrintTable(rows);
  } else if (!response.data().vectors().empty()) {
    std::vector<std::vector<ftxui::Element>> rows;
    rows = {{ftxui::paragraph("ID"), ftxui::paragraph("Ts"), ftxui::paragraph("Flag"), ftxui::paragraph("Ttl"),
             ftxui::paragraph("Vector"), ftxui::paragraph("Scalar"), ftxui::paragraph("Table")}};

    for (const auto& vector : response.data().vectors()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(vector.flag());

      // scalar data
      auto lines = client_v2::Helper::FormatVectorScalar(vector.scalar_data());
      std::vector<ftxui::Element> scalar_elems;
      scalar_elems.reserve(lines.size());
      for (auto& line : lines) {
        scalar_elems.push_back(ftxui::paragraph(line));
      }

      // table data
      auto table_elem =
          ftxui::vflow({ftxui::paragraph("key: " + dingodb::Helper::StringToHex(vector.table_data().table_key())),
                        ftxui::paragraph("value: " + dingodb::Helper::StringToHex(vector.table_data().table_value()))});

      rows.push_back({ftxui::paragraph(std::to_string(vector.vector_id())),
                      ftxui::paragraph(std::to_string(vector.ts())), ftxui::paragraph(flag),
                      ftxui::paragraph(std::to_string(vector.ttl())),
                      ftxui::paragraph(client_v2::Helper::FormatVectorData(vector.vector())),
                      ftxui::vflow(scalar_elems), table_elem});
    }

    PrintTable(rows);
  } else if (!response.data().documents().empty()) {
    std::vector<std::vector<std::string>> rows;
    rows = {{"ID", "Ts", "Flag", "Ttl", "Data"}};

    for (const auto& document : response.data().documents()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(document.flag());

      rows.push_back({std::to_string(document.document_id()), std::to_string(document.ts()), flag,
                      std::to_string(document.ttl()), client_v2::Helper::FormatDocument(document.document())});
    }

    PrintTable(rows);
  }

  // print summary
  int size = std::max(response.data().kvs_size(), response.data().vectors_size());
  size = std::max(size, response.data().documents_size());

  std::cout << fmt::format("Summary: total count({})", size) << std::endl;
}

}  // namespace client_v2