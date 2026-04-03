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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/table.hpp>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client_v2/helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/tso_control.h"
#include "coprocessor/utils.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "ftxui/component/component.hpp"
#include "ftxui/component/screen_interactive.hpp"
#include "ftxui/dom/deprecated.hpp"
#include "ftxui/dom/elements.hpp"
#include "ftxui/dom/node.hpp"
#include "ftxui/dom/table.hpp"
#include "ftxui/screen/color.hpp"
#include "ftxui/screen/screen.hpp"
#include "glog/logging.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/utils.h"
#include "vector/codec.h"
namespace client_v2 {

const uint32_t kStringReserveSize = 32;

static void PrintTableAdaptive(const std::vector<std::vector<std::string>>& rows);

std::string TruncateString(const std::string& str) {
  if (str.size() <= kStringReserveSize) {
    return str;
  }

  return str.substr(0, kStringReserveSize) + "...";
}

std::string TruncateHexString(const std::string& str) {
  auto hex_str = dingodb::Helper::StringToHex(str);
  if (hex_str.size() <= kStringReserveSize) {
    return hex_str;
  }

  return hex_str.substr(0, kStringReserveSize) + "...";
}

bool Pretty::ShowError(const butil::Status& status) {
  if (status.error_code() != dingodb::pb::error::Errno::OK) {
    std::cout << fmt::format("Error: {} {}", dingodb::pb::error::Errno_Name(status.error_code()), status.error_str())
              << std::endl;
    return true;
  }

  return false;
}

bool Pretty::ShowError(const dingodb::pb::error::Error& error) {
  if (error.errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << fmt::format("Error: {} {}", dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg())
              << std::endl;
    return true;
  }

  return false;
}

static void PrintTable(const std::vector<std::vector<std::string>>& rows) {
  if (rows.empty()) {
    return;
  }
  std::cout << std::endl;
  int clounm_num = rows[0].size();

  auto table = ftxui::Table(rows);

  table.SelectAll().Border(ftxui::LIGHT);
  // table.SelectAll().Separator(ftxui::LIGHT);

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

static void PrintTableAdaptive(const std::vector<std::vector<std::string>>& rows) {
  if (rows.empty()) return;
  std::cout << std::endl;

  size_t col_count = rows[0].size();

  // 1. Calculate the maximum content width for each column
  std::vector<size_t> col_widths(col_count, 0);
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size() && i < col_count; ++i) {
      col_widths[i] = std::max(col_widths[i], row[i].size());
    }
  }

  // 2. Detect whether output is to a terminal
  bool is_tty = isatty(STDOUT_FILENO);

  if (!is_tty) {
    // When outputting to a file, use plain text format to ensure complete content
    // Print top border
    std::cout << "+";
    for (size_t i = 0; i < col_count; ++i) {
      std::cout << std::string(col_widths[i] + 2, '-');
      std::cout << "+";
    }
    std::cout << "\n";

    // Print each row
    for (size_t r = 0; r < rows.size(); ++r) {
      std::cout << "|";
      for (size_t i = 0; i < col_count; ++i) {
        std::string cell = (i < rows[r].size()) ? rows[r][i] : "";
        std::cout << " " << std::left << std::setw(col_widths[i]) << cell << " |";
      }
      std::cout << "\n";

      // Print separator line after header
      if (r == 0) {
        std::cout << "+";
        for (size_t i = 0; i < col_count; ++i) {
          std::cout << std::string(col_widths[i] + 2, '=');
          std::cout << "+";
        }
        std::cout << "\n";
      }
    }

    // Print bottom border
    std::cout << "+";
    for (size_t i = 0; i < col_count; ++i) {
      std::cout << std::string(col_widths[i] + 2, '-');
      std::cout << "+";
    }
    std::cout << "\n" << std::endl;
    return;
  }

  // When outputting to terminal, use FTXUI rendering
  bool need_truncate = false;
  struct winsize w {};
  int terminal_width = 120;  // Default width
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
    terminal_width = static_cast<int>(w.ws_col);
  }

  // Calculate total width, scale proportionally if exceeds terminal width
  size_t total_width = 0;
  for (auto cw : col_widths) total_width += cw;
  total_width += col_count * 3 + 4;  // Borders and separators

  if (total_width > static_cast<size_t>(terminal_width)) {
    need_truncate = true;
    double scale = static_cast<double>(terminal_width - col_count * 3 - 4) / (total_width - col_count * 3 - 4);
    for (auto& cw : col_widths) {
      cw = std::max(static_cast<size_t>(3), static_cast<size_t>(cw * scale));
    }
  }

  // Build Element table with fixed column widths
  std::vector<std::vector<ftxui::Element>> elements;
  for (const auto& row : rows) {
    std::vector<ftxui::Element> element_row;
    for (size_t i = 0; i < col_count; ++i) {
      std::string cell_content = (i < row.size()) ? row[i] : "";
      if (need_truncate) {
        element_row.push_back(ftxui::text(cell_content) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, col_widths[i]));
      } else {
        element_row.push_back(ftxui::text(cell_content) |
                              ftxui::size(ftxui::WIDTH, ftxui::GREATER_THAN, col_widths[i]));
      }
    }
    elements.push_back(std::move(element_row));
  }

  auto table = ftxui::Table(elements);
  table.SelectAll().Border(ftxui::LIGHT);
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
  std::cout << std::endl;
  int clounm_num = rows[0].size();

  auto table = ftxui::Table(rows);

  table.SelectAll().Border(ftxui::LIGHT);
  // table.SelectAll().Separator(ftxui::LIGHT);

  // Make first row bold with a double border.
  table.SelectRow(0).Decorate(ftxui::bold);
  table.SelectRow(0).SeparatorVertical(ftxui::LIGHT);
  table.SelectRow(0).Border(ftxui::DOUBLE);
  table.SelectRow(0).DecorateCells(color(ftxui::Color::Green));
  table.SelectRow(0).Decorate(color(ftxui::Color::Green));

  auto document = table.Render();
  auto screen = ftxui::Screen::Create(ftxui::Dimension::Fit(document));
  ftxui::Render(screen, document);
  screen.Print();

  std::cout << std::endl;
}

static int GetTerminalHeightFallback() {
  struct winsize w {};
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_row > 0) return static_cast<int>(w.ws_row);
  return 24;
}

void Pretty::PrintTableInteractive(const std::vector<std::vector<ftxui::Element>>& rows) {
  using namespace ftxui;

  if (rows.empty()) return;

  auto header = rows[0];
  std::vector<std::vector<Element>> body(rows.begin() + 1, rows.end());
  int scroll_y = 0;

  auto screen = ScreenInteractive::Fullscreen();

  // 🧮 Intelligent detection of row count in non-table areas
  static int non_table_lines = -1;  // -1 It indicates that testing has not yet been conducted.
  if (non_table_lines == -1) {
    auto sample_table = Table({header, body.front()});
    auto doc = vbox({
        text("📊 Interactive Table Viewer") | color(ftxui::Color::Green),
        separator(),
        sample_table.Render(),
        separator(),
        text("↑↓ scroll | PageUp/PageDown Turn the page | (H/h)Home/(E/e)End Jump | q Exit"),
        text("show: 1/1"),
    });
    auto sample_screen = Screen::Create(Dimension::Fit(doc));
    Render(sample_screen, doc);
    int total_terminal = GetTerminalHeightFallback();
    non_table_lines = std::max(11, sample_screen.dimy() - 2);  // Dynamic estimation
  }

  auto renderer = Renderer([&] {
    int terminal_height = screen.dimy();
    if (terminal_height <= 4) terminal_height = GetTerminalHeightFallback();

    int visible_body_rows = std::max(1, terminal_height - non_table_lines);

    int total_body = static_cast<int>(body.size());

    if (scroll_y < 0) scroll_y = 0;
    if (scroll_y > std::max(0, total_body - visible_body_rows)) scroll_y = std::max(0, total_body - visible_body_rows);

    int start = scroll_y;
    int end = std::min(total_body, start + visible_body_rows);

    std::vector<std::vector<Element>> visible_part;
    visible_part.push_back(header);
    for (int i = start; i < end; ++i) visible_part.push_back(body[i]);

    auto table = Table(visible_part);
    table.SelectAll().Border(LIGHT);
    table.SelectRow(0).Decorate(bold);
    table.SelectRow(0).Border(DOUBLE);
    table.SelectRow(0).SeparatorVertical(LIGHT);
    table.SelectRow(0).DecorateCells(color(ftxui::Color::Green));
    table.SelectRow(0).Decorate(color(ftxui::Color::Green));

    return vbox({
               text("📊 Interactive Table Viewer") | bold | center | color(ftxui::Color::Green),
               separator(),
               table.Render() | flex,
               separator(),
               text("↑↓ scroll | PageUp/PageDown Turn the page | (H/h)Home/(E/e)End Jump | q Exit") | dim | center,
               text("show: " + std::to_string(start + 1) + "-" + std::to_string(end) + " / " +
                    std::to_string(total_body)) |
                   dim | center,
           }) |
           border;
  });

  auto event_handler = CatchEvent(renderer, [&](Event event) {
    int terminal_height = screen.dimy();
    if (terminal_height <= 4) terminal_height = GetTerminalHeightFallback();

    int visible_body_rows = std::max(1, terminal_height - non_table_lines);
    int total_body = static_cast<int>(body.size());

    if (event == Event::ArrowUp) {
      if (scroll_y > 0) --scroll_y;
      return true;
    } else if (event == Event::ArrowDown) {
      if (scroll_y + visible_body_rows < total_body) ++scroll_y;
      return true;
    } else if (event == Event::PageUp) {
      scroll_y -= visible_body_rows;
      if (scroll_y < 0) scroll_y = 0;
      return true;
    } else if (event == Event::PageDown) {
      scroll_y += visible_body_rows;
      if (scroll_y > total_body - visible_body_rows) scroll_y = std::max(0, total_body - visible_body_rows);
      return true;
    } else if (event == Event::Home || event == Event::Special("h") || event == Event::Special("H")) {
      scroll_y = 0;
      return true;
    } else if (event == Event::End || event == Event::Special("e") || event == Event::Special("E")) {
      scroll_y = std::max(0, total_body - visible_body_rows);
      return true;
    } else if (event == Event::Character('q') || event == Event::Escape) {
      screen.ExitLoopClosure()();
      return true;
    }
    return false;
  });

  screen.PostEvent(Event::Custom);
  screen.Loop(event_handler);
}

void Pretty::Show(dingodb::pb::coordinator::GetCoordinatorMapResponse& response) {
  if (ShowError(response.error())) {
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
  if (ShowError(response.error())) {
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

  // PrintTable(rows);

  PrintTableAdaptive(rows);

  // print summary
  std::string summary = "Summary:";
  for (auto& [type, count] : counts) {
    summary += fmt::format(" {}({})", dingodb::pb::common::StoreType_Name(type), count);
  }
  std::cout << summary << std::endl;
}

static bool IsExcludeColumns(const std::string& column, const std::vector<std::string>& exclude_columns) {
  auto upper_column = dingodb::Helper::ToUpper(column);
  for (const auto& exclude_column : exclude_columns) {
    if (upper_column == exclude_column) {
      return true;
    }
  }

  return false;
}

void ShowTxnTableData(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                      const dingodb::pb::meta::TableDefinition& table_definition,
                      const std::vector<std::string>& exclude_columns) {
  if (txn.datas().empty()) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows;

  // header
  std::vector<ftxui::Element> header = {ftxui::paragraph("Ts")};
  for (const auto& column : table_definition.columns()) {
    if (!IsExcludeColumns(column.name(), exclude_columns)) {
      header.push_back(ftxui::paragraph(fmt::format("{}", column.name())));
    }
  }
  rows.push_back(header);

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
  for (const auto& data : txn.datas()) {
    std::vector<ftxui::Element> row;

    // ts
    row.push_back(ftxui::paragraph(std::to_string(data.ts())));

    // user columns
    auto record_decoder = std::make_shared<dingodb::RecordDecoder>(2, serial_schema, data.partition_id());

    std::vector<std::any> record;
    int ret = record_decoder->Decode(data.key(), data.value(), record);
    CHECK(ret == 0) << "Decode recode failed.";

    for (int i = 0; i < record.size(); ++i) {
      const auto& column_definition = table_definition.columns().at(i);
      if (!IsExcludeColumns(column_definition.name(), exclude_columns)) {
        std::string column_value = dingodb::Helper::ConvertColumnValueToStringV2(column_definition, record[i]);
        row.push_back(ftxui::paragraph(TruncateString(column_value)));
      }
    }

    rows.push_back(row);
  }

  std::cout << "Column Family[data]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnVectorIndexData(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                            const dingodb::pb::meta::TableDefinition& table_definition) {
  if (txn.datas().empty()) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{ftxui::paragraph("ID"), ftxui::paragraph("Ts"),
                                                    ftxui::paragraph("Vector"), ftxui::paragraph("Scalar"),
                                                    ftxui::paragraph("Table")}};

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
  for (const auto& data : txn.datas()) {
    std::vector<ftxui::Element> row;

    dingodb::pb::common::VectorWithId vector_with_id;
    vector_with_id.ParseFromString(data.value());

    // id
    row.push_back(ftxui::paragraph(std::to_string(vector_with_id.id())));

    // ts
    row.push_back(ftxui::paragraph(std::to_string(data.ts())));

    // vector data
    row.push_back(ftxui::paragraph(client_v2::Helper::FormatVectorData(vector_with_id.vector())));

    // scalar data
    auto lines = client_v2::Helper::FormatVectorScalar(vector_with_id.scalar_data());
    std::vector<ftxui::Element> scalar_elems;
    scalar_elems.reserve(lines.size());
    for (auto& line : lines) {
      scalar_elems.push_back(ftxui::paragraph(line));
    }
    row.push_back(ftxui::vflow(scalar_elems));

    // table data
    auto table_elem =
        ftxui::vflow({ftxui::paragraph("key: " + TruncateHexString(vector_with_id.table_data().table_key())),
                      ftxui::paragraph("value: " + TruncateHexString(vector_with_id.table_data().table_value()))});
    row.push_back(table_elem);

    rows.push_back(row);
  }

  std::cout << "Column Family[data]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnDocumentIndexData(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                              const dingodb::pb::meta::TableDefinition& table_definition) {
  if (txn.datas().empty()) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows;
  rows = {{ftxui::paragraph("ID"), ftxui::paragraph("Ts"), ftxui::paragraph("Data")}};

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
  for (const auto& data : txn.datas()) {
    std::vector<ftxui::Element> row;

    dingodb::pb::common::DocumentWithId document_with_id;
    document_with_id.ParseFromString(data.value());

    // id
    row.push_back(ftxui::paragraph(std::to_string(document_with_id.id())));

    // ts
    row.push_back(ftxui::paragraph(std::to_string(data.ts())));

    // scalar data
    auto lines = client_v2::Helper::FormatDocument(document_with_id.document());
    std::vector<ftxui::Element> scalar_elems;
    scalar_elems.reserve(lines.size());
    for (auto& line : lines) {
      scalar_elems.push_back(ftxui::paragraph(line));
    }
    row.push_back(ftxui::vflow(scalar_elems));

    rows.push_back(row);
  }

  std::cout << "Column Family[data]:" << std::endl;
  PrintTable(rows);
}

static std::vector<std::pair<std::string, std::string>> ParseRecord(
    const dingodb::pb::meta::TableDefinition& table_definition, const std::vector<std::any>& values) {
  std::vector<std::pair<std::string, std::string>> result;
  for (int i = 0; i < values.size(); ++i) {
    if (strcmp(values[i].type().name(), "v") == 0) {
      continue;
    }
    const auto& column_definition = table_definition.columns().at(i);

    result.push_back(
        std::make_pair(column_definition.name(),
                       TruncateString(dingodb::Helper::ConvertColumnValueToStringV2(column_definition, values[i]))));
  }

  return result;
}

void ShowTxnTableLock(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                      const dingodb::pb::meta::TableDefinition& table_definition) {
  if (txn.locks().empty()) {
    return;
  }

  auto index_type = table_definition.index_parameter().index_type();

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("PrimaryLock"),
      ftxui::paragraph("Key"),
      ftxui::paragraph("LockTs"),
      ftxui::paragraph("ForUpdateTs"),
      ftxui::paragraph("LockTtl"),
      ftxui::paragraph("TxnSize"),
      ftxui::paragraph("LockType"),
      ftxui::paragraph("ShortValue"),
      ftxui::paragraph("ExtraData"),
      ftxui::paragraph("MinCommitTs"),
  }};

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);

  for (const auto& lock : txn.locks()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", TruncateHexString(lock.lock_info().primary_lock()))),
        ftxui::paragraph(fmt::format("{}", TruncateHexString(lock.lock_info().key()))),
        ftxui::paragraph(fmt::format("{}", lock.lock_info().lock_ts())),
        ftxui::paragraph(fmt::format("{}", lock.lock_info().for_update_ts())),
        ftxui::paragraph(fmt::format("{}", lock.lock_info().lock_ttl())),
        ftxui::paragraph(fmt::format("{}", lock.lock_info().txn_size())),
        ftxui::paragraph(fmt::format("{}", dingodb::pb::store::Op_Name(lock.lock_info().lock_type())))};

    // short value
    std::vector<ftxui::Element> showt_value_elems;
    if (!lock.lock_info().short_value().empty()) {
      if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
        // table data
        auto record_decoder = std::make_shared<dingodb::RecordDecoder>(2, serial_schema, lock.partition_id());

        std::vector<std::any> record;
        int ret = record_decoder->Decode(lock.key(), lock.lock_info().short_value(), record);
        CHECK(ret == 0) << "Decode recode failed.";

        auto keys = ParseRecord(table_definition, record);
        for (const auto& key : keys) {
          showt_value_elems.push_back(ftxui::paragraph(fmt::format("{}: {}", key.first, key.second)));
        }

      } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
        // vector index data
        dingodb::pb::common::VectorWithId vector_with_id;
        vector_with_id.ParseFromString(lock.lock_info().short_value());

        // vector data
        showt_value_elems.push_back(
            ftxui::paragraph(fmt::format("vector: {}", client_v2::Helper::FormatVectorData(vector_with_id.vector()))));

        // scalar data
        showt_value_elems.push_back(ftxui::separator());
        auto lines = client_v2::Helper::FormatVectorScalar(vector_with_id.scalar_data());
        std::vector<ftxui::Element> scalar_elems;
        for (auto& line : lines) {
          showt_value_elems.push_back(ftxui::paragraph(line));
        }

        // table data
        showt_value_elems.push_back(ftxui::separator());
        showt_value_elems.push_back(
            ftxui::paragraph("key: " + TruncateHexString(vector_with_id.table_data().table_key())));
        showt_value_elems.push_back(
            ftxui::paragraph("value: " + TruncateHexString(vector_with_id.table_data().table_value())));

      } else if (index_type == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
        // document index data
        dingodb::pb::common::DocumentWithId document_with_id;
        document_with_id.ParseFromString(lock.lock_info().short_value());

        auto lines = client_v2::Helper::FormatDocument(document_with_id.document());
        for (auto& line : lines) {
          showt_value_elems.push_back(ftxui::paragraph(line));
        }
      }
    }
    row.push_back(ftxui::vflow(showt_value_elems));

    row.push_back(ftxui::paragraph(TruncateHexString(lock.lock_info().extra_data())));
    row.push_back(ftxui::paragraph(fmt::format("{}", lock.lock_info().min_commit_ts())));

    rows.push_back(row);
  }

  std::cout << "Column Family[lock]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnTableWrite(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                       const dingodb::pb::meta::TableDefinition& table_definition) {
  if (txn.writes().empty()) {
    return;
  }

  auto index_type = table_definition.index_parameter().index_type();

  // header
  std::vector<ftxui::Element> header;

  const auto& first_write = txn.writes().at(0);
  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
  auto record_decoder = std::make_shared<dingodb::RecordDecoder>(2, serial_schema, first_write.partition_id());
  std::vector<std::any> record;
  int ret = record_decoder->DecodeKey(first_write.key(), record);
  CHECK(ret == 0) << "Decode recode failed, key: " << dingodb::Helper::StringToHex(first_write.key());
  auto header_keys = ParseRecord(table_definition, record);
  header.reserve(header_keys.size() + 8);
  for (const auto& key : header_keys) {
    header.push_back(ftxui::paragraph(key.first));
  }

  header.push_back(ftxui::paragraph("CommitTs"));
  header.push_back(ftxui::paragraph("StartTs"));
  header.push_back(ftxui::paragraph("Op"));
  header.push_back(ftxui::paragraph("ShortValue"));

  std::vector<std::vector<ftxui::Element>> rows = {header};
  for (const auto& write : txn.writes()) {
    std::vector<ftxui::Element> row;

    // key
    auto record_decoder = std::make_shared<dingodb::RecordDecoder>(2, serial_schema, write.partition_id());
    std::vector<std::any> record;
    int ret = record_decoder->DecodeKey(write.key(), record);
    CHECK(ret == 0) << "Decode recode failed, key: " << dingodb::Helper::StringToHex(write.key());
    auto keys = ParseRecord(table_definition, record);
    if (!keys.empty()) {
      row.reserve(keys.size() + 8);
      for (const auto& key : keys) {
        row.push_back(ftxui::paragraph(key.second));
      }
    } else {
      for (int i = 0; i < header_keys.size(); ++i) {
        row.push_back(ftxui::paragraph(""));
      }
    }

    row.push_back(ftxui::paragraph(fmt::format("{}", write.ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", write.write_info().start_ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", dingodb::pb::store::Op_Name(write.write_info().op()))));

    // short value
    std::vector<ftxui::Element> showt_value_elems;
    if (!write.write_info().short_value().empty()) {
      auto record_decoder = std::make_shared<dingodb::RecordDecoder>(2, serial_schema, write.partition_id());

      std::vector<std::any> record;
      int ret = record_decoder->Decode(write.key(), write.write_info().short_value(), record);
      CHECK(ret == 0) << "Decode recode failed.";

      auto keys = ParseRecord(table_definition, record);
      for (const auto& key : keys) {
        showt_value_elems.push_back(ftxui::paragraph(fmt::format("{}: {}", key.first, key.second)));
      }
    }

    row.push_back(ftxui::vflow(showt_value_elems));

    rows.push_back(row);
  }

  std::cout << "Column Family[write]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnVectorIndexWrite(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                             const dingodb::pb::meta::TableDefinition&) {
  if (txn.writes().empty()) {
    return;
  }

  // header
  std::vector<ftxui::Element> header = {
      ftxui::paragraph("VectorId"), ftxui::paragraph("CommitTs"),   ftxui::paragraph("StartTs"),
      ftxui::paragraph("Op"),       ftxui::paragraph("ShortValue"),
  };

  std::vector<std::vector<ftxui::Element>> rows = {header};
  for (const auto& write : txn.writes()) {
    std::vector<ftxui::Element> row;

    int64_t vector_id = dingodb::VectorCodec::UnPackageVectorId(write.key());
    row.push_back(ftxui::paragraph(fmt::format("{}", vector_id)));
    row.push_back(ftxui::paragraph(fmt::format("{}", write.ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", write.write_info().start_ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", dingodb::pb::store::Op_Name(write.write_info().op()))));

    // short value
    std::vector<ftxui::Element> showt_value_elems;
    if (!write.write_info().short_value().empty()) {
      dingodb::pb::common::VectorWithId vector_with_id;
      vector_with_id.ParseFromString(write.write_info().short_value());

      // vector data
      showt_value_elems.push_back(
          ftxui::paragraph(fmt::format("vector: {}", client_v2::Helper::FormatVectorData(vector_with_id.vector()))));

      // scalar data
      showt_value_elems.push_back(ftxui::separator());
      auto lines = client_v2::Helper::FormatVectorScalar(vector_with_id.scalar_data());
      std::vector<ftxui::Element> scalar_elems;
      for (auto& line : lines) {
        showt_value_elems.push_back(ftxui::paragraph(line));
      }

      // table data
      showt_value_elems.push_back(ftxui::separator());
      showt_value_elems.push_back(
          ftxui::paragraph("key: " + TruncateHexString(vector_with_id.table_data().table_key())));
      showt_value_elems.push_back(
          ftxui::paragraph("value: " + TruncateHexString(vector_with_id.table_data().table_value())));
    }

    row.push_back(ftxui::vflow(showt_value_elems));

    rows.push_back(row);
  }

  std::cout << "Column Family[write]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnDocumentIndexWrite(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                               const dingodb::pb::meta::TableDefinition&) {
  if (txn.writes().empty()) {
    return;
  }

  // header
  std::vector<ftxui::Element> header = {
      ftxui::paragraph("DocumentId"), ftxui::paragraph("CommitTs"),   ftxui::paragraph("StartTs"),
      ftxui::paragraph("Op"),         ftxui::paragraph("ShortValue"),
  };

  std::vector<std::vector<ftxui::Element>> rows = {header};
  for (const auto& write : txn.writes()) {
    std::vector<ftxui::Element> row;

    int64_t document_id = dingodb::DocumentCodec::UnPackageDocumentId(write.key());
    row.push_back(ftxui::paragraph(fmt::format("{}", document_id)));
    row.push_back(ftxui::paragraph(fmt::format("{}", write.ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", write.write_info().start_ts())));
    row.push_back(ftxui::paragraph(fmt::format("{}", dingodb::pb::store::Op_Name(write.write_info().op()))));

    // short value
    std::vector<ftxui::Element> showt_value_elems;
    if (!write.write_info().short_value().empty()) {
      dingodb::pb::common::DocumentWithId document_with_id;
      document_with_id.ParseFromString(write.write_info().short_value());

      auto lines = client_v2::Helper::FormatDocument(document_with_id.document());
      for (auto& line : lines) {
        showt_value_elems.push_back(ftxui::paragraph(line));
      }
    }

    row.push_back(ftxui::vflow(showt_value_elems));

    rows.push_back(row);
  }

  std::cout << "Column Family[write]:" << std::endl;
  PrintTable(rows);
}

void ShowTxnTable(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                  const dingodb::pb::meta::TableDefinition& table_definition,
                  const std::vector<std::string>& exclude_columns) {
  if (table_definition.name().empty()) {
    std::cout << "Error: Missing table definition." << std::endl;
    return;
  }

  auto index_type = table_definition.index_parameter().index_type();
  if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
    ShowTxnTableData(txn, table_definition, exclude_columns);
    ShowTxnTableLock(txn, table_definition);
    ShowTxnTableWrite(txn, table_definition);

  } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
    ShowTxnVectorIndexData(txn, table_definition);
    ShowTxnTableLock(txn, table_definition);
    ShowTxnVectorIndexWrite(txn, table_definition);

  } else if (index_type == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
    ShowTxnDocumentIndexData(txn, table_definition);
    ShowTxnTableLock(txn, table_definition);
    ShowTxnDocumentIndexWrite(txn, table_definition);
  }
}

void ShowTxnVectorIndex(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                        const dingodb::pb::meta::TableDefinition& table_definition) {}

void ShowTxnDocumentIndexx(const dingodb::pb::debug::DumpRegionResponse::Txn& txn,
                           const dingodb::pb::meta::TableDefinition& table_definition) {}

void Pretty::Show(const dingodb::pb::debug::DumpRegionResponse::Data& data,
                  const dingodb::pb::meta::TableDefinition& table_definition,
                  const std::vector<std::string>& exclude_columns) {
  if (!data.kvs().empty()) {
    std::vector<std::vector<std::string>> rows;
    rows = {{"Key", "Ts", "Flag", "Ttl", "Value"}};

    for (const auto& kv : data.kvs()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(kv.flag());

      rows.push_back({dingodb::Helper::StringToHex(kv.key()), std::to_string(kv.ts()), flag, std::to_string(kv.ttl()),
                      TruncateHexString(kv.value())

      });
    }

    PrintTable(rows);
  } else if (!data.vectors().empty()) {
    std::vector<std::vector<ftxui::Element>> rows = {
        {ftxui::paragraph("ID"), ftxui::paragraph("Ts"), ftxui::paragraph("Flag"), ftxui::paragraph("Ttl"),
         ftxui::paragraph("Vector"), ftxui::paragraph("Scalar"), ftxui::paragraph("Table")}};

    int count = 0;
    for (const auto& vector : data.vectors()) {
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
          ftxui::vflow({ftxui::paragraph("key: " + TruncateHexString(vector.table_data().table_key())),
                        ftxui::paragraph("value: " + TruncateHexString(vector.table_data().table_value()))});

      rows.push_back({ftxui::paragraph(std::to_string(vector.vector_id())),
                      ftxui::paragraph(std::to_string(vector.ts())), ftxui::paragraph(flag),
                      ftxui::paragraph(std::to_string(vector.ttl())),
                      ftxui::paragraph(client_v2::Helper::FormatVectorData(vector.vector())),
                      ftxui::vflow(scalar_elems), table_elem});
    }

    PrintTable(rows);
  } else if (!data.documents().empty()) {
    std::vector<std::vector<ftxui::Element>> rows;
    rows = {{ftxui::paragraph("ID"), ftxui::paragraph("Ts"), ftxui::paragraph("Flag"), ftxui::paragraph("Ttl"),
             ftxui::paragraph("Data")}};

    for (const auto& document : data.documents()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(document.flag());

      // scalar data
      auto lines = client_v2::Helper::FormatDocument(document.document());
      std::vector<ftxui::Element> scalar_elems;
      scalar_elems.reserve(lines.size());
      for (auto& line : lines) {
        scalar_elems.push_back(ftxui::paragraph(line));
      }

      rows.push_back({ftxui::paragraph(std::to_string(document.document_id())),
                      ftxui::paragraph(std::to_string(document.ts())), ftxui::paragraph(flag),
                      ftxui::paragraph(std::to_string(document.ttl())), ftxui::vflow(scalar_elems)});
    }

    PrintTable(rows);
  } else if (data.has_txn()) {
    ShowTxnTable(data.txn(), table_definition, exclude_columns);
  }

  // print summary
  int size = std::max(data.kvs_size(), data.vectors_size());
  size = std::max(size, data.documents_size());
  size = std::max(size, data.txn().datas_size());
  size = std::max(size, data.txn().locks_size());
  size = std::max(size, data.txn().writes_size());

  std::cout << fmt::format("Summary: total count({})", size) << std::endl;
}

void Pretty::Show(dingodb::pb::debug::DumpRegionResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  Show(response.data());
}

void Pretty::Show(std::vector<TenantInfo> tenants) {
  std::vector<std::vector<std::string>> rows;
  rows = {{"ID", "Name", "CreateTime", "UpdateTime", "Comment"}};

  for (auto& tenant : tenants) {
    rows.push_back({std::to_string(tenant.id), tenant.name, dingodb::Helper::FormatMsTime(tenant.create_time),
                    dingodb::Helper::FormatMsTime(tenant.update_time), tenant.comment});
  }

  PrintTable(rows);
}

void ShowKeyValues(const std::vector<dingodb::pb::common::KeyValue>& kvs) {
  std::vector<std::vector<ftxui::Element>> rows = {{ftxui::paragraph("Key"), ftxui::paragraph("Value")}};

  for (const auto& kv : kvs) {
    rows.push_back({ftxui::paragraph(TruncateHexString(kv.key())), ftxui::paragraph(TruncateHexString(kv.value()))});
  }

  PrintTable(rows);
}

void ShowVectorWithIds(const std::vector<dingodb::pb::common::VectorWithId>& vectors_with_ids) {
  std::vector<std::vector<ftxui::Element>> rows = {
      {ftxui::paragraph("ID"), ftxui::paragraph("Vector"), ftxui::paragraph("Scalar"), ftxui::paragraph("Table")}};

  for (const auto& vector_with_id : vectors_with_ids) {
    // scalar data
    auto lines = client_v2::Helper::FormatVectorScalar(vector_with_id.scalar_data());
    std::vector<ftxui::Element> scalar_elems;
    scalar_elems.reserve(lines.size());
    for (auto& line : lines) {
      scalar_elems.push_back(ftxui::paragraph(line));
    }

    // table data
    auto table_elem =
        ftxui::vflow({ftxui::paragraph("key: " + TruncateHexString(vector_with_id.table_data().table_key())),
                      ftxui::paragraph("value: " + TruncateHexString(vector_with_id.table_data().table_value()))});

    rows.push_back({ftxui::paragraph(std::to_string(vector_with_id.id())),
                    ftxui::paragraph(client_v2::Helper::FormatVectorData(vector_with_id.vector())),
                    ftxui::vflow(scalar_elems), table_elem});
  }

  PrintTable(rows);
}

void ShowDocumentWithIds(const std::vector<dingodb::pb::common::DocumentWithId>& document_with_ids) {
  std::vector<std::vector<ftxui::Element>> rows = {{ftxui::paragraph("ID"), ftxui::paragraph("Data")}};

  for (const auto& document_with_id : document_with_ids) {
    // scalar data
    auto lines = client_v2::Helper::FormatDocument(document_with_id.document());
    std::vector<ftxui::Element> scalar_elems;
    scalar_elems.reserve(lines.size());
    for (auto& line : lines) {
      scalar_elems.push_back(ftxui::paragraph(line));
    }

    rows.push_back({ftxui::paragraph(std::to_string(document_with_id.id())), ftxui::vflow(scalar_elems)});
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::store::TxnScanResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  if (!response.kvs().empty()) {
    ShowKeyValues(dingodb::Helper::PbRepeatedToVector(response.kvs()));

  } else if (!response.vectors().empty()) {
    ShowVectorWithIds(dingodb::Helper::PbRepeatedToVector(response.vectors()));

  } else if (!response.documents().empty()) {
    ShowDocumentWithIds(dingodb::Helper::PbRepeatedToVector(response.documents()));
  }
}

void ShowLockInfo(std::vector<dingodb::pb::store::LockInfo> locks) {
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("PrimaryLock"),
      ftxui::paragraph("Key"),
      ftxui::paragraph("LockTs"),
      ftxui::paragraph("ForUpdateTs"),
      ftxui::paragraph("LockTtl"),
      ftxui::paragraph("TxnSize"),
      ftxui::paragraph("LockType"),
      ftxui::paragraph("ShortValue"),
      ftxui::paragraph("ExtraData"),
      ftxui::paragraph("MinCommitTs"),
  }};

  for (const auto& lock : locks) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", TruncateHexString(lock.primary_lock()))),
        ftxui::paragraph(fmt::format("{}", TruncateHexString(lock.key()))),
        ftxui::paragraph(fmt::format("{}", lock.lock_ts())),
        ftxui::paragraph(fmt::format("{}", lock.for_update_ts())),
        ftxui::paragraph(fmt::format("{}", lock.lock_ttl())),
        ftxui::paragraph(fmt::format("{}", lock.txn_size())),
        ftxui::paragraph(fmt::format("{}", dingodb::pb::store::Op_Name(lock.lock_type())))};

    row.push_back(ftxui::paragraph(TruncateHexString(lock.short_value())));

    row.push_back(ftxui::paragraph(TruncateHexString(lock.extra_data())));
    row.push_back(ftxui::paragraph(fmt::format("{}", lock.min_commit_ts())));

    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::store::TxnScanLockResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  ShowLockInfo(dingodb::Helper::PbRepeatedToVector(response.locks()));
}

void Pretty::Show(dingodb::pb::meta::CreateIndexResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  const auto& dingo_command_id = response.index_id();
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("EntityType"),
      ftxui::paragraph("ParentId"),
      ftxui::paragraph("EntityId"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", dingodb::pb::meta::EntityType_Name(dingo_command_id.entity_type()))),
      ftxui::paragraph(fmt::format("{}", dingo_command_id.parent_entity_id())),
      ftxui::paragraph(fmt::format("{}", dingo_command_id.entity_id())),
  };
  rows.push_back(row);
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentSearchResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  if (response.document_with_scores_size() == 0) {
    std::cout << "Not search document ." << std::endl;
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("DocumentId"),
      ftxui::paragraph("Score"),
  }};
  for (auto const& document_with_score : response.document_with_scores()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", document_with_score.document_with_id().id())),
        ftxui::paragraph(fmt::format("{}", document_with_score.score())),
    };
    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentSearchAllResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  if (response.document_with_scores_size() == 0) {
    std::cout << "Not search document ." << std::endl;
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("DocumentId"),
      ftxui::paragraph("Score"),
  }};
  for (auto const& document_with_score : response.document_with_scores()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", document_with_score.document_with_id().id())),
        ftxui::paragraph(fmt::format("{}", document_with_score.score())),
    };
    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentBatchQueryResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  if (response.doucments_size() == 0) {
    std::cout << "Not find document." << std::endl;
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("DocumentId"),
      ftxui::paragraph("DocumentData"),
  }};
  for (auto const& document : response.doucments()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", document.id())),
    };
    std::string document_data;
    for (auto const& data : document.document().document_data()) {
      document_data +=
          fmt::format("{}-{}", data.first, dingodb::pb::common::ScalarFieldType_Name(data.second.field_type()));
    }
    row.push_back(ftxui::paragraph(document_data));
    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentGetBorderIdResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("ID"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", response.id())),
  };
  rows.push_back(row);

  PrintTable(rows);
}
void Pretty::Show(dingodb::pb::document::DocumentScanQueryResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  if (response.documents_size() == 0) {
    std::cout << "Not find document." << std::endl;
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("DocumentId"),
      ftxui::paragraph("DocumentData"),
  }};
  for (auto const& document : response.documents()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", document.id())),
    };
    std::string document_data;
    for (auto const& data : document.document().document_data()) {
      document_data +=
          fmt::format("{}-{}", data.first, dingodb::pb::common::ScalarFieldType_Name(data.second.field_type()));
    }
    row.push_back(ftxui::paragraph(document_data));
    rows.push_back(row);
  }

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentCountResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Count"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", response.count())),
  };
  rows.push_back(row);

  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::document::DocumentGetRegionMetricsResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  const auto& metrics = response.metrics();
  {
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("TotalNumDocs"),
        ftxui::paragraph("TotalNumTokens"),
        ftxui::paragraph("MaxId"),
        ftxui::paragraph("MinId"),
    }};
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", metrics.total_num_docs())),
        ftxui::paragraph(fmt::format("{}", metrics.total_num_tokens())),
        ftxui::paragraph(fmt::format("{}", metrics.max_id())),
        ftxui::paragraph(fmt::format("{}", metrics.min_id())),
    };
    rows.push_back(row);

    PrintTable(rows);
  }
  std::cout << "Meta_Json: " << metrics.meta_json() << std::endl;
  std::cout << "Json_Parameter: " << metrics.json_parameter() << std::endl;
}

void Pretty::Show(const std::vector<dingodb::pb::common::Region>& regions) {
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Id"),
      ftxui::paragraph("Name"),
      ftxui::paragraph("TableId"),
      ftxui::paragraph("IndexId"),
      ftxui::paragraph("StartKey"),
      ftxui::paragraph("EndKey"),
  }};
  for (auto const& region : regions) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", region.id())),
        ftxui::paragraph(fmt::format("{}", region.definition().name())),
        ftxui::paragraph(fmt::format("{}", region.definition().table_id())),
        ftxui::paragraph(fmt::format("{}", region.definition().index_id())),
    };
    std::string start_key = dingodb::Helper::StringToHex(region.definition().range().start_key());
    std::string end_key = dingodb::Helper::StringToHex(region.definition().range().end_key());
    auto plaintext_range = client_v2::Helper::DecodeRangeToPlaintext(region);
    start_key += fmt::format("({})", plaintext_range.start_key());
    end_key += fmt::format("({})", plaintext_range.end_key());
    row.push_back(ftxui::paragraph(start_key));
    row.push_back(ftxui::paragraph(end_key));
    rows.push_back(row);
  }
  PrintTable(rows);
}

void ShowIndexParameter(dingodb::pb::common::IndexParameter& index_parameter) {
  // vector
  if (index_parameter.index_type() == dingodb::pb::common::INDEX_TYPE_VECTOR) {
    const auto& vector_index_parameter = index_parameter.vector_index_parameter();
    // header
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("VectorIndexType"),
        ftxui::paragraph("Dimension"),
        ftxui::paragraph("MetrictType"),
    }};
    std::vector<ftxui::Element> row = {ftxui::paragraph(
        fmt::format("{}", dingodb::pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type())))};

    if (vector_index_parameter.has_flat_parameter()) {
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.flat_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.flat_parameter().metric_type()))));
    } else if (vector_index_parameter.has_ivf_flat_parameter()) {
      rows[0].push_back(ftxui::paragraph("NCentroids"));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_flat_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_flat_parameter().metric_type()))));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_flat_parameter().ncentroids())));
    } else if (vector_index_parameter.has_ivf_pq_parameter()) {
      rows[0].push_back(ftxui::paragraph("NCentroids"));
      rows[0].push_back(ftxui::paragraph("NSubVector"));
      rows[0].push_back(ftxui::paragraph("BucketInitSize"));
      rows[0].push_back(ftxui::paragraph("BucketMaxSize"));
      rows[0].push_back(ftxui::paragraph("NbitsPerIdx"));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_pq_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_pq_parameter().metric_type()))));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_pq_parameter().ncentroids())));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_pq_parameter().bucket_init_size())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_pq_parameter().bucket_max_size()))));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.ivf_pq_parameter().nbits_per_idx())));
    } else if (vector_index_parameter.has_hnsw_parameter()) {
      rows[0].push_back(ftxui::paragraph("EfConstruction"));
      rows[0].push_back(ftxui::paragraph("MaxElements"));
      rows[0].push_back(ftxui::paragraph("NLinks"));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.hnsw_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.hnsw_parameter().metric_type()))));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.hnsw_parameter().efconstruction())));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.hnsw_parameter().max_elements())));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.hnsw_parameter().nlinks())));
    } else if (vector_index_parameter.has_diskann_parameter()) {
      rows[0].push_back(ftxui::paragraph("ValueType"));
      rows[0].push_back(ftxui::paragraph("MaxDegree"));
      rows[0].push_back(ftxui::paragraph("SearchListSize"));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.diskann_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.diskann_parameter().metric_type()))));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::ValueType_Name(vector_index_parameter.diskann_parameter().value_type()))));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.diskann_parameter().max_degree())));
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.diskann_parameter().search_list_size())));
    } else if (vector_index_parameter.has_bruteforce_parameter()) {
      row.push_back(ftxui::paragraph(fmt::format("{}", vector_index_parameter.bruteforce_parameter().dimension())));
      row.push_back(ftxui::paragraph(fmt::format(
          "{}", dingodb::pb::common::MetricType_Name(vector_index_parameter.bruteforce_parameter().metric_type()))));
    }
    rows.push_back(row);
    PrintTable(rows);
  } else if (index_parameter.index_type() == dingodb::pb::common::INDEX_TYPE_SCALAR) {
    const auto& scalar_index_parameter = index_parameter.scalar_index_parameter();
    // header
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("ScalarIndexType"),
        ftxui::paragraph("IsUnique"),
    }};
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(
            fmt::format("{}", dingodb::pb::common::ScalarIndexType_Name(scalar_index_parameter.scalar_index_type()))),
        ftxui::paragraph(fmt::format("{}", scalar_index_parameter.is_unique())),
    };
    rows.push_back(row);
    PrintTable(rows);
  } else if (index_parameter.index_type() == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
    const auto& document_index_parameter = index_parameter.document_index_parameter();
    // header
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("JsonParameter"),
    }};
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", document_index_parameter.json_parameter())),
    };
    rows.push_back(row);
    PrintTable(rows);
  }
}

void Pretty::Show(const dingodb::pb::meta::TableDefinitionWithId& table_definition_with_id) {
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("TableId"),
      ftxui::paragraph("TenantId"),
      ftxui::paragraph("TableName"),
      ftxui::paragraph("AutoIncrement"),
      ftxui::paragraph("Engine"),
      ftxui::paragraph("Partitions"),
      ftxui::paragraph("Replica"),
      ftxui::paragraph("CreateTime"),
      ftxui::paragraph("UpdateTime"),
      ftxui::paragraph("IndexType"),
  }};
  const auto& table_definition = table_definition_with_id.table_definition();
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", table_definition_with_id.table_id().entity_id())),
      ftxui::paragraph(fmt::format("{}", table_definition_with_id.tenant_id())),
      ftxui::paragraph(fmt::format("{}", table_definition.name())),
      ftxui::paragraph(fmt::format("{}", table_definition.auto_increment())),
      ftxui::paragraph(fmt::format("{}", dingodb::pb::common::Engine_Name(table_definition.engine()))),
      ftxui::paragraph(fmt::format("{}", table_definition.table_partition().partitions_size())),
      ftxui::paragraph(fmt::format("{}", table_definition.replica())),
      ftxui::paragraph(
          fmt::format("{}", dingodb::Helper::FormatMsTime(table_definition.create_timestamp(), "%Y-%m-%d %H:%M:%S"))),
      ftxui::paragraph(
          fmt::format("{}", dingodb::Helper::FormatMsTime(table_definition.update_timestamp(), "%Y-%m-%d %H:%M:%S"))),
      ftxui::paragraph(
          fmt::format("{}", dingodb::pb::common::IndexType_Name(table_definition.index_parameter().index_type()))),

  };
  rows.push_back(row);
  PrintTable(rows);
  auto index_parameter = table_definition.index_parameter();
  ShowIndexParameter(index_parameter);
}

void Pretty::Show(dingodb::pb::meta::TsoResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("TimeStamp"),
  }};
  auto lambda_tso_2_timestamp_function = [](const ::dingodb::pb::meta::TsoTimestamp& tso) {
    return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
  };

  for (int i = 0; i < 10; i++) {
    dingodb::pb::meta::TsoTimestamp tso;
    tso.set_physical(response.start_timestamp().physical());
    tso.set_logical(response.start_timestamp().logical() + i);
    int64_t time_safe_ts = lambda_tso_2_timestamp_function(tso);
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", time_safe_ts)),
    };
    rows.push_back(row);
  }
  PrintTable(rows);
}
void Pretty::ShowSchemas(const std::vector<dingodb::pb::meta::Schema>& schemas) {
  if (schemas.empty()) {
    std::cout << "Not find schema." << std::endl;
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Id"),
      ftxui::paragraph("Name"),
      ftxui::paragraph("TenantId"),
      ftxui::paragraph("TableCount"),
      ftxui::paragraph("IndexCount"),
      ftxui::paragraph("TableIds"),
      ftxui::paragraph("IndexIds"),
  }};

  for (auto const& schema : schemas) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", schema.id().entity_id())),
        ftxui::paragraph(fmt::format("{}", schema.name())),
        ftxui::paragraph(fmt::format("{}", schema.tenant_id())),
        ftxui::paragraph(fmt::format("{}", schema.table_ids_size())),
        ftxui::paragraph(fmt::format("{}", schema.index_ids_size())),
    };
    std::string table_ids_str;
    std::string index_ids_str;
    for (size_t i = 0; i < schema.table_ids_size(); i++) {
      table_ids_str += fmt::format("{}", schema.table_ids()[i].entity_id());
      if (i != schema.table_ids_size() - 1) {
        table_ids_str += ",";
      }
    }
    for (size_t i = 0; i < schema.index_ids_size(); i++) {
      index_ids_str += fmt::format("{}", schema.index_ids()[i].entity_id());
      if (i != schema.index_ids_size() - 1) {
        index_ids_str += ",";
      }
    }
    row.push_back(ftxui::paragraph(table_ids_str));
    row.push_back(ftxui::paragraph(index_ids_str));
    rows.push_back(row);
  }
  PrintTable(rows);
}
void Pretty::Show(dingodb::pb::meta::GetSchemasResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  ShowSchemas(dingodb::Helper::PbRepeatedToVector(response.schemas()));
}

void Pretty::Show(dingodb::pb::meta::GetSchemaResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<dingodb::pb::meta::Schema> schemas;
  schemas.push_back(response.schema());
  ShowSchemas(schemas);
}

void Pretty::Show(dingodb::pb::meta::GetSchemaByNameResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<dingodb::pb::meta::Schema> schemas;
  schemas.push_back(response.schema());
  ShowSchemas(schemas);
}

void Pretty::Show(dingodb::pb::meta::GetTablesBySchemaResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  if (response.table_definition_with_ids_size() == 0) {
    std::cout << "Schema has no table." << std::endl;
    return;
  }
  for (auto const& table_definition_with_id : response.table_definition_with_ids()) {
    Show(table_definition_with_id);
  }
}

void Pretty::Show(dingodb::pb::coordinator::GetGCSafePointResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  {
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("SafePoint"),
        ftxui::paragraph("GcStop"),
    }};
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", response.safe_point())),
        ftxui::paragraph(fmt::format("{}", response.gc_stop())),
    };
    rows.push_back(row);
    PrintTable(rows);
  }

  // for other tenants that are not default tenant
  {
    std::vector<std::vector<ftxui::Element>> rows = {{
        ftxui::paragraph("TenantId"),
        ftxui::paragraph("SafePoint"),
    }};
    for (auto const& item : response.tenant_safe_points()) {
      std::vector<ftxui::Element> row = {
          ftxui::paragraph(fmt::format("{}", item.first)),
          ftxui::paragraph(fmt::format("{}", item.second)),
      };
      rows.push_back(row);
    }
    PrintTable(rows);
  }
}

void Pretty::Show(const dingodb::pb::debug::DebugResponse::GCMetrics& gc_metrics, bool include_region,
                  bool region_only) {
  auto format_time = [](int64_t ts_ms) -> std::string {
    if (ts_ms <= 0) {
      return "-";
    }
    return dingodb::Helper::FormatMsTime(ts_ms);
  };

  auto build_region_rows = [&]() {
    std::vector<std::vector<std::string>> region_rows = {
        {"RegionId", "LastStart", "LastEnd", "SafePointTs", "IterCount", "DeleteCount", "InfoType"}};
    for (const auto& region_history : gc_metrics.region_histories()) {
      if (region_history.has_last_history()) {
        const auto& last = region_history.last_history();
        region_rows.push_back({fmt::format("{}", region_history.region_id()), format_time(last.start_time_ms()),
                               format_time(last.end_time_ms()), fmt::format("{}", last.safe_point_ts()),
                               fmt::format("{}", last.iter_count()), fmt::format("{}", last.delete_count()), "LastGc"});
      }

      if (region_history.has_last_delete_history()) {
        const auto& last_delete = region_history.last_delete_history();
        region_rows.push_back({fmt::format("{}", region_history.region_id()), format_time(last_delete.start_time_ms()),
                               format_time(last_delete.end_time_ms()), fmt::format("{}", last_delete.safe_point_ts()),
                               fmt::format("{}", last_delete.iter_count()),
                               fmt::format("{}", last_delete.delete_count()), "LastWithDelete"});
      }
    }
    return region_rows;
  };

  if (region_only) {
    std::cout << "Region GC Metrics:" << std::endl;
    auto region_rows = build_region_rows();
    if (region_rows.size() == 1) {
      std::cout << "No region gc metrics found." << std::endl;
      return;
    }
    PrintTableAdaptive(region_rows);
    std::cout << std::endl;
    return;
  }

  std::vector<std::vector<std::string>> metric_rows = {
      {"FirstGcStart", "LastGcEnd", "GCRunCount", "TotalIterCount", "TotalDeleteCount"},
      {format_time(gc_metrics.first_start_time_ms()), format_time(gc_metrics.last_end_time_ms()),
       fmt::format("{}", gc_metrics.run_count()), fmt::format("{}", gc_metrics.total_iter_count()),
       fmt::format("{}", gc_metrics.total_delete_count())},
  };
  PrintTableAdaptive(metric_rows);

  const dingodb::pb::debug::DebugResponse::TxnGcHistoryItem* last_gc = nullptr;
  const dingodb::pb::debug::DebugResponse::TxnGcHistoryItem* last_gc_with_delete = nullptr;

  if (gc_metrics.history_size() > 0) {
    last_gc = &gc_metrics.history(0);
  }

  if (gc_metrics.has_last_with_delete_history()) {
    last_gc_with_delete = &gc_metrics.last_with_delete_history();
  }

  if (last_gc_with_delete == nullptr) {
    for (const auto& item : gc_metrics.history()) {
      if (item.delete_count() > 0) {
        last_gc_with_delete = &item;
        break;
      }
    }
  }

  auto format_item = [&](const dingodb::pb::debug::DebugResponse::TxnGcHistoryItem* item) -> std::string {
    if (item == nullptr) {
      return "StartTime=- EndTime=- IterCount=0 DeleteCount=0";
    }
    return fmt::format("StartTime={} EndTime={} IterCount={} DeleteCount={}", format_time(item->start_time_ms()),
                       format_time(item->end_time_ms()), item->iter_count(), item->delete_count());
  };

  std::cout << "Last GC With Deletions: " << format_item(last_gc_with_delete) << std::endl;
  std::cout << "Last GC: " << format_item(last_gc) << std::endl;

  std::cout << std::endl << "GC History:" << std::endl;
  std::vector<std::vector<std::string>> history_rows = {{"Index", "StartTime", "EndTime", "IterCount", "DeleteCount"}};

  for (int i = 0; i < gc_metrics.history_size(); ++i) {
    const auto& item = gc_metrics.history(i);
    history_rows.push_back({fmt::format("{}", gc_metrics.history_size() - i), format_time(item.start_time_ms()),
                            format_time(item.end_time_ms()), fmt::format("{}", item.iter_count()),
                            fmt::format("{}", item.delete_count())});
  }
  PrintTableAdaptive(history_rows);

  if (include_region) {
    std::cout << std::endl << "Last GC Per Region:" << std::endl;
    PrintTableAdaptive(build_region_rows());
  }
}

void Pretty::Show(dingodb::pb::coordinator::GetJobListResponse& response, bool is_interactive) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Id"),
      ftxui::paragraph("Name"),
      ftxui::paragraph("NextStep"),
      ftxui::paragraph("TaskSize"),
      ftxui::paragraph("CreateTime"),
      ftxui::paragraph("FinishTime"),
  }};
  if (response.job_list_size() == 0) {
    std::cout << "Task list is empty." << std::endl;
    return;
  }
  for (auto const& job : response.job_list()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", job.id())),
        ftxui::paragraph(fmt::format("{}", job.name())),
        ftxui::paragraph(fmt::format("{}", job.next_step())),
        ftxui::paragraph(fmt::format("{}", job.tasks_size())),
        ftxui::paragraph(job.create_time()),
        ftxui::paragraph(job.finish_time()),
    };
    rows.push_back(row);
  }

  if (is_interactive) {
    PrintTableInteractive(rows);
  } else {
    PrintTable(rows);
  }
  std::cout << "Sumary: total_job_size: " << response.job_list_size() << std::endl;
}

void Pretty::Show(dingodb::pb::coordinator::GetExecutorMapResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Id"),
      ftxui::paragraph("Epoch"),
      ftxui::paragraph("State"),
      ftxui::paragraph("ServerLocation"),
      ftxui::paragraph("User"),
      ftxui::paragraph("CreateTime"),
      ftxui::paragraph("LastSeenTime"),
      ftxui::paragraph("ClusterName"),
      ftxui::paragraph("LeaderId"),
  }};
  const auto& executor_map = response.executormap().executors();
  if (response.executormap().executors_size() == 0) {
    std::cout << "Executor map is empty." << std::endl;
    return;
  }
  for (auto const& executor : executor_map) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", executor.id())),
        ftxui::paragraph(fmt::format("{}", executor.epoch())),
        ftxui::paragraph(fmt::format("{}", dingodb::pb::common::ExecutorState_Name(executor.state()))),
        ftxui::paragraph(fmt::format("{}", dingodb::Helper::LocationToString(executor.server_location()))),
        ftxui::paragraph(fmt::format("{}-{}", executor.executor_user().user(), executor.executor_user().keyring())),
        ftxui::paragraph(dingodb::Helper::FormatMsTime(executor.create_timestamp())),
        ftxui::paragraph(dingodb::Helper::FormatMsTime(executor.last_seen_timestamp())),
        ftxui::paragraph(fmt::format("{}", executor.cluster_name())),
        ftxui::paragraph(fmt::format("{}", executor.leader_id())),
    };
    rows.push_back(row);
  }
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::coordinator::QueryRegionResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<std::string>> rows = {
      {"Id", "Epoch", "Type", "State", "LeaderStoreId", "CreateTime", "StartKey", "EndKey", "TableId", "SchemaId",
       "TenantId"}};
  const auto& region = response.region();
  std::vector<std::string> row = {
      fmt::format("{}", region.id()),
      fmt::format("{}", region.epoch()),
      dingodb::pb::common::RegionType_Name(region.region_type()),
      dingodb::pb::common::RegionState_Name(region.state()),
      fmt::format("{}", region.leader_store_id()),
      dingodb::Helper::FormatMsTime(region.create_timestamp()),
      dingodb::Helper::StringToHex(region.definition().range().start_key()),
      dingodb::Helper::StringToHex(region.definition().range().end_key()),
      fmt::format("{}", region.definition().table_id()),
      fmt::format("{}", region.definition().schema_id()),
      fmt::format("{}", region.definition().tenant_id()),
  };
  rows.push_back(row);
  PrintTableAdaptive(rows);
}

void Pretty::Show(dingodb::pb::index::VectorGetBorderIdResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Id"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", response.id())),
  };
  rows.push_back(row);
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::index::VectorCountResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("Count"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", response.count())),
  };
  rows.push_back(row);
  PrintTable(rows);
}

void Pretty::ShowTotalCount(int64_t total_count) {
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("TotalCount"),
  }};
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", total_count)),
  };
  rows.push_back(row);
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::index::VectorCalcDistanceResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("InternalDistence"),
  }};
  for (auto const& distences : response.distances()) {
    std::string distance_str;
    for (auto const internal_distance : distences.internal_distances()) {
      distance_str += fmt::format("{},", internal_distance);
    }
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", distance_str)),
    };
    rows.push_back(row);
  }
  PrintTable(rows);
  std::cout << "Summary: distance size: " << response.distances_size() << std::endl;
}

void Pretty::Show(dingodb::pb::index::VectorGetRegionMetricsResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("VectorType"),
      ftxui::paragraph("CurrentCount"),
      ftxui::paragraph("DeletedCount"),
      ftxui::paragraph("MaxId"),
      ftxui::paragraph("MinId"),
      ftxui::paragraph("MemoryBytes"),
  }};
  const auto& metrics = response.metrics();
  std::vector<ftxui::Element> row = {
      ftxui::paragraph(fmt::format("{}", dingodb::pb::common::VectorIndexType_Name(metrics.vector_index_type()))),
      ftxui::paragraph(fmt::format("{}", metrics.current_count())),
      ftxui::paragraph(fmt::format("{}", metrics.deleted_count())),
      ftxui::paragraph(fmt::format("{}", metrics.max_id())),
      ftxui::paragraph(fmt::format("{}", metrics.min_id())),
      ftxui::paragraph(fmt::format("{}", metrics.memory_bytes())),
  };
  rows.push_back(row);
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::meta::GetTenantsResponse& response) {
  if (ShowError(response.error())) {
    return;
  }
  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("ID"),
      ftxui::paragraph("Name"),
      ftxui::paragraph("CreateTime"),
      ftxui::paragraph("UpdateTime"),
      ftxui::paragraph("Comment"),
  }};
  for (auto const& tenant : response.tenants()) {
    std::cout << "coor tenant create_time:" << tenant.create_timestamp()
              << ", format:" << dingodb::Helper::FormatMsTime(tenant.create_timestamp()) << "\n";
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", tenant.id())),
        ftxui::paragraph(fmt::format("{}", tenant.name())),
        ftxui::paragraph(fmt::format("{}", dingodb::Helper::FormatMsTime(tenant.create_timestamp()))),
        ftxui::paragraph(fmt::format("{}", dingodb::Helper::FormatMsTime(tenant.update_timestamp()))),
        ftxui::paragraph(fmt::format("{}", tenant.comment())),
    };
    rows.push_back(row);
  }
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::coordinator::CreateIdsResponse& response) {
  if (ShowError(response.error())) {
    return;
  }

  std::vector<std::vector<ftxui::Element>> rows = {{
      ftxui::paragraph("ID"),
  }};
  for (auto const& id : response.ids()) {
    std::vector<ftxui::Element> row = {
        ftxui::paragraph(fmt::format("{}", id)),
    };
    rows.push_back(row);
  }
  PrintTable(rows);
}

void Pretty::Show(dingodb::pb::store::TxnScanResponse& response, bool /*calc_count*/) {
  if (ShowError(response.error())) {
    return;
  }

  std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> result_serial_schemas =
      std::make_shared<std::vector<std::shared_ptr<dingodb::BaseSchema>>>();

  std::shared_ptr<dingodb::DingoSchema<std::optional<int64_t>>> serial_schema =
      std::make_shared<dingodb::DingoSchema<std::optional<int64_t>>>();

  serial_schema->SetIsKey(false);
  serial_schema->SetAllowNull(false);
  serial_schema->SetIndex(0);
  serial_schema->SetName("");

  result_serial_schemas->push_back(serial_schema);

  std::shared_ptr<dingodb::RecordDecoder> result_record_decoder =
      std::make_shared<dingodb::RecordDecoder>(1, result_serial_schemas, 60059);

  std::vector<std::any> record;

  if (response.kvs().size() > 1) {
    DINGO_LOG(ERROR) << "response.kvs size  > 0. error. size : " << response.kvs().size();
    return;
  }

  const dingodb::pb::common::KeyValue& kv = response.kvs(0);

  int ret = result_record_decoder->Decode(kv.key(), kv.value(), record);
  if (ret != 0) {
    DINGO_LOG(ERROR) << "Decode failed, ret: " << ret;
    return;
  }
  if (record.size() != 1) {
    DINGO_LOG(ERROR) << "record size invalid, size: " << record.size();
    return;
  }

  int64_t result = 0;
  if (record[0].type() == typeid(int64_t)) {
    result = std::any_cast<int64_t>(record[0]);
    DINGO_LOG(INFO) << "count : " << result << std::endl;
  } else {
    DINGO_LOG(INFO) << "type not match ，expect int64_t，actual: " << record[0].type().name() << std::endl;
    return;
  }

  ShowTotalCount(result);
}

void Pretty::Show(const ShowRegionPeersParam& param) {
  struct LagInfo {
    int64_t region_id;
    int64_t store_id;
    std::string raft_location;
    int64_t lag;
  };

  struct ReplicaCountIssueInfo {
    int64_t region_id;
    std::string table_or_index_type;
    std::string expected_replica_num;
    int64_t region_replica_num;
  };

  struct PeerStatusIssueInfo {
    int64_t region_id;
    int64_t store_id;
    std::string server_location;
    std::string raft_location;
    std::string raft_role;
    std::string replica_state;
  };

  struct PeerRangeIssueInfo {
    int64_t region_id;
    int64_t store_id;
    std::string server_location;
    std::string raft_location;
    std::string raft_role;
    std::string replica_range;
  };

  struct PeerStat {
    int64_t store_id;
    std::string server_location;
    std::string raft_location;
    std::string raft_role;
    std::string replica_state = "N/A";
    std::string replica_range = "N/A";
    std::string range_start_key;
    std::string range_end_key;
    bool has_range = false;
    int64_t term = -1;
    int64_t last_log_index = -1;
    int64_t applied_index = -1;
    std::string ToString() const {
      return fmt::format(
          "StoreId={}, ServerLocation={}, RaftLocation={}, RaftRole={}, ReplicaState={}, \
                          ReplicaRange={}, Term={}, LastLogIndex={}, AppliedIndex={}",
          store_id, server_location, raft_location, raft_role, replica_state, replica_range, term, last_log_index,
          applied_index);
    }
  };

  std::vector<LagInfo> severe_lags;
  std::vector<LagInfo> warn_lags;
  std::vector<PeerStatusIssueInfo> peer_status_issue_peers;
  std::vector<PeerRangeIssueInfo> peer_range_issue_peers;
  std::vector<ReplicaCountIssueInfo> replica_count_inconsistent_regions;

  for (const auto& region : param.regions) {
    // print region basic info
    const auto& definition = region.definition();

    // gather peer stats
    std::vector<PeerStat> peer_stats;
    peer_stats.reserve(definition.peers_size());

    for (const auto& peer : definition.peers()) {
      PeerStat stat;
      stat.store_id = peer.store_id();
      stat.server_location = dingodb::Helper::LocationToString(peer.server_location());
      stat.raft_location = dingodb::Helper::LocationToString(peer.raft_location());
      if (peer.store_id() == region.leader_store_id()) {
        stat.raft_role = "Leader";
      } else if (peer.role() == dingodb::pb::common::PeerRole::LEARNER) {
        stat.raft_role = "Learner";
      } else {
        stat.raft_role = "Follower";
      }

      // query raft meta for term/applied_index
      auto region_raft_meta_iter = param.raft_meta_map.find(region.id());
      if (region_raft_meta_iter != param.raft_meta_map.end()) {
        auto peer_raft_meta_iter = region_raft_meta_iter->second.find(peer.store_id());
        if (peer_raft_meta_iter != region_raft_meta_iter->second.end()) {
          stat.term = peer_raft_meta_iter->second.term();
          stat.applied_index = peer_raft_meta_iter->second.applied_index();
        }
      }

      // query raft log meta for last_log_index
      auto region_raft_log_meta_iter = param.raft_log_meta_map.find(region.id());
      if (region_raft_log_meta_iter != param.raft_log_meta_map.end()) {
        auto peer_raft_log_meta_iter = region_raft_log_meta_iter->second.find(peer.store_id());
        if (peer_raft_log_meta_iter != region_raft_log_meta_iter->second.end()) {
          stat.last_log_index = peer_raft_log_meta_iter->second.last_index();
        }
      }

      // query region detail for replica state/range
      auto region_meta_detail_iter = param.region_meta_detail_map.find(region.id());
      if (region_meta_detail_iter != param.region_meta_detail_map.end()) {
        auto peer_region_meta_detail_iter = region_meta_detail_iter->second.find(peer.store_id());
        if (peer_region_meta_detail_iter != region_meta_detail_iter->second.end()) {
          const auto& store_region = peer_region_meta_detail_iter->second;
          stat.replica_state = dingodb::pb::common::StoreRegionState_Name(store_region.state());
          stat.range_start_key = store_region.definition().range().start_key();
          stat.range_end_key = store_region.definition().range().end_key();
          stat.has_range = true;
          stat.replica_range = fmt::format("[{}, {})", dingodb::Helper::StringToHex(stat.range_start_key),
                                           dingodb::Helper::StringToHex(stat.range_end_key));
        }
      }

      peer_stats.push_back(stat);
    }

    auto bool_to_consistent_str = [](bool value) { return value ? "YES" : "NO"; };

    // check replica count consistency
    int64_t region_replica_num = definition.peers_size();
    int64_t table_or_index_id = definition.table_id();
    std::string table_or_index_type = "Table";
    if (table_or_index_id <= 0) {
      table_or_index_id = definition.index_id();
      table_or_index_type = "Index";
    }
    int64_t table_or_index_replica_num = -1;
    bool replica_count_consistent = false;
    auto table_or_index_iter = param.table_index_replica_num_map.find(table_or_index_id);
    if (table_or_index_iter != param.table_index_replica_num_map.end()) {
      table_or_index_replica_num = table_or_index_iter->second;
      replica_count_consistent = (region_replica_num == table_or_index_replica_num);
    }

    // check peer status consistency
    bool peer_status_consistent = !peer_stats.empty();
    std::string base_replica_state;
    for (const auto& stat : peer_stats) {
      if (stat.replica_state == "N/A") {
        peer_status_consistent = false;
        break;
      }
      if (base_replica_state.empty()) {
        base_replica_state = stat.replica_state;
      } else if (base_replica_state != stat.replica_state) {
        peer_status_consistent = false;
        break;
      }
    }

    // check peer range consistency
    bool peer_range_consistent = !peer_stats.empty();
    std::string base_start_key;
    std::string base_end_key;
    for (const auto& stat : peer_stats) {
      if (!stat.has_range) {
        peer_range_consistent = false;
        break;
      }
      if (base_start_key.empty() && base_end_key.empty()) {
        base_start_key = stat.range_start_key;
        base_end_key = stat.range_end_key;
      } else if (base_start_key != stat.range_start_key || base_end_key != stat.range_end_key) {
        peer_range_consistent = false;
        break;
      }
      if (stat.range_start_key >= stat.range_end_key) {
        peer_range_consistent = false;
        break;
      }
    }
    // print region info and consistency check results
    auto region_info_str =
        fmt::format("RegionId={} Name={} State={} TableId={} IndexId={} LeaderStoreId={} Epoch=[conf={}, ver={}]",
                    region.id(), definition.name(), dingodb::pb::common::RegionState_Name(region.state()),
                    definition.table_id(), definition.index_id(), region.leader_store_id(),
                    definition.epoch().conf_version(), definition.epoch().version());

    auto peer_status_consistent_str =
        fmt::format("PeerStatusConsistent={}", bool_to_consistent_str(peer_status_consistent));

    auto peer_range_consistent_str =
        fmt::format("PeerRangeConsistent={}", bool_to_consistent_str(peer_range_consistent));

    std::string table_or_index_replica_num_str =
        table_or_index_replica_num >= 0 ? fmt::format("{}", table_or_index_replica_num) : "N/A";
    auto replica_count_check_str =
        fmt::format("ReplicaCountCheck={{{}ReplicaNum={}, RegionReplicaNum={}, Consistent={}}}", table_or_index_type,
                    table_or_index_replica_num_str, region_replica_num,
                    table_or_index_replica_num >= 0 ? bool_to_consistent_str(replica_count_consistent) : "N/A");

    if (!peer_status_consistent) {
      for (const auto& stat : peer_stats) {
        peer_status_issue_peers.push_back(
            {region.id(), stat.store_id, stat.server_location, stat.raft_location, stat.raft_role, stat.replica_state});
      }
    }
    if (!peer_range_consistent) {
      for (const auto& stat : peer_stats) {
        peer_range_issue_peers.push_back(
            {region.id(), stat.store_id, stat.server_location, stat.raft_location, stat.raft_role, stat.replica_range});
      }
    }
    if (table_or_index_replica_num >= 0 && !replica_count_consistent) {
      replica_count_inconsistent_regions.push_back(
          {region.id(), table_or_index_type, table_or_index_replica_num_str, region_replica_num});
    }

    if (!param.issues_only) {
      std::cout << region_info_str << std::endl;
      std::cout << peer_status_consistent_str << std::endl;
      std::cout << peer_range_consistent_str << std::endl;
      std::cout << replica_count_check_str << std::endl;
    }

    // determine base applied index for lag calculation
    int64_t leader_applied_index = -1;
    int64_t max_applied_index = -1;
    for (const auto& stat : peer_stats) {
      if (stat.applied_index >= 0) {
        max_applied_index = std::max(max_applied_index, stat.applied_index);
        if (stat.store_id == region.leader_store_id()) {
          leader_applied_index = stat.applied_index;
        }
      }
    }
    int64_t base_applied_index = (leader_applied_index >= 0) ? leader_applied_index : max_applied_index;

    // peer stats to table
    std::vector<std::vector<std::string>> rows;
    rows = {{"StoreId", "ServerLocation", "RaftLocation", "RaftRole", "ReplicaState", "Term", "LastLogIndex",
             "AppliedIndex", "ApplyLag"}};

    std::string replica_ranges_str = "ReplicaRanges: ";

    std::sort(peer_stats.begin(), peer_stats.end(),
              [](const PeerStat& a, const PeerStat& b) { return a.store_id < b.store_id; });

    for (const auto& stat : peer_stats) {
      std::string term_str = stat.term >= 0 ? fmt::format("{}", stat.term) : "N/A";
      std::string last_log_index_str = stat.last_log_index >= 0 ? fmt::format("{}", stat.last_log_index) : "N/A";
      std::string applied_index_str = stat.applied_index >= 0 ? fmt::format("{}", stat.applied_index) : "N/A";

      // calculate lag
      std::string apply_lag_str = "N/A";
      if (base_applied_index >= 0 && stat.applied_index >= 0) {
        auto lag = base_applied_index - stat.applied_index;
        std::string lag_suffix;
        if (param.apply_lag_severe >= 0 && lag >= param.apply_lag_severe) {
          lag_suffix = " (SEVERE)";
          severe_lags.push_back({region.id(), stat.store_id, stat.raft_location, lag});
        } else if (param.apply_lag_warn >= 0 && lag >= param.apply_lag_warn) {
          lag_suffix = " (WARN)";
          warn_lags.push_back({region.id(), stat.store_id, stat.raft_location, lag});
        }
        apply_lag_str = fmt::format("{}{}", lag, lag_suffix);
      }

      // row for each peer
      rows.push_back({fmt::format("{}", stat.store_id), stat.server_location, stat.raft_location, stat.raft_role,
                      stat.replica_state, term_str, last_log_index_str, applied_index_str, apply_lag_str});
      if (replica_ranges_str != "ReplicaRanges: ") {
        replica_ranges_str += " | ";
      }
      replica_ranges_str += fmt::format("{}={}", stat.store_id, stat.replica_range);
    }

    if (!param.issues_only) {
      std::cout << replica_ranges_str << std::endl;
      PrintTableAdaptive(rows);
      std::cout << std::endl;
    }
  }

  // summary of lagging peers and inconsistent regions
  auto show_empty_summary = [&](const std::string& title) { std::cout << title << ":\nNone\n" << std::endl; };

  auto show_title = [&](const std::string& title) { std::cout << title << ":\n"; };

  auto show_lag_summary = [&](const std::vector<LagInfo>& lags, const std::string& title) {
    if (lags.empty()) {
      show_empty_summary(title);
      return;
    }
    show_title(title);
    std::vector<std::vector<std::string>> rows = {{"RegionId", "StoreId", "RaftLocation", "ApplyLag"}};
    for (const auto& item : lags) {
      rows.push_back({fmt::format("{}", item.region_id), fmt::format("{}", item.store_id), item.raft_location,
                      fmt::format("{}", item.lag)});
    }
    PrintTableAdaptive(rows);
    std::cout << std::endl;
  };

  auto show_replica_count_issue_summary = [&](const std::vector<ReplicaCountIssueInfo>& issues,
                                              const std::string& title) {
    if (issues.empty()) {
      show_empty_summary(title);
      return;
    }
    show_title(title);
    std::vector<std::vector<std::string>> rows = {{"RegionId", "Type", "ExpectedReplicaNum", "RegionReplicaNum"}};
    for (const auto& item : issues) {
      rows.push_back({fmt::format("{}", item.region_id), item.table_or_index_type, item.expected_replica_num,
                      fmt::format("{}", item.region_replica_num)});
    }
    PrintTableAdaptive(rows);
    std::cout << std::endl;
  };

  auto show_peer_status_issue_summary = [&](const std::vector<PeerStatusIssueInfo>& issues, const std::string& title) {
    if (issues.empty()) {
      show_empty_summary(title);
      return;
    }
    show_title(title);
    std::vector<std::vector<std::string>> rows = {
        {"RegionId", "StoreId", "ServerLocation", "RaftLocation", "RaftRole", "ReplicaState"}};
    for (const auto& item : issues) {
      rows.push_back({fmt::format("{}", item.region_id), fmt::format("{}", item.store_id), item.server_location,
                      item.raft_location, item.raft_role, item.replica_state});
    }
    PrintTableAdaptive(rows);
    std::cout << std::endl;
  };

  auto show_peer_range_issue_summary = [&](const std::vector<PeerRangeIssueInfo>& issues, const std::string& title) {
    if (issues.empty()) {
      show_empty_summary(title);
      return;
    }
    show_title(title);
    std::vector<std::vector<std::string>> rows = {
        {"RegionId", "StoreId", "ServerLocation", "RaftLocation", "RaftRole", "ReplicaRange"}};
    for (const auto& item : issues) {
      rows.push_back({fmt::format("{}", item.region_id), fmt::format("{}", item.store_id), item.server_location,
                      item.raft_location, item.raft_role, item.replica_range});
    }
    PrintTableAdaptive(rows);
    std::cout << std::endl;
  };

  show_lag_summary(severe_lags, "Severe lag peers");
  show_lag_summary(warn_lags, "Warn lag peers");
  show_peer_status_issue_summary(peer_status_issue_peers, "PeerStatusConsistent failed regions");
  show_peer_range_issue_summary(peer_range_issue_peers, "PeerRangeConsistent failed regions");
  show_replica_count_issue_summary(replica_count_inconsistent_regions, "ReplicaCountCheck failed regions");
}

Pretty::ShowRegionTreeParam::ShowRegionTreeParam(dingodb::pb::debug::DebugResponse& in_response,
                                                 const std::vector<int64_t>& in_region_ids, bool in_show_ancestor,
                                                 bool in_dot_format)
    : response(in_response), region_ids(in_region_ids), show_ancestor(in_show_ancestor), dot_format(in_dot_format) {
  for (const auto& region : response.region_meta_details().regions()) {
    region_map[region.id()] = &region;
    region_childs[region.parent_id()].push_back(&region);
  }
}

void Pretty::ShowRegionTreeParam::SetSchema(const dingodb::pb::meta::Schema& in_schema) { schema = in_schema; }

void Pretty::ShowRegionTreeParam::SetTableDefinitionWithId(const TableDefinitionWithId& in_table_def) {
  table_def = in_table_def;
}

void Pretty::ShowRegionTreeParam::AddIndexDefinitionWithId(const TableDefinitionWithId& in_index_def) {
  if (in_index_def.table_id().entity_id() > 0) {
    index_defs[in_index_def.table_id().entity_id()] = in_index_def;
  }
}

class RegionTreeOutputHelper {
  using Region = dingodb::pb::store_internal::Region;

 public:
  explicit RegionTreeOutputHelper(const Pretty::ShowRegionTreeParam& param) : param_(param) {
    if (param_.dot_format) {
      std::cout << "digraph RegionTree {" << std::endl;
    }
  }

  ~RegionTreeOutputHelper() {
    if (param_.dot_format) {
      std::cout << "}" << std::endl;
    }
  }

 public:
  void PrintTableHierarchy() const {
    if (!param_.table_def.has_table_id() || !param_.table_def.has_table_definition()) {
      return;
    }

    // tenant, schema
    int64_t tenant_id = param_.table_def.tenant_id();  // might be 0
    int64_t schema_id = param_.table_def.table_id().parent_entity_id();
    std::string schema_name = param_.schema.name();

    // table
    int64_t table_id = param_.table_def.table_id().entity_id();
    std::string table_name = param_.table_def.table_definition().name();

    const auto& table_partitions = param_.table_def.table_definition().table_partition().partitions();

    auto build_partition_ids = [](const auto& partitions) {
      std::string part_ids;
      for (const auto& partition : partitions) {
        if (!part_ids.empty()) {
          part_ids += " ";
        }
        part_ids += fmt::format("{}", partition.id().entity_id());
      }
      return part_ids;
    };

    if (param_.dot_format) {
      // print tenant node
      std::string tenant_node_name = fmt::format("Tenant_{}", tenant_id);
      std::cout << fmt::format("{} [label=\"TenantId={}\", color=\"blue\"];", tenant_node_name, tenant_id) << std::endl;

      // print schema node and edge from tenant to schema
      std::string schema_node_name = fmt::format("Schema_{}", schema_id);
      std::cout << fmt::format("{} [label=\"SchemaId={}({})\", color=\"blue\"];", schema_node_name, schema_id,
                               schema_name)
                << std::endl;
      std::cout << fmt::format("{} -> {};", tenant_node_name, schema_node_name) << std::endl;

      // print table node and edge from schema to table
      std::string table_node_name = fmt::format("Table_{}", table_id);
      std::cout << fmt::format("{} [label=\"TableId={}({})\", color=\"blue\"];", table_node_name, table_id, table_name)
                << std::endl;
      std::cout << fmt::format("{} -> {};", schema_node_name, table_node_name) << std::endl;

      for (const auto& partition : table_partitions) {
        std::string partition_node_name = BuildPartitionNodeName(partition.id().entity_id());
        std::cout << fmt::format("{} [label=\"PartitionId={}\", color=\"blue\"];", partition_node_name,
                                 partition.id().entity_id())
                  << std::endl;
        std::cout << fmt::format("{} -> {};", table_node_name, partition_node_name) << std::endl;
      }

      for (const auto& [index_id, index_def] : param_.index_defs) {
        std::string index_node_name = fmt::format("Index_{}", index_id);
        std::cout << fmt::format("{} [label=\"IndexId={}({})\", color=\"blue\"];", index_node_name, index_id,
                                 index_def.table_definition().name())
                  << std::endl;
        std::cout << fmt::format("{} -> {};", table_node_name, index_node_name) << std::endl;

        for (const auto& partition : index_def.table_definition().table_partition().partitions()) {
          std::string partition_node_name = BuildPartitionNodeName(partition.id().entity_id());
          std::cout << fmt::format("{} [label=\"PartitionId={}\", color=\"blue\"];", partition_node_name,
                                   partition.id().entity_id())
                    << std::endl;
          std::cout << fmt::format("{} -> {};", index_node_name, partition_node_name) << std::endl;
        }
      }
    } else {
      std::cout << fmt::format("TenantId={}", tenant_id) << std::endl;
      std::cout << fmt::format("  SchemaId={}({})", schema_id, schema_name) << std::endl;
      std::cout << fmt::format("    TableId={}({})", table_id, table_name);
      std::string table_part_ids = build_partition_ids(table_partitions);
      if (!table_part_ids.empty()) {
        std::cout << fmt::format(" Partitions[{}]", table_part_ids);
      }
      std::cout << std::endl;

      for (const auto& [index_id, index_def] : param_.index_defs) {
        std::cout << fmt::format("      IndexId={}({})", index_id, index_def.table_definition().name());
        std::string part_ids = build_partition_ids(index_def.table_definition().table_partition().partitions());
        if (!part_ids.empty()) {
          std::cout << fmt::format(" Partitions[{}]", part_ids);
        }
        std::cout << std::endl;
      }
      std::cout << std::endl;
    }
  }

  void PrintRegion(const Region* region) const {
    auto parent_id = region->parent_id();
    bool parent_not_found = parent_id > 0 && param_.region_map.find(parent_id) == param_.region_map.end();
    if (param_.dot_format) {
      std::string node_name = fmt::format("Region_{}", region->id());
      std::string label = fmt::format("Id={}\\n State={}", region->id(), RegionStateToString(region->state()));
      // handle parent relationship
      if (parent_not_found) {
        label += fmt::format(" (Parent {} not found)", parent_id);
      } else {
        // no parent or parent found, just print the current node
      }
      // print current node
      std::cout << fmt::format("{} [label=\"{}\"];", node_name, label) << std::endl;

      if (parent_not_found || parent_id <= 0) {
        // if parent not found or parent_id <= 0, try to connect with partition node using part_id
        std::string partition_node_name = BuildPartitionNodeName(region->definition().part_id());
        std::cout << fmt::format("{} -> {}[style = dashed];", partition_node_name, node_name) << std::endl;
      } else {
        // print edge from parent to current node
        std::string parent_node_name = fmt::format("Region_{}", parent_id);
        std::cout << fmt::format("{} -> {};", parent_node_name, node_name) << std::endl;
      }
    } else {
      std::string parent_not_found_str = parent_not_found ? fmt::format(" (Parent {} not found)", parent_id) : "";
      std::cout << BuildRegionInfo(region) << parent_not_found_str << std::endl;
    }
  }

  // parent must be printed before child
  void PrintChildRegion(const Region* region, const std::string& prefix = "") const {
    if (param_.dot_format) {
      // print current node
      std::string node_name = fmt::format("Region_{}", region->id());
      std::string label = fmt::format("Id={}\\n State={}", region->id(), RegionStateToString(region->state()));
      std::cout << fmt::format("{} [label=\"{}\"];", node_name, label) << std::endl;
      // print edge from parent to child
      std::string parent_node_name = fmt::format("Region_{}", region->parent_id());
      std::cout << fmt::format("{} -> {};", parent_node_name, node_name) << std::endl;
    } else {
      std::cout << BuildRegionInfo(region, prefix) << std::endl;
    }
  }

  void PrintRegionNotFound(int64_t region_id) const {
    if (param_.dot_format) {
      std::string node_name = fmt::format("Region_{}", region_id);
      std::string label = fmt::format("RegionId={} (not found)", region_id);
      std::cout << fmt::format("{} [label=\"{}\" style=dashed];", node_name, label) << std::endl;
    } else {
      std::cout << "Region " << region_id << " not found in debug response." << std::endl;
    }
  }

  void PrintRoot2Target(const std::vector<const Region*>& ancestors) const {
    if (!param_.dot_format) {
      std::cout << "Region path (root -> target):" << std::endl;
    }
    for (auto iter = ancestors.rbegin(); iter != ancestors.rend(); ++iter) {
      PrintRegion(*iter);
    }
  }

  void PrintChildTreeTitle() const {
    if (!param_.dot_format) {
      std::cout << "Child tree:" << std::endl;
    }
  }

  void PrintTargetTreeEnd() const {
    if (!param_.dot_format) {
      std::cout << std::endl;
    }
  }

 private:
  std::string BuildRegionInfo(const Region* region, const std::string& prefix = "") const {
    return fmt::format("{}RegionId={} ParentId={} PartId={} State={}", prefix, region->id(), region->parent_id(),
                       region->definition().part_id(), RegionStateToString(region->state()));
  }

  std::string RegionStateToString(int state) const { return dingodb::pb::common::StoreRegionState_Name(state); }

  std::string BuildPartitionNodeName(int64_t partition_id) const { return fmt::format("Partition_{}", partition_id); }

 private:
  const Pretty::ShowRegionTreeParam& param_;
};

static void Show(const Pretty::ShowRegionTreeParam& param, const RegionTreeOutputHelper& helper, int64_t region_id) {
  param.shown_region_ids.insert(region_id);

  auto region_iter = param.region_map.find(region_id);
  if (region_iter == param.region_map.end()) {
    helper.PrintRegionNotFound(region_id);
    return;
  }

  if (param.show_ancestor) {
    // find parent path to root (target region -> parent -> ... -> root)
    std::vector<const dingodb::pb::store_internal::Region*> parents;
    const dingodb::pb::store_internal::Region* current_region = region_iter->second;
    while (current_region != nullptr) {
      parents.push_back(current_region);
      auto parent_iter = param.region_map.find(current_region->parent_id());
      if (parent_iter == param.region_map.end()) {
        break;
      }
      current_region = parent_iter->second;
      param.shown_region_ids.insert(current_region->id());
    }

    // print region path from root to target region (root -> ... -> parent -> target region)
    helper.PrintRoot2Target(parents);
  } else {
    // just print the target region
    helper.PrintRegion(region_iter->second);
  }

  // print child tree using DFS
  std::vector<std::pair<const dingodb::pb::store_internal::Region*, int>> stack;
  auto add_children = [&](int64_t parent_id, int depth) {
    auto child_iter = param.region_childs.find(parent_id);
    if (child_iter == param.region_childs.end()) {
      return;
    }
    const auto& children = child_iter->second;
    for (auto iter = children.rbegin(); iter != children.rend(); ++iter) {
      stack.emplace_back(*iter, depth);
    }
  };

  add_children(region_id, 1);
  if (!stack.empty()) {
    helper.PrintChildTreeTitle();
  }
  while (!stack.empty()) {
    auto [child, depth] = stack.back();
    stack.pop_back();
    helper.PrintChildRegion(child, std::string(depth * 2, ' '));
    param.shown_region_ids.insert(child->id());
    add_children(child->id(), depth + 1);
  }
  helper.PrintTargetTreeEnd();
}

void Pretty::Show(const ShowRegionTreeParam& param) {
  RegionTreeOutputHelper output_helper(param);
  output_helper.PrintTableHierarchy();

  for (const auto& region_id : param.region_ids) {
    auto iter = param.region_map.find(region_id);
    if (iter == param.region_map.end()) {
      client_v2::Show(param, output_helper, region_id);
      continue;
    }
    auto parent_id = iter->second->parent_id();
    if (parent_id <= 0 || param.region_map.find(parent_id) == param.region_map.end()) {
      // is root region or parent region not found, just show the region and its child tree
      client_v2::Show(param, output_helper, region_id);
    }
  }

  for (const auto& region_id : param.region_ids) {
    if (param.shown_region_ids.find(region_id) == param.shown_region_ids.end()) {
      // this region is not shown yet, show it with child tree
      client_v2::Show(param, output_helper, region_id);
    }
  }
}

}  // namespace client_v2