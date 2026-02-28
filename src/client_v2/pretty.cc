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

}  // namespace client_v2