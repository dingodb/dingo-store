
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client_v2/tools.h"

#include <cstdint>
#include <iostream>

#include "client_v2/helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "coordinator/tso_control.h"
#include "proto/version.pb.h"

namespace client_v2 {

void SetUpToolSubCommands(CLI::App &app) {
  SetUpStringToHex(app);
  SetUpHexToString(app);
  SetUpEncodeTablePrefixToHex(app);
  SetUpEncodeVectorPrefixToHex(app);
  SetUpDecodeTablePrefix(app);
  SetUpDecodeVectorPrefix(app);
  SetUpOctalToHex(app);
  SetUpCoordinatorDebug(app);
  SetUpTransformTimeStamp(app);
}

std::string EncodeUint64(int64_t value) {
  std::string str(reinterpret_cast<const char *>(&value), sizeof(value));
  std::reverse(str.begin(), str.end());
  return str;
}

int64_t DecodeUint64(const std::string &str) {
  if (str.size() != sizeof(int64_t)) {
    throw std::invalid_argument("Invalid string size for int64_t decoding");
  }

  std::string reversed_str(str.rbegin(), str.rend());
  int64_t value;
  std::memcpy(&value, reversed_str.data(), sizeof(value));
  return value;
}

void SetUpStringToHex(CLI::App &app) {
  auto opt = std::make_shared<StringToHexOptions>();
  auto *cmd = app.add_subcommand("StringToHex", "String to hex")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunStringToHex(*opt); });
}

void RunStringToHex(StringToHexOptions const &opt) {
  auto str = client_v2::StringToHex(opt.key);
  std::cout << fmt::format("key: {} to hex: {}", opt.key, str) << std::endl;
}

void SetUpHexToString(CLI::App &app) {
  auto opt = std::make_shared<HexToStringOptions>();
  auto *cmd = app.add_subcommand("HexToString", "Hex to string")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunHexToString(*opt); });
}

void RunHexToString(HexToStringOptions const &opt) {
  auto str = client_v2::HexToString(opt.key);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpEncodeTablePrefixToHex(CLI::App &app) {
  auto opt = std::make_shared<EncodeTablePrefixToHexOptions>();
  auto *cmd = app.add_subcommand("EncodeTablePrefixToHex", "Encode table prefix to hex")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--part_id", opt->part_id, "Request parameter part_id");
  cmd->add_option("--region_prefix", opt->region_prefix, "Request parameter region_prefix")->required();
  cmd->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  cmd->callback([opt]() { RunEncodeTablePrefixToHexr(*opt); });
}

void RunEncodeTablePrefixToHexr(EncodeTablePrefixToHexOptions const &opt) {
  std::string region_header;
  std::string key = opt.key;
  if (opt.key_is_hex) {
    key = client_v2::HexToString(opt.key);
  }
  if (opt.key.empty()) {
    region_header = client_v2::TablePrefixToHex(opt.region_prefix, opt.part_id);
  } else if (opt.part_id == 0) {
    region_header = client_v2::TablePrefixToHex(opt.region_prefix, key);
  } else {
    region_header = client_v2::TablePrefixToHex(opt.region_prefix, opt.part_id, key);
  }
  std::cout << fmt::format("prefix: {} part_id: {}, key: {} to key: {}", opt.region_prefix, opt.part_id, opt.key,
                           region_header)
            << std::endl;
}

void SetUpEncodeVectorPrefixToHex(CLI::App &app) {
  auto opt = std::make_shared<EncodeVectorPrefixToHexOptions>();
  auto *cmd = app.add_subcommand("EncodeVectorPrefixToHex", "Encode vector prefix to hex")->group("Tool Command");
  cmd->add_option("--vector_id", opt->vector_id, "Request parameter key")->required();
  cmd->add_option("--part_id", opt->part_id, "Request parameter part_id");
  cmd->add_option("--region_prefix", opt->region_prefix, "Request parameter region_prefix")->required();

  cmd->callback([opt]() { RunEncodeVectorPrefixToHex(*opt); });
}

void RunEncodeVectorPrefixToHex(EncodeVectorPrefixToHexOptions const &opt) {
  std::string region_header;
  if (opt.vector_id == 0) {
    region_header = client_v2::VectorPrefixToHex(opt.region_prefix, opt.part_id);
  } else {
    region_header = client_v2::VectorPrefixToHex(opt.region_prefix, opt.part_id, opt.vector_id);
  }
  std::cout << fmt::format("prefix: {} part_id: {}, vector_id {} to key(hex): [{}]", opt.region_prefix, opt.part_id,
                           opt.vector_id, region_header)
            << std::endl;
}

void SetUpDecodeTablePrefix(CLI::App &app) {
  auto opt = std::make_shared<DecodeTablePrefixOptions>();
  auto *cmd = app.add_subcommand("DecodeTablePrefix", "Decode table prefix")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--part_id", opt->part_id, "Request parameter part_id")->group("Coordinator Manager Commands");
  cmd->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  cmd->callback([opt]() { RunDecodeTablePrefix(*opt); });
}

void RunDecodeTablePrefix(DecodeTablePrefixOptions const &opt) {
  std::string key = opt.key;
  if (!opt.key_is_hex) {
    key = client_v2::StringToHex(opt.key);
  }
  bool has_part_id = opt.part_id > 0;
  std::cout << fmt::format("hex: {} to key: {}", opt.key, client_v2::HexToTablePrefix(key, has_part_id)) << std::endl;
}

void SetUpDecodeVectorPrefix(CLI::App &app) {
  auto opt = std::make_shared<DecodeVectorPrefixOptions>();
  auto *cmd = app.add_subcommand("DecodeVectorPrefix", "Decode vector prefix")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  cmd->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  cmd->callback([opt]() { RunDecodeVectorPrefix(*opt); });
}

void RunDecodeVectorPrefix(DecodeVectorPrefixOptions const &opt) {
  std::string key = opt.key;
  if (!opt.key_is_hex) {
    key = client_v2::StringToHex(opt.key);
  }
  std::cout << fmt::format("hex: {} to key: {}", opt.key, client_v2::HexToVectorPrefix(key)) << std::endl;
}

void SetUpOctalToHex(CLI::App &app) {
  auto opt = std::make_shared<OctalToHexOptions>();
  auto *cmd = app.add_subcommand("OctalToHex", "Octal to hex")->group("Tool Command");
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunOctalToHex(*opt); });
}

void RunOctalToHex(OctalToHexOptions const &opt) {
  auto str = client_v2::OctalToHex(opt.key);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpCoordinatorDebug(CLI::App &app) {
  auto opt = std::make_shared<CoordinatorDebugOptions>();
  auto *cmd = app.add_subcommand("CoordinatorDebug", "Coordinator debug")->group("Tool Command");
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  cmd->callback([opt]() { RunCoordinatorDebug(*opt); });
}

void RunCoordinatorDebug(CoordinatorDebugOptions const &opt) {
  dingodb::pb::common::VectorWithDistance vector_with_distance;
  vector_with_distance.set_distance(-1.1920929e-07);

  DINGO_LOG(INFO) << " 1111 " << vector_with_distance.DebugString();

  vector_with_distance.set_distance(1.0F - (-1.1920929e-07));

  DINGO_LOG(INFO) << " 2222 " << vector_with_distance.DebugString();

  int64_t test1 = 1001;
  auto encode_result = EncodeUint64(test1);

  std::string start_key = dingodb::Helper::HexToString(opt.start_key);
  std::string end_key = dingodb::Helper::HexToString(opt.end_key);

  auto real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);

  if (start_key.size() < end_key.size()) {
    start_key.resize(end_key.size(), 0);
  } else {
    end_key.resize(start_key.size(), 0);
  }

  std::vector<uint8_t> start_vec(start_key.begin(), start_key.end());
  std::vector<uint8_t> end_vec(end_key.begin(), end_key.end());

  // calc the mid value between start_vec and end_vec
  std::vector<uint8_t> diff = dingodb::Helper::SubtractByteArrays(start_vec, end_vec);
  std::vector<uint8_t> half_diff = dingodb::Helper::DivideByteArrayByTwo(diff);
  std::vector<uint8_t> mid = dingodb::Helper::AddByteArrays(start_vec, half_diff);

  std::string mid_key(mid.begin(), mid.end());

  std::vector<uint8_t> half = dingodb::Helper::DivideByteArrayByTwo(start_vec);

  std::cout << "start_key:    " << dingodb::Helper::StringToHex(start_key) << std::endl;
  std::cout << "end_key:      " << dingodb::Helper::StringToHex(end_key) << std::endl;
  std::cout << "half_diff:    " << dingodb::Helper::StringToHex(std::string(half_diff.begin(), half_diff.end()))
            << std::endl;
  std::cout << "half:         " << dingodb::Helper::StringToHex(std::string(half.begin(), half.end())) << std::endl;
}

void SetUpTransformTimeStamp(CLI::App &app) {
  auto opt = std::make_shared<TransformTimeStampOptions>();
  auto *cmd = app.add_subcommand("TransformTS", "Transform timestamp to current time")->group("Tool Command");
  cmd->add_option("--ts", opt->ts, "Request parameter ts")->required();
  cmd->callback([opt]() { RunTransformTimeStamp(*opt); });
}

void RunTransformTimeStamp(TransformTimeStampOptions const &opt) {
  int64_t timestamp = ((opt.ts >> 18) + dingodb::kBaseTimestampMs) / 1000;
  std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(timestamp);
  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm *tm_ptr = std::localtime(&time);
  std::ostringstream oss;
  oss << std::put_time(tm_ptr, "%Y-%m-%dT%H:%M:%SZ");  // RFC 3339
  std::cout << oss.str();
}

}  // namespace client_v2