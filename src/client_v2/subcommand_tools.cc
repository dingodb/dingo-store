
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

#include <iostream>

#include "client_v2/client_helper.h"
#include "client_v2/subcommand_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "proto/version.pb.h"
#include "subcommand_coordinator.h"
namespace client_v2 {

void SetUpSubcommandStringToHex(CLI::App &app) {
  auto opt = std::make_shared<StringToHexOptions>();
  auto coor = app.add_subcommand("StringToHex", "String to hex")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required();
  coor->callback([opt]() { RunSubcommandStringToHex(*opt); });
}

void RunSubcommandStringToHex(StringToHexOptions const &opt) {
  auto str = client_v2::StringToHex(opt.key);
  DINGO_LOG(INFO) << fmt::format("key: {} to hex: {}", opt.key, str);
  std::cout << fmt::format("key: {} to hex: {}", opt.key, str) << std::endl;
}

void SetUpSubcommandHexToString(CLI::App &app) {
  auto opt = std::make_shared<HexToStringOptions>();
  auto coor = app.add_subcommand("HexToString", "Hex to string")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required();
  coor->callback([opt]() { RunSubcommandHexToString(*opt); });
}

void RunSubcommandHexToString(HexToStringOptions const &opt) {
  auto str = client_v2::HexToString(opt.key);
  DINGO_LOG(INFO) << fmt::format("hex: {} to key: {}", opt.key, str);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpSubcommandEncodeTablePrefixToHex(CLI::App &app) {
  auto opt = std::make_shared<EncodeTablePrefixToHexOptions>();
  auto coor = app.add_subcommand("EncodeTablePrefixToHex", "Encode table prefix to hex")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required();
  coor->add_option("--part_id", opt->part_id, "Request parameter part_id");
  coor->add_option("--region_prefix", opt->region_prefix, "Request parameter region_prefix")->required();
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  coor->callback([opt]() { RunSubcommandEncodeTablePrefixToHexr(*opt); });
}

void RunSubcommandEncodeTablePrefixToHexr(EncodeTablePrefixToHexOptions const &opt) {
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
  DINGO_LOG(INFO) << fmt::format("prefix: {} part_id: {}, key: {} to key: {}", opt.region_prefix, opt.part_id, opt.key,
                                 region_header);
  std::cout << fmt::format("prefix: {} part_id: {}, key: {} to key: {}", opt.region_prefix, opt.part_id, opt.key,
                           region_header)
            << std::endl;
}

void SetUpSubcommandEncodeVectorPrefixToHex(CLI::App &app) {
  auto opt = std::make_shared<EncodeVectorPrefixToHexOptions>();
  auto coor = app.add_subcommand("EncodeVectorPrefixToHex", "Encode vector prefix to hex")->group("Tool Commands");
  coor->add_option("--vector_id", opt->vector_id, "Request parameter key")->required();
  coor->add_option("--part_id", opt->part_id, "Request parameter part_id");
  coor->add_option("--region_prefix", opt->region_prefix, "Request parameter region_prefix")->required();

  coor->callback([opt]() { RunSubcommandEncodeVectorPrefixToHex(*opt); });
}

void RunSubcommandEncodeVectorPrefixToHex(EncodeVectorPrefixToHexOptions const &opt) {
  std::string region_header;
  if (opt.vector_id == 0) {
    region_header = client_v2::VectorPrefixToHex(opt.region_prefix, opt.part_id);
  } else {
    region_header = client_v2::VectorPrefixToHex(opt.region_prefix, opt.part_id, opt.vector_id);
  }
  DINGO_LOG(INFO) << fmt::format("prefix: {} part_id: {}, vector_id {} to key(hex): [{}]", opt.region_prefix,
                                 opt.part_id, opt.vector_id, region_header);
  std::cout << fmt::format("prefix: {} part_id: {}, vector_id {} to key(hex): [{}]", opt.region_prefix, opt.part_id,
                           opt.vector_id, region_header)
            << std::endl;
}

void SetUpSubcommandDecodeTablePrefix(CLI::App &app) {
  auto opt = std::make_shared<DecodeTablePrefixOptions>();
  auto coor = app.add_subcommand("DecodeTablePrefix", "Decode table prefix")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--part_id", opt->part_id, "Request parameter part_id")->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  coor->callback([opt]() { RunSubcommandDecodeTablePrefix(*opt); });
}

void RunSubcommandDecodeTablePrefix(DecodeTablePrefixOptions const &opt) {
  std::string key = opt.key;
  if (!opt.key_is_hex) {
    key = client_v2::StringToHex(opt.key);
  }
  bool has_part_id = opt.part_id > 0;
  DINGO_LOG(INFO) << "part_id: " << opt.part_id << " has_part_id: " << has_part_id;

  auto str = client_v2::HexToTablePrefix(key, has_part_id);
  DINGO_LOG(INFO) << fmt::format("hex: {} to key: {}", opt.key, str);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpSubcommandDecodeVectorPrefix(CLI::App &app) {
  auto opt = std::make_shared<DecodeVectorPrefixOptions>();
  auto coor = app.add_subcommand("DecodeVectorPrefix", "Decode vector prefix")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter region_prefix")->default_val(false);
  coor->callback([opt]() { RunSubcommandDecodeVectorPrefix(*opt); });
}

void RunSubcommandDecodeVectorPrefix(DecodeVectorPrefixOptions const &opt) {
  std::string key = opt.key;
  if (!opt.key_is_hex) {
    key = client_v2::StringToHex(opt.key);
  }
  auto str = client_v2::HexToVectorPrefix(key);
  DINGO_LOG(INFO) << fmt::format("hex: {} to key: {}", opt.key, str);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpSubcommandOctalToHex(CLI::App &app) {
  auto opt = std::make_shared<OctalToHexOptions>();
  auto coor = app.add_subcommand("OctalToHex", "Octal to hex")->group("Tool Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required();
  coor->callback([opt]() { RunSubcommandOctalToHex(*opt); });
}

void RunSubcommandOctalToHex(OctalToHexOptions const &opt) {
  auto str = client_v2::OctalToHex(opt.key);
  DINGO_LOG(INFO) << fmt::format("oct: {} to hex: {}", opt.key, str);
  std::cout << fmt::format("hex: {} to key: {}", opt.key, str) << std::endl;
}

void SetUpSubcommandCoordinatorDebug(CLI::App &app) {
  auto opt = std::make_shared<CoordinatorDebugOptions>();
  auto coor = app.add_subcommand("CoordinatorDebug", "Coordinator debug")->group("Tool Commands");
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  coor->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  coor->callback([opt]() { RunSubcommandCoordinatorDebug(*opt); });
}

void RunSubcommandCoordinatorDebug(CoordinatorDebugOptions const &opt) {
  dingodb::pb::common::VectorWithDistance vector_with_distance;
  vector_with_distance.set_distance(-1.1920929e-07);

  DINGO_LOG(INFO) << " 1111 " << vector_with_distance.DebugString();

  vector_with_distance.set_distance(1.0F - (-1.1920929e-07));

  DINGO_LOG(INFO) << " 2222 " << vector_with_distance.DebugString();

  int64_t test1 = 1001;
  auto encode_result = EncodeUint64(test1);
  DINGO_LOG(INFO) << encode_result.size();
  DINGO_LOG(INFO) << dingodb::Helper::StringToHex(encode_result);

  DINGO_LOG(INFO) << "==========================";

  std::string start_key = dingodb::Helper::HexToString(opt.start_key);
  std::string end_key = dingodb::Helper::HexToString(opt.end_key);

  auto real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);
  DINGO_LOG(INFO) << " mid real  = " << dingodb::Helper::StringToHex(real_mid);

  DINGO_LOG(INFO) << "==========================";

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

  DINGO_LOG(INFO) << "start_key:    " << dingodb::Helper::StringToHex(start_key);
  DINGO_LOG(INFO) << "end_key:      " << dingodb::Helper::StringToHex(end_key);
  DINGO_LOG(INFO) << "diff:         " << dingodb::Helper::StringToHex(std::string(diff.begin(), diff.end()));
  DINGO_LOG(INFO) << "half_diff:    " << dingodb::Helper::StringToHex(std::string(half_diff.begin(), half_diff.end()));
  DINGO_LOG(INFO) << "half:         " << dingodb::Helper::StringToHex(std::string(half.begin(), half.end()));

  DINGO_LOG(INFO) << "mid_key:      " << dingodb::Helper::StringToHex(mid_key.substr(1, mid_key.size() - 1))
                  << std::endl;
  std::cout << "start_key:    " << dingodb::Helper::StringToHex(start_key) << std::endl;
  std::cout << "end_key:      " << dingodb::Helper::StringToHex(end_key) << std::endl;
  std::cout << "half_diff:    " << dingodb::Helper::StringToHex(std::string(half_diff.begin(), half_diff.end()))
            << std::endl;
  std::cout << "half:         " << dingodb::Helper::StringToHex(std::string(half.begin(), half.end())) << std::endl;
}

}  // namespace client_v2