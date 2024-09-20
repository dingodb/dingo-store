
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

#ifndef DINGODB_CLIENT_TOOLS_H_
#define DINGODB_CLIENT_TOOLS_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/store.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

namespace client_v2 {

void SetUpToolSubCommands(CLI::App &app);
std::string EncodeUint64(int64_t value);
int64_t DecodeUint64(const std::string &str);

// sumcomand tools
struct StringToHexOptions {
  std::string key;
};
void SetUpStringToHex(CLI::App &app);
void RunStringToHex(StringToHexOptions const &opt);

struct HexToStringOptions {
  std::string key;
};
void SetUpHexToString(CLI::App &app);
void RunHexToString(HexToStringOptions const &opt);

struct EncodeTablePrefixToHexOptions {
  std::string key;
  int64_t part_id;
  char region_prefix;
  bool key_is_hex;
};
void SetUpEncodeTablePrefixToHex(CLI::App &app);
void RunEncodeTablePrefixToHexr(EncodeTablePrefixToHexOptions const &opt);

struct EncodeVectorPrefixToHexOptions {
  int64_t vector_id;
  int64_t part_id;
  char region_prefix;
};
void SetUpEncodeVectorPrefixToHex(CLI::App &app);
void RunEncodeVectorPrefixToHex(EncodeVectorPrefixToHexOptions const &opt);

struct DecodeTablePrefixOptions {
  std::string key;
  bool key_is_hex;
  int64_t part_id;
};
void SetUpDecodeTablePrefix(CLI::App &app);
void RunDecodeTablePrefix(DecodeTablePrefixOptions const &opt);

struct DecodeVectorPrefixOptions {
  std::string key;
  bool key_is_hex;
};
void SetUpDecodeVectorPrefix(CLI::App &app);
void RunDecodeVectorPrefix(DecodeVectorPrefixOptions const &opt);

struct OctalToHexOptions {
  std::string key;
};
void SetUpOctalToHex(CLI::App &app);
void RunOctalToHex(OctalToHexOptions const &opt);

struct CoordinatorDebugOptions {
  std::string start_key;
  std::string end_key;
};
void SetUpCoordinatorDebug(CLI::App &app);
void RunCoordinatorDebug(CoordinatorDebugOptions const &opt);

struct TransformTimeStampOptions {
  int64_t ts;
};
void SetUpTransformTimeStamp(CLI::App &app);
void RunTransformTimeStamp(TransformTimeStampOptions const &opt);

struct GenPlainKeyOptions {
  std::string coor_url;
  int64_t id;
  std::string key;
};
void SetUpGenPlainKey(CLI::App &app);
void RunGenPlainKey(GenPlainKeyOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_TOOLS_H_