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

#include "common/uuid.h"

#include <cstdint>
#include <iostream>
#include <random>
#include <sstream>
#include <vector>

namespace dingodb {

std::string UUIDGenerator::GenerateUUID() {
  std::ostringstream oss;

  std::vector<uint32_t> lengths = {4, 2, 2, 2, 6};
  for (int i = 0; i < lengths.size(); ++i) {
    oss << (i == 0 ? GenerateHex(lengths[i]) : '-' + GenerateHex(lengths[i]));
  }

  return oss.str();
}

std::string UUIDGenerator::GenerateUUIDV3(const std::string& seed) {
  std::ostringstream oss;

  std::vector<uint32_t> lengths = {4, 2, 2, 2, 6};
  for (int i = 0; i < lengths.size(); ++i) {
    oss << (i == 0 ? GenerateHex(seed, lengths[i]) : '-' + GenerateHex(seed, lengths[i]));
  }

  return oss.str();
}

std::string UUIDGenerator::GenerateHex(const std::string& seed, uint32_t length) {
  std::ostringstream oss;
  int seed_pos = 0;
  for (uint32_t i = 0; i < length; ++i) {
    unsigned char random_char = seed.at(++seed_pos % seed.size());
    std::stringstream hex_stream;
    hex_stream << std::hex << int(random_char);
    std::string hex_str = hex_stream.str();
    oss << (hex_str.length() < 2 ? '0' + hex_str : hex_str);
  }

  return oss.str();
}

std::string UUIDGenerator::GenerateHex(uint32_t length) {
  std::ostringstream oss;
  for (uint32_t i = 0; i < length; ++i) {
    unsigned char random_char = GenerateRandomChar();
    std::stringstream hex_stream;
    hex_stream << std::hex << int(random_char);
    std::string hex_str = hex_stream.str();
    oss << (hex_str.length() < 2 ? '0' + hex_str : hex_str);
  }

  return oss.str();
}

unsigned char UUIDGenerator::GenerateRandomChar() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  return static_cast<unsigned char>(dis(gen));
}

}  // namespace dingodb