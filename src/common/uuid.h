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

#ifndef DINGODB_COMMON_UUID_H_
#define DINGODB_COMMON_UUID_H_

#include <cstdint>
#include <string>

namespace dingodb {

class UUIDGenerator {
 public:
  UUIDGenerator() = default;
  ~UUIDGenerator() = default;

  static std::string GenerateUUID();
  static std::string GenerateUUIDV3(const std::string& seed);

 private:
  static std::string GenerateHex(const std::string& seed, uint32_t length);
  static std::string GenerateHex(uint32_t length);
  static unsigned char GenerateRandomChar();
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_UUID_H_