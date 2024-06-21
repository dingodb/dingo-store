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

#ifndef DINGODB_COMMON_SERIAL_HELPER_H_
#define DINGODB_COMMON_SERIAL_HELPER_H_

#include <cstdint>
#include <string>

#include "proto/common.pb.h"

namespace dingodb {

class SerialHelper {
 public:
  static bool IsLE() {
    uint32_t i = 1;
    char* c = (char*)&i;
    return *c == 1;
  }

  // write value
  static void WriteLong(int64_t value, std::string& output);
  static int64_t ReadLong(const std::string_view& value);

  // write ~value
  static void WriteLongWithNegation(int64_t value, std::string& output);
  static int64_t ReadLongWithNegation(const std::string_view& value);

  // highest bit ~
  static void WriteLongComparable(int64_t data, std::string& output);
  static int64_t ReadLongComparable(const std::string& value);
  static int64_t ReadLongComparable(const std::string_view& value);
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_SERIAL_HELPER_H_