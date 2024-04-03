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

#include <gtest/gtest.h>

#include <string>

#include "sdk/expression/coding.h"

namespace dingodb {
namespace sdk {
namespace expression {

TEST(SDKBytesHexConvert, Convert) {
  std::string hex_string1 = "2F4A33";
  std::string bytes = HexStringToBytes(hex_string1);

  EXPECT_EQ(BytesToHexString(bytes), hex_string1);
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb