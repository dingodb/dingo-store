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

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "common/logging.h"

class DingoLoggerTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// TEST(DingoLoggerTest, DingoLOGTest) {
//   google::InitGoogleLogging("DingoLoggerTest1");
//   EXPECT_EQ(FLAGS_v, 0);
//   int debug_level = 100;
//   FLAGS_v = debug_level;
//   DINGO_LOG(INFO) << "This is an info log message.";
//   DINGO_LOG(DEBUG) << "This is an debug log from dingodb.";
//   VLOG(1) << "This is a log";
//   EXPECT_EQ(FLAGS_v, debug_level);
// }
