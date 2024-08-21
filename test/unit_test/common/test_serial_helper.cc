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
#include <pthread.h>
#include <sched.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/helper.h"
#include "common/serial_helper.h"
#include "fmt/core.h"

void Print(const unsigned char* addr, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    std::cout << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(*(addr + i)) << " ";
  }

  std::cout << std::endl;
}

class SerialHelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SerialHelperTest, Misc) {
  GTEST_SKIP() << "Skip...";
  {
    int32_t v = 0x14131211;
    Print((const unsigned char*)&v, 4);

    int32_t vv = v >> 8;

    Print((const unsigned char*)&vv, 4);
  }

  {
    int32_t v = 0xff131211;
    Print((const unsigned char*)&v, 4);

    int32_t vv = v >> 8;

    Print((const unsigned char*)&vv, 4);
  }

  {
    uint32_t v = 0xff131211;
    Print((const unsigned char*)&v, 4);

    uint32_t vv = v >> 8;

    Print((const unsigned char*)&vv, 4);
  }

  {
    int32_t v = 0xff131211;
    Print((const unsigned char*)&v, 4);

    uint32_t vv = static_cast<uint32_t>(v);
    Print((const unsigned char*)&vv, 4);
  }

  {
    uint32_t v = 0xff131211;
    Print((const unsigned char*)&v, 4);

    int32_t vv = static_cast<int32_t>(v);
    Print((const unsigned char*)&vv, 4);
  }

  {
    int32_t v = 0x14131211;
    Print((const unsigned char*)&v, 4);

    int32_t vv = ~v;
    Print((const unsigned char*)&vv, 4);
  }

  {
    int32_t v = 0xff131211;
    Print((const unsigned char*)&v, 4);

    int32_t vv = ~v;
    Print((const unsigned char*)&vv, 4);
  }
}

TEST_F(SerialHelperTest, ReadWriteLong) {
  {
    int64_t value = 0;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = 1;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = 12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = -1;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = -12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = INT64_MAX;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }

  {
    int64_t value = INT64_MIN;
    std::string output;
    dingodb::SerialHelper::WriteLong(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLong(output));
  }
}

TEST_F(SerialHelperTest, ReadWriteLongWithNegation) {
  {
    int64_t value = 0;
    std::string output;
    dingodb::SerialHelper::WriteLongWithNegation(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongWithNegation(output));
  }

  {
    int64_t value = 1;
    std::string output;
    dingodb::SerialHelper::WriteLongWithNegation(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongWithNegation(output));
  }

  {
    int64_t value = 12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLongWithNegation(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongWithNegation(output));
  }

  {
    int64_t value = INT64_MAX;
    std::string output;
    dingodb::SerialHelper::WriteLongWithNegation(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongWithNegation(output));
  }

  {
    int64_t value1 = 0;
    std::string output1;
    dingodb::SerialHelper::WriteLongWithNegation(value1, output1);

    int64_t value2 = 1;
    std::string output2;
    dingodb::SerialHelper::WriteLongWithNegation(value2, output2);

    EXPECT_LT(output2, output1);
  }

  {
    int64_t value1 = 123456789;
    std::string output1;
    dingodb::SerialHelper::WriteLongWithNegation(value1, output1);

    int64_t value2 = 222222222;
    std::string output2;
    dingodb::SerialHelper::WriteLongWithNegation(value2, output2);

    EXPECT_LT(output2, output1);
  }
}

TEST_F(SerialHelperTest, ReadWriteLongComparable) {
  {
    int64_t value = 0;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = 1;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = 12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = -1;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = -12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = INT64_MAX;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value = INT64_MIN;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    EXPECT_EQ(value, dingodb::SerialHelper::ReadLongComparable(output));
  }

  {
    int64_t value1 = 0;
    std::string output1;
    dingodb::SerialHelper::WriteLongComparable(value1, output1);

    int64_t value2 = 1;
    std::string output2;
    dingodb::SerialHelper::WriteLongComparable(value2, output2);

    EXPECT_LT(output1, output2);
  }

  {
    int64_t value1 = 123456789;
    std::string output1;
    dingodb::SerialHelper::WriteLongComparable(value1, output1);

    int64_t value2 = 222222222;
    std::string output2;
    dingodb::SerialHelper::WriteLongComparable(value2, output2);

    EXPECT_LT(output1, output2);
  }

  {
    int64_t value1 = -10;
    std::string output1;
    dingodb::SerialHelper::WriteLongComparable(value1, output1);

    int64_t value2 = -1;
    std::string output2;
    dingodb::SerialHelper::WriteLongComparable(value2, output2);

    EXPECT_LT(output1, output2);
  }

  {
    int64_t value1 = -10;
    std::string output1;
    dingodb::SerialHelper::WriteLongComparable(value1, output1);

    int64_t value2 = 3;
    std::string output2;
    dingodb::SerialHelper::WriteLongComparable(value2, output2);

    EXPECT_LT(output1, output2);
  }
}

TEST_F(SerialHelperTest, Performace) {
  GTEST_SKIP() << "Skip...";

  int64_t start_time = dingodb::Helper::TimestampMs();

  for (int i = 0; i < 10000000; ++i) {
    int64_t value = 12345678900001;
    std::string output;
    dingodb::SerialHelper::WriteLongComparable(value, output);

    // Print((const unsigned char*)&value, 8);
    int64_t vv = dingodb::SerialHelper::ReadLongComparable(output);
    // Print((const unsigned char*)&vv, 8);
    // EXPECT_EQ(value, vv);
  }

  std::cout << "eplapsed time: " << dingodb::Helper::TimestampMs() - start_time << std::endl;
}