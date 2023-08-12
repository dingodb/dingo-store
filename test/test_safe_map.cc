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
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "butil/containers/flat_map.h"
#include "butil/string_printf.h"
#include "common/logging.h"
#include "common/safe_map.h"

class DingoSafeMapTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(DingoSafeMapTest, DingoSafeMap) {
  dingodb::DingoSafeMap<uint64_t, uint64_t> safe_map;
  safe_map.Init(1000);
  safe_map.Put(1, 1);
  auto val1 = safe_map.Get(1);
  EXPECT_EQ(val1, 1);

  auto ret2 = safe_map.PutIfAbsent(1, 2);
  auto val2 = safe_map.Get(1);
  EXPECT_EQ(ret2, -1);
  EXPECT_EQ(val2, 1);

  auto ret3 = safe_map.PutIfNotEqual(1, 2);
  auto val3 = safe_map.Get(1);
  EXPECT_EQ(ret3, 1);
  EXPECT_EQ(val3, 2);

  safe_map.PutIfExists(2, 2);
  uint64_t val4 = 0;
  auto ret4 = safe_map.Get(2, val4);
  EXPECT_EQ(ret4, -1);
  EXPECT_EQ(val4, 0);

  safe_map.PutIfExists(1, 3);
  auto val5 = safe_map.Get(1);
  EXPECT_EQ(val5, 3);

  std::vector<uint64_t> key_list = {1, 2, 3};
  std::vector<uint64_t> value_list = {1, 2, 3};
  safe_map.MultiPut(key_list, value_list);
  auto val6 = safe_map.Get(3);
  EXPECT_EQ(val6, 3);
  auto val7 = safe_map.Get(2);
  EXPECT_EQ(val7, 2);
  auto val8 = safe_map.Get(1);
  EXPECT_EQ(val8, 1);

  auto ret10 = safe_map.PutIfEqual(3, 4);
  EXPECT_EQ(ret10, -1);
  auto ret11 = safe_map.PutIfEqual(3, 3);
  EXPECT_EQ(ret11, 1);
}

TEST(DingoSafeMapTest, DingoSafeMapCopy) {
  dingodb::DingoSafeMap<uint64_t, uint64_t> safe_map;
  safe_map.Init(1000);

  butil::FlatMap<uint64_t, uint64_t> map2;
  map2.init(100);
  map2.insert(1, 1);
  map2.insert(2, 2);
  map2.insert(3, 3);
  map2.insert(4, 4);
  safe_map.Clear();
  EXPECT_EQ(safe_map.Size(), 0);
  safe_map.CopyFromRawMap(map2);
  EXPECT_EQ(safe_map.Size(), 4);
  auto val9 = safe_map.Get(4);
  EXPECT_EQ(val9, 4);
  auto ret8 = safe_map.Exists(4);
  EXPECT_EQ(ret8, true);

  safe_map.Erase(4);
  auto ret9 = safe_map.Exists(4);
  EXPECT_EQ(ret9, false);

  butil::FlatMap<uint64_t, uint64_t> map3;
  map3.init(100);
  safe_map.GetRawMapCopy(map3);
  EXPECT_EQ(map3.size(), 3);
}

TEST(DingoSafeStdMapTest, DingoSafeStdMapGetRangeValues) {
  dingodb::DingoSafeStdMap<std::string, std::string> safe_map;

  for (int i = 0; i < 1000; i++) {
    safe_map.Put(butil::string_printf("%03d", i), std::to_string(i));
  }

  std::vector<std::string> values;
  auto ret =
      safe_map.GetRangeValues(values, "900", "999", [](const std::string& value) { return value.compare("990") > 0; });

  for (auto& value : values) {
    std::cout << value << std::endl;
  }

  EXPECT_EQ(ret, 8);

  std::cout << std::string("91").compare("900") << std::endl;
  std::cout << std::string("900").compare("91") << std::endl;
  std::cout << std::string("900").compare("910") << std::endl;
  std::cout << std::string("998").compare("990") << std::endl;

  values.clear();
  ret = safe_map.GetRangeValues(
      values, "900", "999", [](const std::string& key) { return key.compare("990") < 0; }, nullptr);

  for (auto& value : values) {
    std::cout << value << std::endl;
  }

  EXPECT_EQ(ret, 90);
}
