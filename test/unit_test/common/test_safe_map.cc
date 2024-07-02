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
#include "common/helper.h"
#include "common/safe_map.h"

class DingoSafeMapTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(DingoSafeMapTest, DingoSafeMap) {
  dingodb::DingoSafeMap<int64_t, int64_t> safe_map;
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
  int64_t val4 = 0;
  auto ret4 = safe_map.Get(2, val4);
  EXPECT_EQ(ret4, -1);
  EXPECT_EQ(val4, 0);

  safe_map.PutIfExists(1, 3);
  auto val5 = safe_map.Get(1);
  EXPECT_EQ(val5, 3);

  std::vector<int64_t> key_list = {1, 2, 3};
  std::vector<int64_t> value_list = {1, 2, 3};
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
  dingodb::DingoSafeMap<int64_t, int64_t> safe_map;
  safe_map.Init(1000);

  butil::FlatMap<int64_t, int64_t> map2;
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

  butil::FlatMap<int64_t, int64_t> map3;
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
    LOG(INFO) << value;
  }

  EXPECT_EQ(ret, 8);

  LOG(INFO) << std::string("91").compare("900");
  LOG(INFO) << std::string("900").compare("91");
  LOG(INFO) << std::string("900").compare("910");
  LOG(INFO) << std::string("998").compare("990");

  values.clear();
  ret = safe_map.GetRangeValues(
      values, "900", "999", [](const std::string& key) { return key.compare("990") < 0; }, nullptr);

  for (auto& value : values) {
    LOG(INFO) << value;
  }

  EXPECT_EQ(ret, 90);
}

TEST(DingoSafeStdMapTest, DingoSafeStdMapFindIntervalValues) {
  std::vector<std::string> values;
  std::string start_key;
  std::string end_key;
  int ret = 0;
  dingodb::DingoSafeStdMap<std::string, std::string> safe_map;

  safe_map.Put(dingodb::Helper::HexToString("77000000000000ea62"), "77000000000000ea62");
  safe_map.Put(dingodb::Helper::HexToString("77000000000000ea63"), "77000000000000ea63");

  values.clear();
  start_key = dingodb::Helper::HexToString("77000000000000ea61");
  end_key = dingodb::Helper::HexToString("77000000000000ea63");
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << dingodb::Helper::StringToHex(start_key) << "," << dingodb::Helper::StringToHex(end_key) << "]"
              << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = dingodb::Helper::HexToString("7700");
  end_key = dingodb::Helper::HexToString("77000000000000ea63");
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << dingodb::Helper::StringToHex(start_key) << "," << dingodb::Helper::StringToHex(end_key) << "]"
              << value;
  }

  EXPECT_EQ(ret, 1);

  safe_map.Clear();
  safe_map.Put("wa", "wa0");
  safe_map.Put("wa0", "wb");
  safe_map.Put("wb", "wb0");
  safe_map.Put("wb0", "wb5");
  safe_map.Put("wb5", "wc");
  safe_map.Put("wc", "wc0");
  safe_map.Put("wc0", "wd");

  values.clear();
  start_key = "w0";
  end_key = "wa0";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wd";
  end_key = "wd";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 0);

  values.clear();
  start_key = "wd";
  end_key = "wc";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 0);

  values.clear();
  start_key = "wa";
  end_key = "wa0";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wa1";
  end_key = "wa3";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wc1";
  end_key = "wc3";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wa0";
  end_key = "wc0";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 5);

  values.clear();
  start_key = "wb1";
  end_key = "wb2";
  ret = safe_map.FindIntervalValues(
      values, start_key, end_key, [start_key, end_key](const std::string& key) { return key.compare(start_key) <= 0; },
      [start_key, end_key](const std::string& value) { return value.compare(end_key) >= 0; });

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wb1";
  end_key = "wb2";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wc1";
  end_key = "wc2";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wa1";
  end_key = "wa2";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);

  values.clear();
  start_key = "wa1";
  end_key = "wb2";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 3);

  values.clear();
  start_key = "wd";
  end_key = "wd0";
  ret = safe_map.FindIntervalValues(values, start_key, end_key);

  for (auto& value : values) {
    LOG(INFO) << "[" << start_key << "," << end_key << "]" << value;
  }

  EXPECT_EQ(ret, 1);
}

TEST(DingoSafeStdMapTest, DingoSafeStdMapMultiGet) {
  dingodb::DingoSafeStdMap<std::string, std::string> safe_map;

  for (int i = 0; i < 1000; i++) {
    safe_map.Put(butil::string_printf("%03d", i), std::to_string(i));
  }

  std::vector<std::string> keys;
  for (int i = 998; i < 1002; i++) {
    keys.push_back(butil::string_printf("%03d", i));
  }
  std::vector<std::string> values;
  std::vector<bool> exists;

  auto ret = safe_map.MultiGet(keys, values, exists);

  for (auto& value : values) {
    LOG(INFO) << value;
  }

  for (auto exist : exists) {
    LOG(INFO) << exist;
  }

  EXPECT_EQ(ret, 1);
  EXPECT_EQ(values.size(), 4);
  EXPECT_EQ(exists.size(), 4);
}

TEST(DingoSafeStdMapTest, DingoSafeStdMapGetFirstKey) {
  dingodb::DingoSafeStdMap<int64_t, std::string> safe_map;

  int64_t first_key = 0;

  auto ret = safe_map.GetFirstKey(first_key);

  EXPECT_TRUE(ret == 0);

  ret = safe_map.GetLastKey(first_key);

  EXPECT_TRUE(ret == 0);

  for (int64_t i = 0; i < 1000; i++) {
    safe_map.Put(i, std::to_string(i));
  }

  ret = safe_map.GetFirstKey(first_key);

  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(first_key, 0);

  ret = safe_map.GetLastKey(first_key);

  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(first_key, 999);

  safe_map.Erase(0);

  ret = safe_map.GetFirstKey(first_key);
  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(first_key, 1);

  safe_map.Erase(999);

  ret = safe_map.GetLastKey(first_key);
  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(first_key, 998);

  safe_map.Clear();

  ret = safe_map.GetFirstKey(first_key);
  EXPECT_TRUE(ret == 0);

  ret = safe_map.GetLastKey(first_key);
  EXPECT_TRUE(ret == 0);
}

TEST(DingoSafeStdMapTest, DingoSafeStdMapGetRangeKeyValues) {
  dingodb::DingoSafeStdMap<int64_t, std::string> safe_map;

  for (int64_t i = 0; i < 1000; i++) {
    safe_map.Put(i, std::to_string(i));
  }

  std::vector<int64_t> keys;
  std::vector<std::string> values;
  auto ret = safe_map.GetRangeKeyValues(keys, values, 10, 100);

  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(keys.size(), values.size());
  EXPECT_EQ(keys.size(), 90);

  keys.clear();
  values.clear();

  ret = safe_map.GetRangeKeyValues(keys, values, 100, 10);

  EXPECT_TRUE(ret == 0);
  EXPECT_EQ(keys.size(), values.size());
  EXPECT_EQ(keys.size(), 0);

  keys.clear();
  values.clear();

  ret = safe_map.GetRangeKeyValues(keys, values, 100, 100);

  EXPECT_TRUE(ret == 0);
  EXPECT_EQ(keys.size(), values.size());
  EXPECT_EQ(keys.size(), 0);

  keys.clear();
  values.clear();

  ret = safe_map.GetRangeKeyValues(keys, values, 90, 95);

  EXPECT_TRUE(ret > 0);
  EXPECT_EQ(keys.size(), values.size());
  EXPECT_EQ(keys.size(), 5);
}
