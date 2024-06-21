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

#include "crontab/crontab.h"

class CrontabManagerTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(CrontabManagerTest, immediately) {
  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "test";
  crontab->immediately = true;
  crontab->max_times = 1;
  crontab->interval = 10000;
  crontab->func = [](void* arg) -> void {
    std::string* str = static_cast<std::string*>(arg);
    str->append("0");
  };
  std::string str;
  crontab->arg = &str;

  crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  EXPECT_EQ("0", str);
  crontab_manager.Destroy();
}

TEST(CrontabManagerTest, max_times) {
  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "test";
  crontab->max_times = 3;
  crontab->interval = 1000;
  crontab->func = [](void* arg) -> void {
    std::string* str = static_cast<std::string*>(arg);
    str->append("0");
  };
  std::string str;
  crontab->arg = &str;

  crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(5));

  EXPECT_EQ("000", str);
  crontab_manager.Destroy();
}

TEST(CrontabManagerTest, pause) {
  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "test";
  crontab->max_times = 3;
  crontab->interval = 3000;
  crontab->func = [](void* arg) -> void {
    std::string* str = static_cast<std::string*>(arg);
    str->append("0");
  };
  std::string str;
  crontab->arg = &str;

  uint32_t crontab_id = crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  crontab_manager.PauseCrontab(crontab_id);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  EXPECT_EQ("", str);
  crontab_manager.Destroy();
}
