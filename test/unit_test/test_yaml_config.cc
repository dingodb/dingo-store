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

#include "config/yaml_config.h"

class ConfigTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(ConfigTest, scalar_bool) {
  {
    dingodb::YamlConfig config;
    const std::string yaml = "enable: true";

    config.Load(yaml);
    EXPECT_EQ(true, config.GetBool("enable"));
  }

  {
    dingodb::YamlConfig config;
    const std::string yaml = "enable: false";

    config.Load(yaml);
    EXPECT_EQ(false, config.GetBool("enable"));
  }
}

TEST(ConfigTest, scalar_01) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host: dingo.com\n"
      "port: 8400\n"
      "ip: 127.0.0.1";

  config.Load(yaml);
  EXPECT_EQ("dingo.com", config.GetString("host"));
  EXPECT_EQ(8400, config.GetInt("port"));
  EXPECT_EQ("127.0.0.1", config.GetString("ip"));
}

TEST(ConfigTest, scalar_02) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "  port: 8400\n"
      "  ip: 127.0.0.1";

  config.Load(yaml);
  EXPECT_EQ(8400, config.GetInt("host.port"));
  EXPECT_EQ("127.0.0.1", config.GetString("host.ip"));
}

TEST(ConfigTest, scalar_int64) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "raft:\n"
      "  segmentlog_max_segment_size: 34359738368";

  config.Load(yaml);
  EXPECT_EQ(34359738368, config.GetInt64("raft.segmentlog_max_segment_size"));
}

TEST(ConfigTest, scalar_double) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "server:\n"
      "  worker_thread_ratio: 0.5";

  config.Load(yaml);
  EXPECT_EQ(0.5, config.GetDouble("server.worker_thread_ratio"));
}

TEST(ConfigTest, not_exist) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "  port: 8400\n"
      "  ip: 127.0.0.1";

  config.Load(yaml);
  EXPECT_EQ(0, config.GetInt("host.host.host"));
  EXPECT_EQ(0.0, config.GetDouble("host.host.host"));
  EXPECT_EQ("", config.GetString("host.host.host"));
}

TEST(ConfigTest, list_01) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "- 8400\n"
      "- 127.0.0.1";

  config.Load(yaml);
  auto result = config.GetStringList("host");
  std::vector<std::string> expected = {"8400", "127.0.0.1"};
  EXPECT_EQ(result.size(), expected.size());
  for (int i = 0; i < result.size(); ++i) {
    EXPECT_EQ(expected[i], result[i]);
  }
}

TEST(ConfigTest, list_02) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "- 8400\n"
      "- 127.0.0.1";

  config.Load(yaml);
  auto result = config.GetStringList("port");
  std::vector<std::string> expected = {};
  EXPECT_EQ(result.size(), expected.size());
}

TEST(ConfigTest, map_01) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "  port: 8400\n"
      "  ip: 127.0.0.1";

  config.Load(yaml);
  auto result = config.GetStringMap("host");
  for (auto& it : result) {
    if (it.first == "port") {
      EXPECT_EQ("8400", it.second);
    } else if (it.first == "ip") {
      EXPECT_EQ("127.0.0.1", it.second);
    }
  }
}

TEST(ConfigTest, map_02) {
  dingodb::YamlConfig config;

  const std::string yaml =
      "host:\n"
      "  port: 8400\n"
      "  ip: 127.0.0.1";

  config.Load(yaml);
  auto result = config.GetStringMap("port");
  EXPECT_EQ(0, result.size());
}