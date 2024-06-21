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

#include <dirent.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "proto/common.pb.h"

static const std::string &kDefaultCf = "default";  // NOLINT

static const std::vector<std::string> kAllCFs = {kDefaultCf};

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "log:\n"
    "  path: " +
    kLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kStorePath + "\n";

class RawRocksEngineBugTest : public testing::Test {
 protected:  // NOLINT
  static void SetUpTestSuite() { dingodb::Helper::CreateDirectories(kStorePath); }
  static void TearDownTestSuite() { dingodb::Helper::RemoveAllFileOrDirectory(kRootPath); }

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(RawRocksEngineBugTest, test) {
  auto engine = std::make_shared<dingodb::RocksRawEngine>();

  auto config = std::make_shared<dingodb::YamlConfig>();
  ASSERT_EQ(0, config->Load(kYamlConfigContent));

  ASSERT_TRUE(engine->Init(config, kAllCFs));

  {
    auto reader = engine->Reader();

    const std::string &key = "key";
    std::string value;

    butil::Status ok = reader->KvGet(kDefaultCf, key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOT_FOUND);
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key");
    kv.set_value("value");

    auto writer = engine->Writer();

    butil::Status ok = writer->KvPut(kDefaultCf, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  engine->Close();
  engine->Destroy();
}
