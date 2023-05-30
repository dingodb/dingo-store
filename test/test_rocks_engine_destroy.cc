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

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/raw_rocks_engine.h"
#include "engine/rocks_engine.h"
#include "proto/common.pb.h"
#include "server/server.h"

template <typename T>
class RawRocksEngineBugTest : public testing::Test {
 public:
  std::shared_ptr<dingodb::Config> GetConfig() { return config_; }

  dingodb::RawRocksEngine &GetRawRocksEngine() { return raw_raw_rocks_engine_; }

 protected:  // NOLINT
  void SetUp() override {
    std::cout << "RawRocksEngineTest::SetUp()" << std::endl;
    server_ = dingodb::Server::GetInstance();
    filename_ = "../../conf/store.yaml";
    server_->SetRole(dingodb::pb::common::ClusterRole::STORE);
    server_->InitConfig(filename_);
    config_manager_ = dingodb::ConfigManager::GetInstance();
    config_ = config_manager_->GetConfig(dingodb::pb::common::ClusterRole::STORE);
  }
  void TearDown() override {}

 private:
  dingodb::Server *server_;
  std::string filename_ = "../../conf/store.yaml";
  dingodb::ConfigManager *config_manager_;
  std::shared_ptr<dingodb::Config> config_;
  dingodb::RawRocksEngine raw_raw_rocks_engine_;
};

using RawRocksEngineBugTestIMPL = testing::Types<void>;
TYPED_TEST_SUITE(RawRocksEngineBugTest, RawRocksEngineBugTestIMPL);

TYPED_TEST(RawRocksEngineBugTest, test) {
  dingodb::RawRocksEngine &raw_rocks_engine = this->GetRawRocksEngine();
  std::shared_ptr<dingodb::Config> config = this->GetConfig();

  bool ret = raw_rocks_engine.Init(config);
  EXPECT_TRUE(ret);

  {
    const std::string &cf_name = "default";
    std::shared_ptr<dingodb::RawEngine::Reader> reader = raw_rocks_engine.NewReader(cf_name);

    const std::string &key = "key";
    std::string value;

    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOT_FOUND);
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key");
    kv.set_value("value");

    const std::string &cf_name = "default";
    std::shared_ptr<dingodb::RawEngine::Writer> writer = raw_rocks_engine.NewWriter(cf_name);

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}
