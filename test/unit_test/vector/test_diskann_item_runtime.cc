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
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "butil/status.h"
#include "common/logging.h"
#include "config/yaml_config.h"
#include "diskann/diskann_item_runtime.h"

namespace dingodb {

const std::string kYamlConfigContent =
    "server:\n"
    "  listen_host: $SERVER_LISTEN_HOST$\n"
    "  host: $SERVER_HOST$\n"
    "  port: $SERVER_PORT$\n"
    "  diskann_import_worker_num: 32 # the number of import worker used by diskann_service\n"
    "  diskann_import_worker_max_pending_num: 1024 # 0 is unlimited\n"
    "  diskann_build_worker_num: 1 # the number of build worker used by diskann_service\n"
    "  diskann_build_worker_max_pending_num: 128 # 0 is unlimited\n"
    "  diskann_load_worker_num: 10 # the number of load worker used by diskann_service\n"
    "  diskann_load_worker_max_pending_num: 512 # 0 is unlimited\n"
    "  diskann_search_worker_num: 1024 # the number of search worker used by diskann_service\n"
    "  diskann_search_worker_max_pending_num: 10240 # 0 is unlimited\n"
    "  diskann_misc_worker_num: 32 # the number of misc worker used by diskann_service\n"
    "  diskann_misc_worker_max_pending_num: 1024 # 0 is unlimited\n";

class DiskANNItemRuntimeTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<Config> config;
};

TEST_F(DiskANNItemRuntimeTest, Init) {
  bool ret = DiskANNItemRuntime::Init(config);
  EXPECT_TRUE(ret);
}
TEST_F(DiskANNItemRuntimeTest, Get) {
  auto& import_worker_set = DiskANNItemRuntime::GetImportWorkerSet();
  auto& build_worker_set = DiskANNItemRuntime::GetBuildWorkerSet();
  auto& load_worker_set = DiskANNItemRuntime::GetLoadWorkerSet();
  auto& search_worker_set = DiskANNItemRuntime::GetSearchWorkerSet();
  auto& misc_worker_set = DiskANNItemRuntime::GetMiscWorkerSet();
  EXPECT_TRUE(import_worker_set);
  EXPECT_TRUE(build_worker_set);
  EXPECT_TRUE(load_worker_set);
  EXPECT_TRUE(search_worker_set);
  EXPECT_TRUE(misc_worker_set);
}

}  // namespace dingodb
