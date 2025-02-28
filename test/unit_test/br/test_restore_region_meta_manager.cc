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

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/restore_region_meta.h"
#include "br/restore_region_meta_manager.h"
#include "br/sst_file_reader.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreRegionMetaManagerTest;

static void TestCreateRegionManager(const BrRestoreRegionMetaManagerTest& br_restore_region_meta_manager_test,
                                    const std::string& file_name);

class BrRestoreRegionMetaManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};

    coordinator_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coordinator_interaction->Init(coor_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coordinator_interaction);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
  friend void TestCreateRegionManager(const BrRestoreRegionMetaManagerTest& br_restore_region_meta_manager_test,
                                      const std::string& file_name);

  inline static br::ServerInteractionPtr coordinator_interaction;
  inline static int64_t replica_num = 1;
  inline static std::string backup_meta_region_name;
  inline static int64_t create_region_timeout_s = 60;
  inline static std::string base_dir = "./backup2/";
  inline static std::string storage_internal = "/home/server/work/dingo-store/build/bin/backup2";
  inline static uint32_t concurrency = 10;
};

static void TestCreateRegionManager(const BrRestoreRegionMetaManagerTest& br_restore_region_meta_manager_test,
                                    const std::string& file_name) {
  const std::string file_path = br_restore_region_meta_manager_test.base_dir + file_name;
  br_restore_region_meta_manager_test.backup_meta_region_name = file_name;

  std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> kvs;
  auto status = reader_sst->ReadFile(file_path, kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs =
      std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>();

  for (const auto& [region_id, region_ptr] : kvs) {
    auto region = std::make_shared<dingodb::pb::common::Region>();
    auto ret = region->ParseFromString(region_ptr);
    EXPECT_TRUE(ret);

    id_and_region_kvs->emplace(std::stoll(region_id), region);
  }

  std::shared_ptr<br::RestoreRegionMetaManager> restore_region_meta_manager =
      std::make_shared<br::RestoreRegionMetaManager>(
          br_restore_region_meta_manager_test.coordinator_interaction, br_restore_region_meta_manager_test.concurrency,
          br_restore_region_meta_manager_test.replica_num, br_restore_region_meta_manager_test.storage_internal,
          id_and_region_kvs, br_restore_region_meta_manager_test.backup_meta_region_name,
          br_restore_region_meta_manager_test.create_region_timeout_s);

  status = restore_region_meta_manager->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_meta_manager->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_meta_manager->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestDocumentRegionSdkData) {
  const std::string& file_name = "document_region_sdk_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestDocumentRegionSqlData) {
  const std::string& file_name = "document_region_sql_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestIndexRegionSdkData) {
  const std::string& file_name = "index_region_sdk_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestIndexRegionSqlData) {
  const std::string& file_name = "index_region_sql_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestStoreRegionSdkData) {
  const std::string& file_name = "store_region_sdk_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestStoreRegionSqlData) {
  const std::string& file_name = "store_region_sql_data.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, TestStoreRegionSqlMeta) {
  const std::string& file_name = "store_region_sql_meta.sst";
  TestCreateRegionManager(*this, file_name);
}

TEST_F(BrRestoreRegionMetaManagerTest, StaticsCreateRegion) {}
