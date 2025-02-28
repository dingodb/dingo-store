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

#include <memory>
#include <string>
#include <vector>

#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/restore_region_data_manager.h"
#include "br/sst_file_reader.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreRegionDataManagerTest;

static void TestImportDataToRegionManager(const BrRestoreRegionDataManagerTest& br_restore_region_data_manager_test,
                                          br::ServerInteractionPtr interaction,
                                          const std::string& group_belongs_to_whom, const std::string& region_file_name,
                                          const std::string& cf_file_name);

class BrRestoreRegionDataManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};
    std::vector<std::string> store_addrs = {"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"};
    std::vector<std::string> index_addrs = {"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"};
    std::vector<std::string> document_addrs = {"127.0.0.1:33001", "127.0.0.1:33002", "127.0.0.1:33003"};

    coor_interaction = std::make_shared<br::ServerInteraction>();
    br::ServerInteractionPtr store_interaction = std::make_shared<br::ServerInteraction>();
    br::ServerInteractionPtr index_interaction = std::make_shared<br::ServerInteraction>();
    br::ServerInteractionPtr document_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coor_interaction->Init(coor_addrs));
    EXPECT_TRUE(store_interaction->Init(store_addrs));
    EXPECT_TRUE(index_interaction->Init(index_addrs));
    EXPECT_TRUE(document_interaction->Init(document_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coor_interaction);
    br::InteractionManager::GetInstance().SetStoreInteraction(store_interaction);
    br::InteractionManager::GetInstance().SetIndexInteraction(index_interaction);
    br::InteractionManager::GetInstance().SetDocumentInteraction(document_interaction);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
  friend void TestImportDataToRegionManager(const BrRestoreRegionDataManagerTest& br_restore_region_data_manager_test,
                                            br::ServerInteractionPtr interaction,
                                            const std::string& group_belongs_to_whom,
                                            const std::string& region_file_name, const std::string& cf_file_name);

  inline static br::ServerInteractionPtr coor_interaction;
  inline static int64_t replica_num = 0;
  inline static std::string backup_meta_region_cf_name;
  inline static int64_t restore_region_timeout_s = 60;
  inline static std::string base_dir = "./backup2/";
  inline static std::string restorets;
  inline static int64_t restoretso_internal;
  inline static std::string storage;
  inline static std::string storage_internal;
  inline static uint32_t concurrency = 10;
};

static void TestImportDataToRegionManager(const BrRestoreRegionDataManagerTest& br_restore_region_data_manager_test,
                                          br::ServerInteractionPtr interaction,
                                          const std::string& group_belongs_to_whom, const std::string& region_file_name,
                                          const std::string& cf_file_name) {
  const std::string region_file_path = br_restore_region_data_manager_test.base_dir + region_file_name;

  std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> kvs;
  auto status = reader_sst->ReadFile(region_file_path, kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs =
      std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>();

  for (const auto& [region_id, region_ptr] : kvs) {
    dingodb::pb::common::Region region;
    auto ret = region.ParseFromString(region_ptr);
    EXPECT_TRUE(ret);
    (*id_and_region_kvs)[region.id()] = std::make_shared<dingodb::pb::common::Region>(region);
  }

  const std::string cf_file_path = br_restore_region_data_manager_test.base_dir + cf_file_name;

  reader_sst = std::make_shared<br::SstFileReader>();

  kvs.clear();
  status = reader_sst->ReadFile(cf_file_path, kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      id_and_sst_meta_group_kvs =
          std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>();

  for (const auto& [region_id, group_ptr] : kvs) {
    dingodb::pb::common::BackupDataFileValueSstMetaGroup group;
    auto ret = group.ParseFromString(group_ptr);
    EXPECT_TRUE(ret);
    LOG(INFO) << group.DebugString();
    (*id_and_sst_meta_group_kvs)[std::stoll(region_id)] =
        std::make_shared<dingodb::pb::common::BackupDataFileValueSstMetaGroup>(group);
  }

  std::shared_ptr<br::RestoreRegionDataManager> restore_region_data_manager =
      std::make_shared<br::RestoreRegionDataManager>(
          br_restore_region_data_manager_test.coor_interaction, interaction,
          br_restore_region_data_manager_test.concurrency, br_restore_region_data_manager_test.replica_num,
          br_restore_region_data_manager_test.restorets, br_restore_region_data_manager_test.restoretso_internal,
          br_restore_region_data_manager_test.storage, br_restore_region_data_manager_test.storage_internal,
          id_and_sst_meta_group_kvs, cf_file_name, group_belongs_to_whom,
          br_restore_region_data_manager_test.restore_region_timeout_s, id_and_region_kvs);

  status = restore_region_data_manager->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_data_manager->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_data_manager->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreRegionDataManagerTest, Init) {
  const std::string backupmeta_file_path = base_dir + "backupmeta";

  std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> kvs;
  auto status = reader_sst->ReadFile(backupmeta_file_path, kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  auto iter = kvs.find(dingodb::Constant::kBackupBackupParamKey);
  if (iter != kvs.end()) {
    dingodb::pb::common::BackupParam backup_param;
    auto ret = backup_param.ParseFromString(iter->second);
    EXPECT_TRUE(ret);

    restorets = backup_param.backupts();
    restoretso_internal = backup_param.backuptso_internal();
    storage = backup_param.storage();
    storage_internal = backup_param.storage_internal();
  } else {
    LOG(ERROR) << "backupmeta file not found";
  }
}

TEST_F(BrRestoreRegionDataManagerTest, TestDocumentCfSstMetaSdkData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetDocumentInteraction();
  const std::string& region_file_name = "document_region_sdk_data.sst";
  const std::string& cf_file_name = "document_cf_sst_meta_sdk_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreData;
  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestDocumentCfSstMetaSqlData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetDocumentInteraction();
  const std::string& region_file_name = "document_region_sql_data.sst";
  const std::string& cf_file_name = "document_cf_sst_meta_sql_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreData;
  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestIndexCfSstMetaSdkData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetIndexInteraction();
  const std::string& region_file_name = "index_region_sdk_data.sst";
  const std::string& cf_file_name = "index_cf_sst_meta_sdk_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreData;
  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestIndexCfSstMetaSqlData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetIndexInteraction();
  const std::string& region_file_name = "index_region_sql_data.sst";
  const std::string& cf_file_name = "index_cf_sst_meta_sql_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreData;
  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestStoreCfSstMetaSdkData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  const std::string& region_file_name = "store_region_sdk_data.sst";
  const std::string& cf_file_name = "store_cf_sst_meta_sdk_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreData;

  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestStoreCfSstMetaSqlData) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  const std::string& region_file_name = "store_region_sql_data.sst";
  const std::string& cf_file_name = "store_cf_sst_meta_sql_data.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreMeta;

  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}

TEST_F(BrRestoreRegionDataManagerTest, TestStoreCfSstMetaSqlMeta) {
  br::ServerInteractionPtr interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  const std::string& region_file_name = "store_region_sql_meta.sst";
  const std::string& cf_file_name = "store_cf_sst_meta_sql_meta.sst";
  const std::string& group_belongs_to_whom = dingodb::Constant::kRestoreMeta;

  TestImportDataToRegionManager(*this, interaction, group_belongs_to_whom, region_file_name, cf_file_name);
}
