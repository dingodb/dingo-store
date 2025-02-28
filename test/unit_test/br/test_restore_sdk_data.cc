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
#include "br/restore_sdk_data.h"
#include "br/sst_file_reader.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreSdkDataTest;

static void TestRestoreData(const BrRestoreSdkDataTest& restore_data,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst);

class BrRestoreSdkDataTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};
    std::vector<std::string> store_addrs = {"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"};
    std::vector<std::string> index_addrs = {"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"};
    std::vector<std::string> document_addrs = {"127.0.0.1:33001", "127.0.0.1:33002", "127.0.0.1:33003"};

    coor_interaction = std::make_shared<br::ServerInteraction>();
    store_interaction = std::make_shared<br::ServerInteraction>();
    index_interaction = std::make_shared<br::ServerInteraction>();
    document_interaction = std::make_shared<br::ServerInteraction>();

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
  friend void TestRestoreData(const BrRestoreSdkDataTest& restore_data,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst,
                              std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst);

  inline static br::ServerInteractionPtr coor_interaction;
  inline static br::ServerInteractionPtr store_interaction;
  inline static br::ServerInteractionPtr index_interaction;
  inline static br::ServerInteractionPtr document_interaction;
  inline static int64_t replica_num = 0;
  inline static std::string backup_meta_region_cf_name;
  inline static int64_t create_region_timeout_s = 60;
  inline static int64_t restore_region_timeout_s = 60;
  inline static std::string base_dir = "./backup2/";
  inline static std::string restorets;
  inline static int64_t restoretso_internal;
  inline static std::string storage;
  inline static std::string storage_internal;
  inline static uint32_t create_region_concurrency = 10;
  inline static uint32_t restore_region_concurrency = 5;
};

static void TestRestoreData(const BrRestoreSdkDataTest& restore_data,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst,
                            std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst) {
  std::shared_ptr<br::RestoreSdkData> restore_region_data_manager = std::make_shared<br::RestoreSdkData>(
      restore_data.coor_interaction, restore_data.store_interaction, restore_data.index_interaction,
      restore_data.document_interaction, restore_data.restorets, restore_data.restoretso_internal, restore_data.storage,
      restore_data.storage_internal, store_region_data_sst, store_cf_sst_meta_data_sst, index_region_data_sst,
      index_cf_sst_meta_data_sst, document_region_data_sst, document_cf_sst_meta_data_sst,
      restore_data.create_region_concurrency, restore_data.restore_region_concurrency,
      restore_data.create_region_timeout_s, restore_data.restore_region_timeout_s, restore_data.replica_num);

  auto status = restore_region_data_manager->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_data_manager->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_region_data_manager->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreSdkDataTest, Init) {
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

TEST_F(BrRestoreSdkDataTest, RestoreSdkData) {
  const std::string backupmeta_datafile_file_path = base_dir + "backupmeta.datafile";

  std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> backupmeta_datafile_kvs;
  auto status = reader_sst->ReadFile(backupmeta_datafile_file_path, backupmeta_datafile_kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  // find store_region_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst;
  auto iter = backupmeta_datafile_kvs.find(dingodb::Constant::kStoreRegionSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_store_region_sdk_data_sst;
    auto ret = internal_store_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreRegionSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    store_region_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_region_sdk_data_sst));
  }

  // find store_cf_sst_meta_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst;
  iter = backupmeta_datafile_kvs.find(dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_store_cf_sst_meta_sdk_data_sst;
    auto ret = internal_store_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    store_cf_sst_meta_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_cf_sst_meta_sdk_data_sst));
  }

  // find index_region_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst;
  iter = backupmeta_datafile_kvs.find(dingodb::Constant::kIndexRegionSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_index_region_sdk_data_sst;
    auto ret = internal_index_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexRegionSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    index_region_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_region_sdk_data_sst));
  }

  // find index_cf_sst_meta_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst;
  iter = backupmeta_datafile_kvs.find(dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_index_cf_sst_meta_sdk_data_sst;
    auto ret = internal_index_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    index_cf_sst_meta_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_cf_sst_meta_sdk_data_sst));
  }

  // find document_region_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst;
  iter = backupmeta_datafile_kvs.find(dingodb::Constant::kDocumentRegionSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_document_region_sdk_data_sst;
    auto ret = internal_document_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentRegionSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    document_region_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_region_sdk_data_sst));
  }

  // find document_cf_sst_meta_sdk_data.sst
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst;
  iter = backupmeta_datafile_kvs.find(dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_document_cf_sst_meta_sdk_data_sst;
    auto ret = internal_document_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    document_cf_sst_meta_sdk_data_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_cf_sst_meta_sdk_data_sst));
  }

  TestRestoreData(*this, store_region_sdk_data_sst, store_cf_sst_meta_sdk_data_sst, index_region_sdk_data_sst,
                  index_cf_sst_meta_sdk_data_sst, document_region_sdk_data_sst, document_cf_sst_meta_sdk_data_sst);
}
