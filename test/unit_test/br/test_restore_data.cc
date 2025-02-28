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
#include "br/restore_data.h"
#include "br/sst_file_reader.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreDataTest : public testing::Test {
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
  inline static std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta;
};

TEST_F(BrRestoreDataTest, Init) {
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

  iter = kvs.find(dingodb::Constant::kBackupMetaDataFileName);
  if (iter != kvs.end()) {
    dingodb::pb::common::BackupMeta internal_backup_meta;
    auto ret = internal_backup_meta.ParseFromString(iter->second);
    EXPECT_TRUE(ret);
    backup_meta = std::make_shared<dingodb::pb::common::BackupMeta>(internal_backup_meta);
  } else {
    LOG(ERROR) << "backupmeta file not found";
  }
}

TEST_F(BrRestoreDataTest, RestoreData) {
  std::shared_ptr<br::RestoreData> restore_data = std::make_shared<br::RestoreData>(
      coor_interaction, store_interaction, index_interaction, document_interaction, restorets, restoretso_internal,
      storage, storage_internal, backup_meta, create_region_concurrency, restore_region_concurrency,
      create_region_timeout_s, restore_region_timeout_s, replica_num);

  auto status = restore_data->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_data->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_data->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}