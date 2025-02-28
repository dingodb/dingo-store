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
#include "br/restore_meta.h"
#include "br/sst_file_reader.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};
    std::vector<std::string> store_addrs = {"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"};

    coor_interaction = std::make_shared<br::ServerInteraction>();
    store_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coor_interaction->Init(coor_addrs));
    EXPECT_TRUE(store_interaction->Init(store_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coor_interaction);
    br::InteractionManager::GetInstance().SetStoreInteraction(store_interaction);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static br::ServerInteractionPtr coor_interaction;
  inline static br::ServerInteractionPtr store_interaction;
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
  inline static std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value;
  inline static std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group;
};

TEST_F(BrRestoreMetaTest, Init) {
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

  iter = kvs.find(dingodb::Constant::kBackupMetaSchemaName);
  if (iter != kvs.end()) {
    dingodb::pb::common::BackupMeta internal_backup_meta;
    auto ret = internal_backup_meta.ParseFromString(iter->second);
    EXPECT_TRUE(ret);
    backup_meta = std::make_shared<dingodb::pb::common::BackupMeta>(internal_backup_meta);
  } else {
    LOG(ERROR) << "backupmeta.schema file not found";
  }

  // find IdEpochTypeAndValueKey
  iter = kvs.find(dingodb::Constant::kIdEpochTypeAndValueKey);
  if (iter == kvs.end()) {
    std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kIdEpochTypeAndValueKey);
    LOG(ERROR) << s;
  }

  dingodb::pb::meta::IdEpochTypeAndValue internal_id_epoch_type_and_value;
  auto ret = internal_id_epoch_type_and_value.ParseFromString(iter->second);
  if (!ret) {
    EXPECT_TRUE(ret);
    std::string s = fmt::format("parse dingodb::pb::meta::IdEpochTypeAndValue failed");
    LOG(ERROR) << s;
  }

  id_epoch_type_and_value =
      std::make_shared<dingodb::pb::meta::IdEpochTypeAndValue>(std::move(internal_id_epoch_type_and_value));

  // find TableIncrementKey
  iter = kvs.find(dingodb::Constant::kTableIncrementKey);
  if (iter == kvs.end()) {
    EXPECT_TRUE(ret);
    std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kTableIncrementKey);
    LOG(ERROR) << s;
  }

  dingodb::pb::meta::TableIncrementGroup internal_table_increment_group;
  ret = internal_table_increment_group.ParseFromString(iter->second);
  if (!ret) {
    EXPECT_TRUE(ret);
    std::string s = fmt::format("parse dingodb::pb::meta::TableIncrementGroup failed");
    LOG(ERROR) << s;
  }

  table_increment_group =
      std::make_shared<dingodb::pb::meta::TableIncrementGroup>(std::move(internal_table_increment_group));
}

TEST_F(BrRestoreMetaTest, RestoreMeta) {
  std::shared_ptr<br::RestoreMeta> restore_meta = std::make_shared<br::RestoreMeta>(
      coor_interaction, store_interaction, restorets, restoretso_internal, storage, storage_internal, backup_meta,
      id_epoch_type_and_value, table_increment_group, create_region_concurrency, restore_region_concurrency,
      create_region_timeout_s, restore_region_timeout_s, replica_num);

  auto status = restore_meta->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_meta->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_meta->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  status = restore_meta->ImportIdEpochTypeToMeta();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  status = restore_meta->CreateOrUpdateAutoIncrementsToMeta();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}