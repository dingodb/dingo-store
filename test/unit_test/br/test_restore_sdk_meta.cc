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
#include "br/restore_sdk_meta.h"
#include "br/sst_file_reader.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreSdkMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};

    coor_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coor_interaction->Init(coor_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coor_interaction);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static br::ServerInteractionPtr coor_interaction;

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
  inline static std::shared_ptr<dingodb::pb::common::BackupMeta> coordinator_sdk_meta_sst;
};

TEST_F(BrRestoreSdkMetaTest, Init) {
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

  const std::string backupmeta_schema_file_path = base_dir + "backupmeta.schema";

  reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> backupmeta_schema_kvs;
  status = reader_sst->ReadFile(backupmeta_schema_file_path, backupmeta_schema_kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  // find coordinator_sdk_meta.sst;
  iter = backupmeta_schema_kvs.find(dingodb::Constant::kCoordinatorSdkMetaSstName);
  if (iter != backupmeta_schema_kvs.end()) {
    dingodb::pb::common::BackupMeta internal_coordinator_sdk_meta_sst;
    auto ret = internal_coordinator_sdk_meta_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kCoordinatorSdkMetaSstName);
      LOG(ERROR) << s;
      EXPECT_TRUE(false);
    }
    coordinator_sdk_meta_sst =
        std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_coordinator_sdk_meta_sst));
  }
}

TEST_F(BrRestoreSdkMetaTest, RestoreMeta) {
  std::shared_ptr<br::RestoreSdkMeta> restore_sdk_meta = std::make_shared<br::RestoreSdkMeta>(
      coor_interaction, restorets, restoretso_internal, storage, storage_internal, coordinator_sdk_meta_sst);

  auto status = restore_sdk_meta->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_sdk_meta->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  status = restore_sdk_meta->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}