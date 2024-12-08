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

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "br/backup_sdk_meta.h"
#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/utils.h"
#include "common/logging.h"
#include "fmt/core.h"

class BrBackupSdkMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string backupts = "2024-12-30 13:58:00 +08:00";
  inline static int64_t backuptso_internal = 0;
  inline static std::string storage = "local://./backup";
  inline static std::string storage_internal = "./backup";

  inline static std::shared_ptr<br::BackupSdkMeta> backup_sdk_meta;

  inline static std::shared_ptr<dingodb::pb::common::RegionMap> region_maps;
};

TEST_F(BrBackupSdkMetaTest, Init) {
  std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};
  std::vector<std::string> store_addrs = {"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"};
  std::vector<std::string> index_addrs = {"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"};
  std::vector<std::string> document_addrs = {"127.0.0.1:33001", "127.0.0.1:33002", "127.0.0.1:33003"};

  br::ServerInteractionPtr coor_interaction = std::make_shared<br::ServerInteraction>();
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

  std::filesystem::path temp_storage_internal = std::filesystem::absolute(storage_internal);
  if (temp_storage_internal.has_relative_path()) {
    auto filename = temp_storage_internal.filename();
    temp_storage_internal = std::filesystem::absolute(filename);
  }

  // auto temp_storage_internal = std::filesystem::absolute(storage_internal);
  storage_internal = temp_storage_internal.string();
  br::Utils::RemoveAllDir(storage_internal, true);
  br::Utils::CreateDir(storage_internal);

  butil::Status status = br::Utils::ConvertBackupTsToTso(backupts, backuptso_internal);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  backup_sdk_meta = std::make_shared<br::BackupSdkMeta>(coor_interaction, storage_internal);

  status = backup_sdk_meta->GetSdkMetaFromCoordinator();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupSdkMetaTest, Run) {
  butil::Status status = backup_sdk_meta->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupSdkMetaTest, Backup) {
  butil::Status status = backup_sdk_meta->Backup();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupSdkMetaTest, GetBackupMeta) {
  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta;
  backup_meta = backup_sdk_meta->GetBackupMeta();
  EXPECT_NE(backup_meta.get(), nullptr);

  DINGO_LOG(INFO) << backup_meta->DebugString();
}
