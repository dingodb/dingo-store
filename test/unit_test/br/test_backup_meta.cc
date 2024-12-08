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

#include "br/backup_meta.h"
#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/utils.h"
#include "fmt/core.h"

class BrBackupMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string backupts = "2024-12-31 14:39:00 +08:00";
  inline static int64_t backuptso_internal = 0;
  inline static std::string storage = "local://./backup";
  inline static std::string storage_internal = "./backup";

  inline static std::shared_ptr<br::BackupMeta> backup_meta;

  inline static std::shared_ptr<dingodb::pb::common::RegionMap> region_maps;
};

TEST_F(BrBackupMetaTest, Init) {
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

  backup_meta =
      std::make_shared<br::BackupMeta>(coor_interaction, store_interaction, index_interaction, document_interaction,
                                       backupts, backuptso_internal, storage, storage_internal);

  status = backup_meta->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupMetaTest, SetRegionMap) {
  auto [status, interaction] = br::InteractionManager::GetInstance().CloneCoordinatorInteraction();

  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_tenant_id(-1);  // get all tenants region map

  status = interaction->SendRequest("CoordinatorService", "GetRegionMap", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
  }

  region_maps = std::make_shared<dingodb::pb::common::RegionMap>(response.regionmap());

  DINGO_LOG(INFO) << fmt::format("region_maps size : {}", region_maps->regions_size());

  for (const auto& region : region_maps->regions()) {
    DINGO_LOG(INFO) << fmt::format("region_id: {}, region: {}", region.id(), region.DebugString());
  }
}

TEST_F(BrBackupMetaTest, Run) {
  butil::Status status = backup_meta->Run(region_maps);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupMetaTest, Finish) {
  butil::Status status = backup_meta->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrBackupMetaTest, GetSqlMetaRegionList) {
  std::vector<int64_t> sql_meta_region_list;
  sql_meta_region_list = backup_meta->GetSqlMetaRegionList();

  for (const auto& region_id : sql_meta_region_list) {
    DINGO_LOG(INFO) << fmt::format("region: {}", region_id);
  }
}

TEST_F(BrBackupMetaTest, GetBackupMeta) {
  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_item;
  backup_meta_item = backup_meta->GetBackupMeta();
  EXPECT_NE(backup_meta.get(), nullptr);
  DINGO_LOG(INFO) << fmt::format("backup_meta: {}", backup_meta_item->DebugString());
}

TEST_F(BrBackupMetaTest, GetIdEpochTypeAndValue) {
  const auto& [status, id_epoch_type_and_value] = backup_meta->GetIdEpochTypeAndValue();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  DINGO_LOG(INFO) << fmt::format("id_epoch_type_and_value: {}", id_epoch_type_and_value->DebugString());
}

TEST_F(BrBackupMetaTest, GetAllTableIncrement) {
  const auto& [status, table_increment_group] = backup_meta->GetAllTableIncrement();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  DINGO_LOG(INFO) << fmt::format("id_epoch_type_and_value: {}", table_increment_group->DebugString());
}