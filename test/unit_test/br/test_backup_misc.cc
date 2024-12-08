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

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "common/helper.h"
#include "common/logging.h"

class BrBackupMiscTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string backupts = "2024-12-31 14:39:00 +08:00";
  inline static int64_t backuptso_internal = 0;
  inline static std::string storage = "local://./backup";
  inline static std::string storage_internal = "./backup";

  inline static bool region_auto_split_enable_after_finish = false;
  inline static bool region_auto_merge_enable_after_finish = false;

  inline static bool balance_leader_enable_after_finish = false;
  inline static bool balance_region_enable_after_finish = false;

  inline static bool is_gc_stop = false;
  inline static bool is_gc_enable_after_finish = false;
};

TEST_F(BrBackupMiscTest, Init) {
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
}

static butil::Status RegisterBackupToCoordinator(bool is_first, const std::string &task_id,
                                                 const std::string &storage_internal,
                                                 br::ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::RegisterBackupRequest request;
  dingodb::pb::coordinator::RegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(task_id);
  request.set_backup_path(storage_internal);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_backup_start_timestamp(current_now_s);
  }
  request.set_backup_current_timestamp(current_now_s);
  request.set_backup_timeout_s(3000);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, RegisterBackup) {
  butil::Status status;

  status = RegisterBackupToCoordinator(true, "task_id", storage_internal,
                                       br::InteractionManager::GetInstance().GetCoordinatorInteraction());

  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  // status = RegisterBackupToCoordinator(true, "task_id2", storage_internal,
  //                                      br::InteractionManager::GetInstance().GetCoordinatorInteraction());

  // EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status UnregisterBackupToCoordinator(const std::string &task_id,
                                                   br::ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::UnRegisterBackupRequest request;
  dingodb::pb::coordinator::UnRegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(task_id);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, UnregisterBackup) {
  butil::Status status;
  status = UnregisterBackupToCoordinator("task_id2", br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  EXPECT_EQ(status.error_code(), dingodb::pb::error::EBACKUP_TASK_NAME_NOT_MATCH);

  status = UnregisterBackupToCoordinator("task_id", br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status DisableSplitAndMergeToStoreAndIndex(br::ServerInteractionPtr store_interaction,
                                                         br::ServerInteractionPtr index_interaction,
                                                         bool &region_auto_split_enable_after_finish,
                                                         bool &region_auto_merge_enable_after_finish) {
  dingodb::pb::store::ControlConfigRequest request;
  dingodb::pb::store::ControlConfigResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_auto_split;
  config_auto_split.set_name("FLAGS_region_enable_auto_split");
  config_auto_split.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_auto_split));

  dingodb::pb::common::ControlConfigVariable config_auto_merge;
  config_auto_merge.set_name("FLAGS_region_enable_auto_merge");
  config_auto_merge.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_auto_merge));

  butil::Status status = store_interaction->AllSendRequest("StoreService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  for (const auto &config : response.control_config_variable()) {
    if (config.is_error_occurred()) {
      DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
      return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                           config.name().c_str());
    }

    if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_split") {
      region_auto_split_enable_after_finish = true;
    }

    if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_merge") {
      region_auto_merge_enable_after_finish = true;
    }
  }

  DINGO_LOG(INFO) << "DisableSplitAndMergeToStoreAndIndex success." << response.DebugString();

  status = index_interaction->AllSendRequest("IndexService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  for (const auto &config : response.control_config_variable()) {
    if (config.is_error_occurred()) {
      DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
      return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                           config.name().c_str());
    }

    if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_split") {
      region_auto_split_enable_after_finish = true;
    }

    if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_merge") {
      region_auto_merge_enable_after_finish = true;
    }
  }

  DINGO_LOG(INFO) << "DisableSplitAndMergeToStoreAndIndex success." << response.DebugString();

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, DisableSplitAndMerge) {
  butil::Status status;

  status =
      DisableSplitAndMergeToStoreAndIndex(br::InteractionManager::GetInstance().GetStoreInteraction(),
                                          br::InteractionManager::GetInstance().GetIndexInteraction(),
                                          region_auto_split_enable_after_finish, region_auto_merge_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status EnableSplitAndMergeToStoreAndIndex(br::ServerInteractionPtr store_interaction,
                                                        br::ServerInteractionPtr index_interaction,
                                                        bool region_auto_split_enable_after_finish,
                                                        bool region_auto_merge_enable_after_finish) {
  dingodb::pb::store::ControlConfigRequest request;
  dingodb::pb::store::ControlConfigResponse response;

  if (region_auto_split_enable_after_finish) {
    dingodb::pb::common::ControlConfigVariable config_auto_split;
    config_auto_split.set_name("FLAGS_region_enable_auto_split");
    config_auto_split.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_auto_split));
  }

  if (region_auto_merge_enable_after_finish) {
    dingodb::pb::common::ControlConfigVariable config_auto_merge;
    config_auto_merge.set_name("FLAGS_region_enable_auto_merge");
    config_auto_merge.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_auto_merge));
  }

  if (!request.control_config_variable().empty()) {
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    butil::Status status = store_interaction->AllSendRequest("StoreService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }

    DINGO_LOG(INFO) << "EnableSplitAndMergeToStoreAndIndex success." << response.DebugString();

    status = index_interaction->AllSendRequest("IndexService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }
    DINGO_LOG(INFO) << "EnableSplitAndMergeToStoreAndIndex success." << response.DebugString();
  }
  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, EnableSplitAndMerge) {
  butil::Status status;
  status =
      EnableSplitAndMergeToStoreAndIndex(br::InteractionManager::GetInstance().GetStoreInteraction(),
                                         br::InteractionManager::GetInstance().GetIndexInteraction(),
                                         region_auto_split_enable_after_finish, region_auto_merge_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status DisableBalanceToCoordinator(br::ServerInteractionPtr coordinator_interaction,
                                                 bool &balance_leader_enable_after_finish,
                                                 bool &balance_region_enable_after_finish) {
  dingodb::pb::coordinator::ControlConfigRequest request;
  dingodb::pb::coordinator::ControlConfigResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_balance_leader;
  config_balance_leader.set_name("FLAGS_enable_balance_leader");
  config_balance_leader.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_balance_leader));

  dingodb::pb::common::ControlConfigVariable config_balance_region;
  config_balance_region.set_name("FLAGS_enable_balance_region");
  config_balance_region.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_balance_region));

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  for (const auto &config : response.control_config_variable()) {
    if (config.is_error_occurred()) {
      DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
      return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                           config.name().c_str());
    }

    if (!config.is_already_set() && config.name() == "FLAGS_enable_balance_leader") {
      balance_leader_enable_after_finish = true;
    }

    if (!config.is_already_set() && config.name() == "FLAGS_enable_balance_region") {
      balance_region_enable_after_finish = true;
    }
  }

  DINGO_LOG(INFO) << "DisableBalanceToCoordinator success." << response.DebugString();

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, DisableBalance) {
  butil::Status status;
  status = DisableBalanceToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction(),
                                       balance_leader_enable_after_finish, balance_region_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status EnableBalanceToCoordinator(br::ServerInteractionPtr coordinator_interaction,
                                                bool balance_leader_enable_after_finish,
                                                bool balance_region_enable_after_finish) {
  dingodb::pb::coordinator::ControlConfigRequest request;
  dingodb::pb::coordinator::ControlConfigResponse response;

  if (balance_leader_enable_after_finish) {
    dingodb::pb::common::ControlConfigVariable config_balance_leader;
    config_balance_leader.set_name("FLAGS_enable_balance_leader");
    config_balance_leader.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_balance_leader));
  }

  if (balance_region_enable_after_finish) {
    dingodb::pb::common::ControlConfigVariable config_balance_region;
    config_balance_region.set_name("FLAGS_enable_balance_region");
    config_balance_region.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_balance_region));
  }

  if (!request.control_config_variable().empty()) {
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    butil::Status status =
        coordinator_interaction->AllSendRequest("CoordinatorService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }
  }

  DINGO_LOG(INFO) << "EnableBalanceToCoordinator success." << response.DebugString();

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, EnableBalance) {
  butil::Status status;
  status = EnableBalanceToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction(),
                                      balance_leader_enable_after_finish, balance_region_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status SetGcStop(bool &is_gc_stop, bool &is_gc_enable_after_finish) {
  if (is_gc_stop) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "Set GC stop ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  is_gc_stop = true;
  is_gc_enable_after_finish = true;

  DINGO_LOG(INFO) << "GC is stopped. Backup will enable GC.  if backup is finished.";

  return butil::Status::OK(
      
  );
}

TEST_F(BrBackupMiscTest, GcStop) {
  butil::Status status;
  status = SetGcStop(is_gc_stop, is_gc_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

static butil::Status SetGcStart(bool &is_gc_stop, bool &is_gc_enable_after_finish) {
  if (!is_gc_enable_after_finish) {
    return butil::Status::OK();
  }
  DINGO_LOG(INFO) << "Set GC start ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  is_gc_stop = false;
  is_gc_enable_after_finish = false;

  DINGO_LOG(INFO) << "Set GC start success.";

  return butil::Status::OK();
}

TEST_F(BrBackupMiscTest, GcStart) {
  butil::Status status;
  status = SetGcStart(is_gc_stop, is_gc_enable_after_finish);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}