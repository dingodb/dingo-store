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
#include <fstream>
#include <string>
#include <vector>

#include "br/helper.h"
#include "br/interation.h"
#include "br/utils.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"

class BrInterationTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static br::ServerInteractionPtr coor_interaction;
  inline static br::ServerInteractionPtr store_interaction;
  inline static br::ServerInteractionPtr index_interaction;
  inline static br::ServerInteractionPtr document_interaction;
};

TEST_F(BrInterationTest, InitSingle) {
  std::string coor_addrs = "127.0.0.1:32001, 127.0.0.1:32002, 127.0.0.1:32003";
  std::string store_addrs = "127.0.0.1:30001, 127.0.0.1:30002, 127.0.0.1:30003";
  std::string index_addrs = "127.0.0.1:31001, 127.0.0.1:31002, 127.0.0.1:31003";
  std::string document_addrs = "127.0.0.1:33001, 127.0.0.1:33002, 127.0.0.1:33003";

  coor_interaction = std::make_shared<br::ServerInteraction>();
  store_interaction = std::make_shared<br::ServerInteraction>();
  index_interaction = std::make_shared<br::ServerInteraction>();
  document_interaction = std::make_shared<br::ServerInteraction>();

  EXPECT_TRUE(coor_interaction->Init(coor_addrs));
  EXPECT_TRUE(store_interaction->Init(store_addrs));
  EXPECT_TRUE(index_interaction->Init(index_addrs));
  EXPECT_TRUE(document_interaction->Init(document_addrs));

  coor_interaction.reset();
  store_interaction.reset();
  index_interaction.reset();
  document_interaction.reset();
}

TEST_F(BrInterationTest, Init) {
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
}

TEST_F(BrInterationTest, GetAddrs) {
  std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};
  std::vector<std::string> store_addrs = {"127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003"};
  std::vector<std::string> index_addrs = {"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"};
  std::vector<std::string> document_addrs = {"127.0.0.1:33001", "127.0.0.1:33002", "127.0.0.1:33003"};

  auto addrs = coor_interaction->GetAddrs();
  for (int i = 0; i < addrs.size(); ++i) {
    EXPECT_EQ(addrs[i], coor_addrs[i]);
  }

  addrs = store_interaction->GetAddrs();
  EXPECT_EQ(store_addrs, addrs);
  for (int i = 0; i < addrs.size(); ++i) {
    EXPECT_EQ(addrs[i], store_addrs[i]);
  }

  addrs = index_interaction->GetAddrs();
  EXPECT_EQ(index_addrs, addrs);
  for (int i = 0; i < addrs.size(); ++i) {
    EXPECT_EQ(addrs[i], index_addrs[i]);
  }

  addrs = document_interaction->GetAddrs();
  EXPECT_EQ(document_addrs, addrs);
  for (int i = 0; i < addrs.size(); ++i) {
    EXPECT_EQ(addrs[i], document_addrs[i]);
  }
}

TEST_F(BrInterationTest, SendRequest) {
  // coordinator
  {
    dingodb::pb::coordinator::HelloRequest request;
    dingodb::pb::coordinator::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = coor_interaction->SendRequest("CoordinatorService", "Hello", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    DINGO_LOG(INFO) << "local version info : " << response.version_info().DebugString();
  }

  // store
  {
    dingodb::pb::store::HelloRequest request;
    dingodb::pb::store::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = store_interaction->SendRequest("StoreService", "Hello", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    DINGO_LOG(INFO) << "local version info : " << response.version_info().DebugString();
  }

  // document
  {
    dingodb::pb::document::HelloRequest request;
    dingodb::pb::document::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = document_interaction->SendRequest("DocumentService", "Hello", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    DINGO_LOG(INFO) << "local version info : " << response.version_info().DebugString();
  }
}

TEST_F(BrInterationTest, AllSendRequest) {
  // coordinator
  {
    dingodb::pb::coordinator::RegisterBackupRequest request;
    dingodb::pb::coordinator::RegisterBackupResponse response;
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

    request.set_backup_name("RegisterBackupTaskBackName");
    request.set_backup_path("/home/dingodb/backup");
    int64_t current_now_s = dingodb::Helper::Timestamp();
    request.set_backup_current_timestamp(current_now_s);
    request.set_backup_timeout_s(60);

    butil::Status status = coor_interaction->AllSendRequest("CoordinatorService", "RegisterBackup", request, response);
    if (!status.ok()) {
      std::string s = fmt::format("Fail to set RegisterBackup, status={}", status.error_cstr());
      DINGO_LOG(ERROR) << s;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      std::string s = fmt::format("Fail to set RegisterBackup, error={}", response.error().errmsg());
      DINGO_LOG(ERROR) << s;
    }

    EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
  }

  // store
  {
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
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
    }

    DINGO_LOG(INFO) << response.DebugString();
  }

  // document
  {
    dingodb::pb::document::HelloRequest request;
    dingodb::pb::document::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = document_interaction->AllSendRequest("DocumentService", "Hello", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    DINGO_LOG(INFO) << "local version info : " << response.version_info().DebugString();
  }
}

TEST_F(BrInterationTest, CreateInteraction) {
  std::vector<std::string> addrs = {"127.0.0.1:33001", "127.0.0.1:33002", "127.0.0.1:33003"};
  std::shared_ptr<br::ServerInteraction> interaction;
  butil::Status status = br::ServerInteraction::CreateInteraction(addrs, interaction);

  // document
  {
    dingodb::pb::document::HelloRequest request;
    dingodb::pb::document::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->AllSendRequest("DocumentService", "Hello", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return;
    }

    DINGO_LOG(INFO) << "local version info : " << response.version_info().DebugString();
  }
}
