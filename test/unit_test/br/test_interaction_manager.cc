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

#include <string>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"

class BrInterationManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BrInterationManagerTest, SetCoordinatorInteraction) {
  std::string coor_addrs = "127.0.0.1:32001, 127.0.0.1:32002, 127.0.0.1:32003";

  std::shared_ptr<br::ServerInteraction> interaction = std::make_shared<br::ServerInteraction>();
  bool is_true = interaction->Init(coor_addrs);
  EXPECT_TRUE(is_true);

  br::InteractionManager::GetInstance().SetCoordinatorInteraction(interaction);
}

TEST_F(BrInterationManagerTest, SetStoreInteraction) {
  std::string store_addrs = "127.0.0.1:30001, 127.0.0.1:30002, 127.0.0.1:30003";

  std::shared_ptr<br::ServerInteraction> interaction = std::make_shared<br::ServerInteraction>();
  bool is_true = interaction->Init(store_addrs);
  EXPECT_TRUE(is_true);

  br::InteractionManager::GetInstance().SetStoreInteraction(interaction);
}

TEST_F(BrInterationManagerTest, SetIndexInteraction) {
  std::string index_addrs = "127.0.0.1:31001, 127.0.0.1:31002, 127.0.0.1:31003";

  std::shared_ptr<br::ServerInteraction> interaction = std::make_shared<br::ServerInteraction>();
  bool is_true = interaction->Init(index_addrs);
  EXPECT_TRUE(is_true);

  br::InteractionManager::GetInstance().SetIndexInteraction(interaction);
}

TEST_F(BrInterationManagerTest, SetDocumentInteraction) {
  std::string document_addrs = "127.0.0.1:33001, 127.0.0.1:33002, 127.0.0.1:33003";

  std::shared_ptr<br::ServerInteraction> interaction = std::make_shared<br::ServerInteraction>();
  bool is_true = interaction->Init(document_addrs);
  EXPECT_TRUE(is_true);

  br::InteractionManager::GetInstance().SetDocumentInteraction(interaction);
}

TEST_F(BrInterationManagerTest, GetCoordinatorInteraction) {
  std::shared_ptr<br::ServerInteraction> interaction =
      br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  EXPECT_NE(interaction, nullptr);
  // coordinator
  {
    dingodb::pb::coordinator::HelloRequest request;
    dingodb::pb::coordinator::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("CoordinatorService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, GetStoreInteraction) {
  std::shared_ptr<br::ServerInteraction> interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  EXPECT_NE(interaction, nullptr);
  // store
  {
    dingodb::pb::store::HelloRequest request;
    dingodb::pb::store::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("StoreService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, GetIndexInteraction) {
  std::shared_ptr<br::ServerInteraction> interaction = br::InteractionManager::GetInstance().GetIndexInteraction();
  EXPECT_NE(interaction, nullptr);
  // index
  {
    dingodb::pb::index::HelloRequest request;
    dingodb::pb::index::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("IndexService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, GetDocumentInteraction) {
  std::shared_ptr<br::ServerInteraction> interaction = br::InteractionManager::GetInstance().GetDocumentInteraction();
  EXPECT_NE(interaction, nullptr);
  // document
  {
    dingodb::pb::document::HelloRequest request;
    dingodb::pb::document::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("DocumentService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, CloneCoordinatorInteraction) {
  auto [status, interaction] = br::InteractionManager::GetInstance().CloneCoordinatorInteraction();
  EXPECT_TRUE(status.ok());
  EXPECT_NE(interaction, nullptr);
  // coordinator
  {
    dingodb::pb::coordinator::HelloRequest request;
    dingodb::pb::coordinator::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("CoordinatorService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, CloneStoreInteraction) {
  auto [status, interaction] = br::InteractionManager::GetInstance().CloneStoreInteraction();
  EXPECT_TRUE(status.ok());
  EXPECT_NE(interaction, nullptr);
  // store
  {
    dingodb::pb::store::HelloRequest request;
    dingodb::pb::store::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("StoreService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, CloneIndexInteraction) {
  auto [status, interaction] = br::InteractionManager::GetInstance().CloneIndexInteraction();
  EXPECT_TRUE(status.ok());
  EXPECT_NE(interaction, nullptr);

  // index
  {
    dingodb::pb::index::HelloRequest request;
    dingodb::pb::index::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("IndexService", "Hello", request, response);
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

TEST_F(BrInterationManagerTest, CloneDocumentInteraction) {
  auto [status, interaction] = br::InteractionManager::GetInstance().CloneDocumentInteraction();
  EXPECT_TRUE(status.ok());
  EXPECT_NE(interaction, nullptr);

  // document
  {
    dingodb::pb::document::HelloRequest request;
    dingodb::pb::document::HelloResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.set_is_just_version_info(true);

    butil::Status status = interaction->SendRequest("DocumentService", "Hello", request, response);
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