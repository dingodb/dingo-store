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

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/restore_region_meta.h"
#include "br/sst_file_reader.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreRegionMetaTest;

static bool GetRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test,
                      std::shared_ptr<dingodb::pb::common::Region> region);

static void TestCreateRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test, const std::string& file_name);
class BrRestoreRegionMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};

    coordinator_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coordinator_interaction->Init(coor_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coordinator_interaction);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  friend bool GetRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test,
                        std::shared_ptr<dingodb::pb::common::Region> region);
  friend void TestCreateRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test,
                               const std::string& file_name);

  inline static br::ServerInteractionPtr coordinator_interaction;
  inline static int64_t replica_num = 0;
  inline static std::string backup_meta_region_name;
  inline static int64_t create_region_timeout_s = 60;
  inline static std::string base_dir = "./backup2/";
  inline static std::string statics_regions;
};

static bool GetRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test,
                      std::shared_ptr<dingodb::pb::common::Region> region) {
  butil::Status status;
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_region_id(region->id());

  status = br_restore_region_meta_test.coordinator_interaction->SendRequest("CoordinatorService", "QueryRegion",
                                                                            request, response);
  if (!status.ok()) {
    LOG(INFO) << status.error_cstr();
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    LOG(INFO) << response.error().errmsg();
  }

  LOG(INFO) << response.DebugString();

  EXPECT_EQ(region->id(), response.region().id());
  EXPECT_EQ(region->region_type(), response.region().region_type());

  EXPECT_EQ(region->definition().id(), response.region().definition().id());
  EXPECT_EQ(region->definition().name(), response.region().definition().name());
  EXPECT_EQ(region->definition().range().start_key(), response.region().definition().range().start_key());
  EXPECT_EQ(region->definition().range().end_key(), response.region().definition().range().end_key());
  EXPECT_EQ(region->definition().raw_engine(), response.region().definition().raw_engine());
  EXPECT_EQ(region->definition().store_engine(), response.region().definition().store_engine());
  EXPECT_EQ(region->definition().schema_id(), response.region().definition().schema_id());
  EXPECT_EQ(region->definition().table_id(), response.region().definition().table_id());
  EXPECT_EQ(region->definition().index_id(), response.region().definition().index_id());
  EXPECT_EQ(region->definition().part_id(), response.region().definition().part_id());
  EXPECT_EQ(region->definition().tenant_id(), response.region().definition().tenant_id());
  if (region->definition().has_index_parameter()) {
    EXPECT_EQ(region->definition().index_parameter().index_type(),
              response.region().definition().index_parameter().index_type());

    if (region->definition().index_parameter().has_vector_index_parameter()) {
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
          region->definition().index_parameter().vector_index_parameter(),
          response.region().definition().index_parameter().vector_index_parameter()));
    }

    if (region->definition().index_parameter().has_scalar_index_parameter()) {
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
          region->definition().index_parameter().scalar_index_parameter(),
          response.region().definition().index_parameter().scalar_index_parameter()));
    }

    if (region->definition().index_parameter().has_document_index_parameter()) {
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
          region->definition().index_parameter().document_index_parameter(),
          response.region().definition().index_parameter().document_index_parameter()));
    }

    EXPECT_EQ(region->definition().index_parameter().origin_keys_size(),
              response.region().definition().index_parameter().origin_keys_size());

    for (int i = 0; i < region->definition().index_parameter().origin_keys_size(); i++) {
      EXPECT_EQ(region->definition().index_parameter().origin_keys(i),
                response.region().definition().index_parameter().origin_keys(i));
    }

    EXPECT_EQ(region->definition().index_parameter().origin_with_keys_size(),
              response.region().definition().index_parameter().origin_with_keys_size());

    for (int i = 0; i < region->definition().index_parameter().origin_with_keys_size(); i++) {
      EXPECT_EQ(region->definition().index_parameter().origin_with_keys(i),
                response.region().definition().index_parameter().origin_with_keys(i));
    }
  }

  return true;
}

static void TestCreateRegion(const BrRestoreRegionMetaTest& br_restore_region_meta_test, const std::string& file_name) {
  const std::string file_path = br_restore_region_meta_test.base_dir + file_name;
  br_restore_region_meta_test.backup_meta_region_name = file_name;

  std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

  std::map<std::string, std::string> kvs;
  auto status = reader_sst->ReadFile(file_path, kvs);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

  br_restore_region_meta_test.statics_regions += "\n" + file_name + " : [";

  int i = 0;

  for (const auto& [region_id, region_ptr] : kvs) {
    auto region = std::make_shared<dingodb::pb::common::Region>();
    auto ret = region->ParseFromString(region_ptr);
    EXPECT_TRUE(ret);

    auto restore_region_meta = std::make_shared<br::RestoreRegionMeta>(
        br_restore_region_meta_test.coordinator_interaction, region, br_restore_region_meta_test.replica_num,
        br_restore_region_meta_test.backup_meta_region_name, br_restore_region_meta_test.create_region_timeout_s);

    status = restore_region_meta->Init();
    EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
    status = restore_region_meta->Run();
    EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
    status = restore_region_meta->Finish();
    EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

    bool is_true = GetRegion(br_restore_region_meta_test, region);
    EXPECT_TRUE(is_true);
    if (0 == i) {
      br_restore_region_meta_test.statics_regions += fmt::format("{}", region->id());
    } else {
      if (i % 16 == 0) {
        br_restore_region_meta_test.statics_regions += fmt::format("\n{}", region->id());
      } else {
        br_restore_region_meta_test.statics_regions += fmt::format(" {}", region->id());
      }
    }
    i++;
  }
  br_restore_region_meta_test.statics_regions += "]";
}

TEST_F(BrRestoreRegionMetaTest, TestDocumentRegionSdkData) {
  const std::string& file_name = "document_region_sdk_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestDocumentRegionSqlData) {
  const std::string& file_name = "document_region_sql_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestIndexRegionSdkData) {
  const std::string& file_name = "index_region_sdk_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestIndexRegionSqlData) {
  const std::string& file_name = "index_region_sql_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestStoreRegionSdkData) {
  const std::string& file_name = "store_region_sdk_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestStoreRegionSqlData) {
  const std::string& file_name = "store_region_sql_data.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, TestStoreRegionSqlMeta) {
  const std::string& file_name = "store_region_sql_meta.sst";
  TestCreateRegion(*this, file_name);
}

TEST_F(BrRestoreRegionMetaTest, StaticsCreateRegion) { LOG(INFO) << statics_regions; }

TEST_F(BrRestoreRegionMetaTest, TestHasValue) {
  dingodb::pb::common::Region region;
  region.set_id(10);
  bool has_definition = region.has_definition();
  EXPECT_FALSE(has_definition);
  bool has_index_parameter = region.definition().has_index_parameter();
  EXPECT_FALSE(has_index_parameter);

  dingodb::pb::common::IndexParameter index_parameter;
  // region.mutable_definition()->mutable_index_parameter()->CopyFrom(index_parameter);
  *(region.mutable_definition()->mutable_index_parameter()) = index_parameter;

  has_definition = region.has_definition();
  EXPECT_TRUE(has_definition);

  has_index_parameter = region.definition().has_index_parameter();
  EXPECT_TRUE(has_index_parameter);
}