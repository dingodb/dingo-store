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
#include "br/restore_region_meta.h"
#include "br/sst_file_reader.h"
#include "fmt/core.h"
#include "glog/logging.h"

class BrRestoreRegionMetaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::vector<std::string> coor_addrs = {"127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"};

    coordinator_interaction = std::make_shared<br::ServerInteraction>();

    EXPECT_TRUE(coordinator_interaction->Init(coor_addrs));

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coordinator_interaction);

    const std::string file_name = "store_region_sql_meta.sst";
    const std::string file_path = "/home/server/work/dingo-store/build/bin/backup2/" + file_name;

    std::shared_ptr<br::SstFileReader> reader_sst = std::make_shared<br::SstFileReader>();

    std::map<std::string, std::string> kvs;
    auto status = reader_sst->ReadFile(file_path, kvs);

    EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);

    region = std::make_shared<dingodb::pb::common::Region>();
    auto ret = region->ParseFromString(kvs.begin()->second);
    EXPECT_TRUE(ret);

    LOG(INFO) << region->DebugString();

    backup_meta_region_name = file_name;

    restore_region_meta = std::make_shared<br::RestoreRegionMeta>(coordinator_interaction, region, replica_num,
                                                                  backup_meta_region_name, create_region_timeout_s);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::shared_ptr<br::RestoreRegionMeta> restore_region_meta;
  inline static br::ServerInteractionPtr coordinator_interaction;
  inline static std::shared_ptr<dingodb::pb::common::Region> region;
  inline static int64_t replica_num = 0;
  inline static std::string backup_meta_region_name;
  inline static int64_t create_region_timeout_s = 60;
};

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

  // dingodb::pb::common::Region region2;
  // dingodb::pb::common::Region region3 = region2;


}

TEST_F(BrRestoreRegionMetaTest, Init) {
  butil::Status status = restore_region_meta->Init();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreRegionMetaTest, Run) {
  butil::Status status = restore_region_meta->Run();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreRegionMetaTest, Finish) {
  butil::Status status = restore_region_meta->Finish();
  EXPECT_EQ(status.error_code(), dingodb::pb::error::OK);
}

TEST_F(BrRestoreRegionMetaTest, Get) {
  butil::Status status;

  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_region_id(80001);

  status = coordinator_interaction->SendRequest("CoordinatorService", "QueryRegion", request, response);
  if (!status.ok()) {
    LOG(INFO) << status.error_cstr();
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    LOG(INFO) << response.error().errmsg();
  }

  LOG(INFO) << response.DebugString();

  if (response.region().definition().has_index_parameter()) {
    LOG(INFO) << "has_index_parameter ***************meta store region id : " << response.region().id()
              << ",  name : " << response.region().definition().name()
              << ", index_parameter : " << response.region().definition().index_parameter().DebugString();
  } else {
    LOG(INFO) << "no_index_parameter ***************meta store region id : " << response.region().id()
              << ",  name : " << response.region().definition().name()
              << ", index_parameter : " << response.region().definition().index_parameter().DebugString();
  }
}
