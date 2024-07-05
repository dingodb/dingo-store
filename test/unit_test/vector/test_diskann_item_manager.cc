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
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "config/yaml_config.h"
#include "diskann/diskann_item_manager.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: $INSTANCE_ID$\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  listen_host: $SERVER_LISTEN_HOST$\n"
    "  host: $SERVER_HOST$\n"
    "  port: $SERVER_PORT$\n"
    "log:\n"
    "  level: INFO\n"
    "  path: $BASE_PATH$/log\n"
    "store:\n"
    "  path: /home/server/work/dingo-store/build/bin/\n"
    "  num_threads: 64\n"
    "  search_dram_budget_gb: 1.0\n"
    "  build_dram_budget_gb: 10.0\n"
    "  import_timeout_s: 300\n";

class DiskANNItemManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));
    ASSERT_TRUE(diskann_item_manager.Init(config));
    brpc::Controller cntl;
    ctx = std::make_shared<Context>();
    ctx->SetCntl(&cntl);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;

  inline static DiskANNItemManager& diskann_item_manager = DiskANNItemManager::GetInstance();
  inline static std::shared_ptr<Config> config;
  inline static int64_t id = 1;
  inline static std::shared_ptr<Context> ctx;
};

TEST_F(DiskANNItemManagerTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  ////////////////////////////////////////////////diskann//////////////////////////////////////////////////////////////////////////
  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    auto item1 = diskann_item_manager.Create(ctx, id, index_parameter);
    EXPECT_NE(nullptr, item1.get());
    auto item2 = diskann_item_manager.Create(ctx, id, index_parameter);
    EXPECT_EQ(nullptr, item2.get());
  }

  // diskann valid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    auto item1 = diskann_item_manager.Create(ctx, id + 1, index_parameter);
    EXPECT_NE(nullptr, item1.get());
    auto item2 = diskann_item_manager.Create(ctx, id + 1, index_parameter);
    EXPECT_EQ(nullptr, item2.get());
  }

  // diskann valid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    auto item1 = diskann_item_manager.Create(ctx, id + 2, index_parameter);
    EXPECT_NE(nullptr, item1.get());
    auto item2 = diskann_item_manager.Create(ctx, id + 2, index_parameter);
    EXPECT_EQ(nullptr, item2.get());
  }
}

TEST_F(DiskANNItemManagerTest, Find) {
  std::shared_ptr<DiskANNItem> item = diskann_item_manager.Find(id);
  EXPECT_NE(nullptr, item.get());

  item = diskann_item_manager.Find(id + 23333);
  EXPECT_EQ(nullptr, item.get());
}

TEST_F(DiskANNItemManagerTest, FindAll) {
  std::vector<std::shared_ptr<DiskANNItem>> items = diskann_item_manager.FindAll();

  EXPECT_EQ(3, items.size());

  for (const auto& item : items) {
    EXPECT_NE(nullptr, item.get());
    auto data = item->Dump(ctx);
    DINGO_LOG(INFO) << "data " << data;
  }
}

TEST_F(DiskANNItemManagerTest, Delete) {
  diskann_item_manager.Delete(id);
  diskann_item_manager.Delete(id + 23333);
}

TEST_F(DiskANNItemManagerTest, Import) {
  butil::Status status;
  DiskANNCoreState state;
  std::vector<pb::common::Vector> vectors;
  std::vector<int64_t> vector_ids;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
    }
  }

  {
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
      vector_ids.push_back(i);
    }
  }

  // once
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    std::shared_ptr<DiskANNItem> item = diskann_item_manager.Find(id + 1);

    status = item->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist, already_send_vector_count, 0,
                          0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, data_base_size);
  }
}

}  // namespace dingodb
