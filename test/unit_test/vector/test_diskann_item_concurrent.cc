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
#include "diskann/diskann_item.h"
#include "diskann/diskann_utils.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

DEFINE_int64(diskann_item_concurrent_num, 16, "diskann Test diskann object concurrent number. default 16");

namespace dingodb {

class DiskANNItemConcurrentTest;
static void DoCreate(DiskANNItemConcurrentTest &diskann_item_test);

class DiskANNItemConcurrentTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    DiskANNItem::SetBaseDir(base_dir);
    ctx = std::make_shared<Context>();
    brpc::Controller cntl;
    ctx->SetCntl(&cntl);

    num_items = FLAGS_diskann_item_concurrent_num;

    DINGO_LOG(INFO) << "num_items :  " << num_items;
    DINGO_LOG(INFO) << "num_threads :  " << num_threads;
    DINGO_LOG(INFO) << "total aio nums  :  " << num_items * num_threads * 1024;
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  friend void DoCreate(DiskANNItemConcurrentTest &diskann_item_test);

  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;

  inline static std::shared_ptr<DiskANNItem> disk_concurrent_item_l2;
  inline static std::shared_ptr<DiskANNItem> disk_concurrent_item_ip;
  inline static std::shared_ptr<DiskANNItem> disk_concurrent_item_cosine;
  inline static int64_t id = 1;
  inline static uint32_t num_threads = 64;
  inline static float search_dram_budget_gb = 2.0f;
  inline static float build_dram_budget_gb = 4.0f;
  inline static std::shared_ptr<Context> ctx;
  inline static std::string base_dir = ".";
  inline static std::vector<std::shared_ptr<DiskANNItem>> disk_concurrent_items;
  inline static int64_t num_items = 16;
};

TEST_F(DiskANNItemConcurrentTest, ClearAll) {
  DiskANNUtils::CreateDir(DiskANNItemConcurrentTest::base_dir);
  DiskANNUtils::RemoveFile(DiskANNItemConcurrentTest::base_dir + "/" + "data.bin");
  DiskANNUtils::RemoveDir(DiskANNItemConcurrentTest::base_dir + "/" + "tmp");
  DiskANNUtils::RemoveDir(DiskANNItemConcurrentTest::base_dir + "/" + "destroyed");
  DiskANNUtils::RemoveDir(DiskANNItemConcurrentTest::base_dir + "/" + "normal");
  DiskANNUtils::RemoveDir(DiskANNItemConcurrentTest::base_dir + "/" + "blackhole");
}

static void DoCreate(DiskANNItemConcurrentTest &diskann_item_test) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  for (int64_t i = 0; i < diskann_item_test.num_items; i++) {
    // diskann valid param L2
    if (0 == (i % 3)) {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      diskann_item_test.disk_concurrent_item_l2 = std::make_shared<DiskANNItem>(
          diskann_item_test.ctx, diskann_item_test.id + i, index_parameter, diskann_item_test.num_threads,
          diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

      EXPECT_NE(diskann_item_test.disk_concurrent_item_l2.get(), nullptr);
      diskann_item_test.disk_concurrent_items.push_back(diskann_item_test.disk_concurrent_item_l2);
    }

    // diskann valid param IP
    if (1 == (i % 3)) {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      diskann_item_test.disk_concurrent_item_ip = std::make_shared<DiskANNItem>(
          diskann_item_test.ctx, diskann_item_test.id + i, index_parameter, diskann_item_test.num_threads,
          diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

      EXPECT_NE(diskann_item_test.disk_concurrent_item_ip.get(), nullptr);
      diskann_item_test.disk_concurrent_items.push_back(diskann_item_test.disk_concurrent_item_ip);
    }

    // diskann valid param cosine
    if (2 == (i % 3)) {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      diskann_item_test.disk_concurrent_item_cosine = std::make_shared<DiskANNItem>(
          diskann_item_test.ctx, diskann_item_test.id + i, index_parameter, diskann_item_test.num_threads,
          diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

      EXPECT_NE(diskann_item_test.disk_concurrent_item_cosine.get(), nullptr);
      diskann_item_test.disk_concurrent_items.push_back(diskann_item_test.disk_concurrent_item_cosine);
    }
  }
}

TEST_F(DiskANNItemConcurrentTest, Create) { DoCreate(*this); }

TEST_F(DiskANNItemConcurrentTest, Prepare) {
  std::string data_path = "./data.bin";
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
    int nb = data_base_size;
    int d = dimension;
    float *xb = data_base.data();
    std::cout << "generate data complete!!!" << std::endl;

    std::ofstream writer(data_path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!writer.is_open()) {
      DINGO_LOG(FATAL) << "file open fail : " << data_path;
    }

    writer.write(reinterpret_cast<char *>(&nb), sizeof(nb));
    writer.write(reinterpret_cast<char *>(&d), sizeof(d));

    int i = 0;
    int total = d * nb * sizeof(float);
    int left = total;
    int size = 1024;
    while (true) {
      size = std::min(size, left);
      writer.write(reinterpret_cast<char *>(xb) + i, size);
      left -= size;
      i += size;
      if (left <= 0) break;
    }
    writer.close();
  }
}

TEST_F(DiskANNItemConcurrentTest, Import) {
  std::vector<pb::common::Vector> vectors;
  std::vector<int64_t> vector_ids;
  int ts = 1;
  int tso = 10;

  // prepare data
  {
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
      vector_ids.push_back(i + 1);
    }
  }

  // multi thread import
  {
    std::vector<std::thread> threads;
    threads.reserve(num_items);
    for (int i = 0; i < num_items; i++) {
      threads.emplace_back(std::thread([&vectors, &vector_ids, &ts, &tso, i]() {
        butil::Status status;
        std::shared_ptr<Context> ctx;
        ctx = std::make_shared<Context>();
        brpc::Controller cntl;
        ctx->SetCntl(&cntl);
        bool has_more = false;
        bool force_to_load_data_if_exist = false;
        int64_t already_send_vector_count = 0;
        int64_t already_recv_vector_count = 0;

        status = disk_concurrent_items[i]->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                                  already_send_vector_count, ts, tso, already_recv_vector_count);
        EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
        EXPECT_EQ(already_recv_vector_count, data_base_size);
      }));
    }

    for (auto &t : threads) {
      t.join();
    }
  }
}

TEST_F(DiskANNItemConcurrentTest, Build) {
  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int i = 0; i < num_items; i++) {
    threads.emplace_back(std::thread([i]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      status = disk_concurrent_items[i]->Build(ctx, false, true);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

      state = disk_concurrent_items[i]->Status(ctx);
      EXPECT_EQ(state, DiskANNCoreState::kUpdatedPath);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(DiskANNItemConcurrentTest, Load) {
  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int i = 0; i < num_items; i++) {
    threads.emplace_back(std::thread([i]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      pb::common::LoadDiskAnnParam load_param;
      load_param.set_num_nodes_to_cache(2);
      load_param.set_warmup(true);
      status = disk_concurrent_items[i]->Load(ctx, load_param, true);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

      state = disk_concurrent_items[i]->Status(ctx);
      EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(DiskANNItemConcurrentTest, Search) {
  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int k = 0; k < num_items; k++) {
    threads.emplace_back(std::thread([k]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      int64_t ts;
      std::vector<pb::common::Vector> vectors;
      for (int i = 0; i < data_base_size; i++) {
        pb::common::Vector vector;
        vector.set_dimension(dimension);
        vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < dimension; j++) {
          vector.add_float_values(data_base[i * dimension + j]);
        }

        vectors.push_back(vector);
      }
      uint32_t top_n = 3;
      pb::common::SearchDiskAnnParam search_param;
      std::vector<pb::index::VectorWithDistanceResult> results;
      status = disk_concurrent_items[k]->Search(ctx, top_n, search_param, vectors, results, ts);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(DiskANNItemConcurrentTest, Close) {
  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int i = 0; i < num_items; i++) {
    threads.emplace_back(std::thread([i]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      status = disk_concurrent_items[i]->Close(ctx);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(DiskANNItemConcurrentTest, TryLoad) {
  butil::Status status;
  DiskANNCoreState state;

  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int i = 0; i < num_items; i++) {
    threads.emplace_back(std::thread([i]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      pb::common::LoadDiskAnnParam load_param;
      load_param.set_num_nodes_to_cache(2);
      load_param.set_warmup(true);
      status = disk_concurrent_items[i]->TryLoad(ctx, load_param, true);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      state = disk_concurrent_items[i]->Status(ctx);
      EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(DiskANNItemConcurrentTest, Destroy) {
  std::vector<std::thread> threads;
  threads.reserve(num_items);
  for (int i = 0; i < num_items; i++) {
    threads.emplace_back(std::thread([i]() {
      butil::Status status;
      DiskANNCoreState state;
      std::shared_ptr<Context> ctx;
      ctx = std::make_shared<Context>();
      brpc::Controller cntl;
      ctx->SetCntl(&cntl);
      status = disk_concurrent_items[i]->Destroy(ctx);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
}

}  // namespace dingodb
