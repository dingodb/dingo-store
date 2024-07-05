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

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "diskann/diskann_core.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

// note : inline static faiss::idx_t dimension = 128;    inline static int data_base_size = 10;  inline static uint32_t
// num_threads = 1;
class DiskANNCoreStressTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  inline static faiss::idx_t dimension = 1024;
  // inline static faiss::idx_t dimension = 128;
  inline static int data_base_size = 100000;
  // inline static int data_base_size = 10;
  inline static std::vector<float> data_base;

  static ThreadPoolPtr vector_index_thread_pool;

  inline static std::shared_ptr<DiskANNCore> disk_ann_core_l2;
  inline static std::shared_ptr<DiskANNCore> disk_ann_core_ip;
  inline static std::shared_ptr<DiskANNCore> disk_ann_core_cosine;
  inline static std::string data_path = "./data.bin";
  inline static std::string index_path_prefix = "./tmp_diskann_index_";
  inline static int64_t id = 1;
  inline static uint32_t num_threads = 64;
  // inline static uint32_t num_threads = 1;
  inline static float search_dram_budget_gb = 50.0f;
  inline static float build_dram_budget_gb = 0.3f;
  // inline static float build_dram_budget_gb = 100.0f;
  //  inline static float build_dram_budget_gb = 0.000001f;
};

TEST_F(DiskANNCoreStressTest, Create) {
  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    disk_ann_core_l2 = std::make_shared<DiskANNCore>(id, index_parameter, num_threads, search_dram_budget_gb,
                                                     build_dram_budget_gb, data_path, index_path_prefix + "l2");

    EXPECT_NE(disk_ann_core_l2.get(), nullptr);
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

    disk_ann_core_ip = std::make_shared<DiskANNCore>(id, index_parameter, num_threads, search_dram_budget_gb,
                                                     build_dram_budget_gb, data_path, index_path_prefix + "ip");

    EXPECT_NE(disk_ann_core_l2.get(), nullptr);
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

    disk_ann_core_cosine = std::make_shared<DiskANNCore>(id, index_parameter, num_threads, search_dram_budget_gb,
                                                         build_dram_budget_gb, data_path, index_path_prefix + "cosine");

    EXPECT_NE(disk_ann_core_l2.get(), nullptr);
  }
}

TEST_F(DiskANNCoreStressTest, Prepare) {
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
    float* xb = data_base.data();
    std::cout << "generate data complete!!!" << std::endl;

    std::ofstream writer(data_path, std::ios::out | std::ios::binary);
    if (!writer.is_open()) {
      DINGO_LOG(FATAL) << "file open fail : " << data_path;
    }

    writer.write(reinterpret_cast<char*>(&nb), sizeof(nb));
    writer.write(reinterpret_cast<char*>(&d), sizeof(d));

    int i = 0;
    int total = d * nb;
    int left = total;
    int size = 1024;
    while (true) {
      size = std::min(size, left);
      writer.write(reinterpret_cast<char*>(xb + i), sizeof(float) * size);
      left -= size;
      if (left <= 0) break;
    }
    writer.close();
  }
}

TEST_F(DiskANNCoreStressTest, Build) {
  butil::Status status;
  DiskANNCoreState state;

  // ok
  {
    std::filesystem::create_directory(index_path_prefix + "l2");
    std::filesystem::create_directory(index_path_prefix + "ip");
    std::filesystem::create_directory(index_path_prefix + "cosine");

    status = disk_ann_core_l2->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_ip->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_cosine->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
  }
}

TEST_F(DiskANNCoreStressTest, UpdateIndexPathPrefix) {
  butil::Status status;
  DiskANNCoreState state;

  // ok
  {
    std::string new_index_path_prefix = "./diskann_index_";
    DiskANNUtils::Rename(index_path_prefix + "l2", new_index_path_prefix + "l2");
    DiskANNUtils::Rename(index_path_prefix + "ip", new_index_path_prefix + "ip");
    DiskANNUtils::Rename(index_path_prefix + "cosine", new_index_path_prefix + "cosine");

    status = disk_ann_core_l2->UpdateIndexPathPrefix(new_index_path_prefix + "l2", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatedPath);
    status = disk_ann_core_ip->UpdateIndexPathPrefix(new_index_path_prefix + "ip", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatedPath);
    status = disk_ann_core_cosine->UpdateIndexPathPrefix(new_index_path_prefix + "cosine", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatedPath);
  }
}

TEST_F(DiskANNCoreStressTest, Load) {
  butil::Status status;
  DiskANNCoreState state;

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_core_l2->Load(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_ip->Load(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_cosine->Load(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }
}

TEST_F(DiskANNCoreStressTest, Search) {
  butil::Status ok;
  DiskANNCoreState state;

  // ok
  {
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
    ok = disk_ann_core_l2->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_core_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_core_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_core_ip->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_core_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_core_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_core_cosine->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNCoreStressTest, Reset) {
  butil::Status ok;
  DiskANNCoreState state;

  {
    bool is_delete_files = true;
    bool is_force = false;
    ok = disk_ann_core_l2->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_ip->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_cosine->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

}  // namespace dingodb
