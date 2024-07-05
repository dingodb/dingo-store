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
#include "common/logging.h"
#include "diskann/diskann_core.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class DiskANNCoreTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {
    vector_index_flat_l2.reset();
    vector_index_flat_ip.reset();
    vector_index_flat_cosine.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_cosine;
  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;

  static ThreadPoolPtr vector_index_thread_pool;

  inline static std::shared_ptr<DiskANNCore> disk_ann_core_l2;
  inline static std::shared_ptr<DiskANNCore> disk_ann_core_ip;
  inline static std::shared_ptr<DiskANNCore> disk_ann_core_cosine;
  inline static std::string data_path = "./data.bin";
  inline static std::string index_path_prefix = "./tmp_diskann_index_";
  inline static int64_t id = 1;
  inline static uint32_t num_threads = 64;
  inline static float search_dram_budget_gb = 2.0f;
  inline static float build_dram_budget_gb = 4.0f;
  // inline static float build_dram_budget_gb = 0.000001f;
};

TEST_F(DiskANNCoreTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  // valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_flat_l2.get(), nullptr);
  }

  // valid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_flat_ip = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_flat_ip.get(), nullptr);
  }

  // valid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    vector_index_flat_cosine = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_flat_cosine.get(), nullptr);
  }

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

TEST_F(DiskANNCoreTest, BuildNotDataPath) {
  butil::Status status;
  DiskANNCoreState state;

  // invalid param failed, data_path not exist, return NOT_FOUND
  {
    DiskANNUtils::RemoveFile(data_path);
    status = disk_ann_core_l2->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
    status = disk_ann_core_ip->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
    status = disk_ann_core_cosine->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
  }
}

TEST_F(DiskANNCoreTest, Prepare) {
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

TEST_F(DiskANNCoreTest, Build) {
  butil::Status status;
  DiskANNCoreState state;

  // invalid param failed, data_path not exist, return NOT_FOUND
  {
    DiskANNUtils::RemoveDir(index_path_prefix + "l2");
    DiskANNUtils::RemoveDir(index_path_prefix + "ip");
    DiskANNUtils::RemoveDir(index_path_prefix + "cosine");
    status = disk_ann_core_l2->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
    status = disk_ann_core_ip->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
    status = disk_ann_core_cosine->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
    EXPECT_EQ(state, DiskANNCoreState::kBuilding);
  }

  // many threads, build success
  {
    disk_ann_core_l2->Reset(false, state, true);
    disk_ann_core_ip->Reset(false, state, true);
    disk_ann_core_cosine->Reset(false, state, true);

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
      disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "l2");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "ip");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb,
                                 data_path, index_path_prefix + "cosine");
    }

    DiskANNUtils::RemoveDir(index_path_prefix + "l2");
    DiskANNUtils::RemoveDir(index_path_prefix + "ip");
    DiskANNUtils::RemoveDir(index_path_prefix + "cosine");

    std::filesystem::create_directory(index_path_prefix + "l2");
    std::filesystem::create_directory(index_path_prefix + "ip");
    std::filesystem::create_directory(index_path_prefix + "cosine");

    int n = 10;

    std::vector<std::pair<butil::Status, DiskANNCoreState>> threads_vars;
    threads_vars.resize(n);
    std::vector<std::thread> threads;
    threads.reserve(n);
    for (int i = 0; i < n; i++) {
      threads.emplace_back([this, &threads_vars, i]() {
        butil::Status status;
        DiskANNCoreState state;  // NOLINT
        status = disk_ann_core_l2->Build(false, state);
        threads_vars[i].first = status;
        threads_vars[i].second = state;
      });
    };

    for (auto& t : threads) {
      t.join();
    }

    bool is_build_ok = false;
    for (auto& var : threads_vars) {
      if (!is_build_ok) {
        is_build_ok = (var.first.error_code() == pb::error::Errno::OK && var.second == DiskANNCoreState::kBuilded);
        if (is_build_ok) {
          EXPECT_EQ(var.first.error_code(), pb::error::Errno::OK);
          EXPECT_EQ(var.second, DiskANNCoreState::kBuilded);
        } else {
          EXPECT_EQ(var.first.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
          EXPECT_EQ(var.second, DiskANNCoreState::kBuilding);
        }
      } else {
        EXPECT_EQ(var.first.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
        EXPECT_EQ(var.second, DiskANNCoreState::kBuilding);
      }
    }

    DiskANNUtils::RemoveDir(index_path_prefix + "l2");
    DiskANNUtils::RemoveDir(index_path_prefix + "ip");
    DiskANNUtils::RemoveDir(index_path_prefix + "cosine");
  }

  // ok
  {
    disk_ann_core_l2->Reset(false, state, true);
    disk_ann_core_ip->Reset(false, state, true);
    disk_ann_core_cosine->Reset(false, state, true);
    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
      disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "l2");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "ip");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb,
                                 data_path, index_path_prefix + "cosine");
    }
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

    // already built, return OK
    status = disk_ann_core_l2->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_ip->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_cosine->Build(false, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);

    // rebuild index. force rebuild
    status = disk_ann_core_l2->Build(true, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_ip->Build(true, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
    status = disk_ann_core_cosine->Build(true, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kBuilded);
  }
}

TEST_F(DiskANNCoreTest, UpdateIndexPathPrefix) {
  butil::Status status;
  DiskANNCoreState state;

  // invalid param failed, index_path_prefix not exist, return NOT_FOUND
  {
    status = disk_ann_core_l2->UpdateIndexPathPrefix("", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
    status = disk_ann_core_ip->UpdateIndexPathPrefix("", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
    status = disk_ann_core_cosine->UpdateIndexPathPrefix("", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
  }

  // index_path_prefix not exist
  {
    disk_ann_core_l2->Reset(false, state, true);
    disk_ann_core_ip->Reset(false, state, true);
    disk_ann_core_cosine->Reset(false, state, true);
    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
      disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "l2");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "ip");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb,
                                 data_path, index_path_prefix + "cosine");
    }
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

    status = disk_ann_core_l2->UpdateIndexPathPrefix("./not_exist_path/", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
    status = disk_ann_core_ip->UpdateIndexPathPrefix("./not_exist_path/", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
    status = disk_ann_core_cosine->UpdateIndexPathPrefix("./not_exist_path/", state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kUpdatingPath);
  }

  // ok
  {
    disk_ann_core_l2->Reset(false, state, true);
    disk_ann_core_ip->Reset(false, state, true);
    disk_ann_core_cosine->Reset(false, state, true);
    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
      disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "l2");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                             index_path_prefix + "ip");
    }

    {
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
      index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
      index_parameter.mutable_diskann_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
      index_parameter.mutable_diskann_parameter()->set_max_degree(64);
      index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

      disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb,
                                 data_path, index_path_prefix + "cosine");
    }
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

TEST_F(DiskANNCoreTest, Load) {
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

TEST_F(DiskANNCoreTest, Search) {
  butil::Status ok;
  DiskANNCoreState state;

  // vectors empty topk == 0, return OK
  {
    uint32_t top_n = 0;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::common::Vector> vectors;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = disk_ann_core_l2->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_ip->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_cosine->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // top_n = 0, return OK
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
    uint32_t top_n = 0;
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

  // ok top_n = 20
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
    uint32_t top_n = 20;
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

TEST_F(DiskANNCoreTest, Add) {
  butil::Status ok;
  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id + data_base_size);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNCoreTest, SearchCompare) {
  butil::Status ok;
  DiskANNCoreState state;
  uint32_t top_n = 3;

  std::vector<pb::index::VectorWithDistanceResult> disk_ann_core_l2_results;
  std::vector<pb::index::VectorWithDistanceResult> disk_ann_core_ip_results;
  std::vector<pb::index::VectorWithDistanceResult> disk_ann_core_cosine_results;

  std::vector<pb::index::VectorWithDistanceResult> vector_index_flat_l2_results;
  std::vector<pb::index::VectorWithDistanceResult> vector_index_flat_ip_results;
  std::vector<pb::index::VectorWithDistanceResult> vector_index_flat_cosine_results;

  // diskann
  {
    std::vector<pb::common::Vector> vectors;
    pb::common::Vector vector;
    vector.set_dimension(dimension);
    vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector.add_float_values(data_base[0 * dimension + j]);
    }

    vectors.push_back(vector);

    pb::common::SearchDiskAnnParam search_param;

    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = disk_ann_core_l2->Search(top_n, search_param, vectors, nullptr, disk_ann_core_l2_results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    // DINGO_LOG(INFO) << "disk_ann_core_l2 results size: " << results.size();
    // for (size_t i = 0; i < results.size(); i++) {
    //   DINGO_LOG(INFO) << "disk_ann_core_l2 result: " << i << " " << results[i].DebugString();
    // }
    // results.clear();

    ok = disk_ann_core_ip->Search(top_n, search_param, vectors, nullptr, disk_ann_core_ip_results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    // DINGO_LOG(INFO) << "disk_ann_core_ip results size: " << results.size();
    // for (size_t i = 0; i < results.size(); i++) {
    //   DINGO_LOG(INFO) << "disk_ann_core_ip result: " << i << " " << results[i].DebugString();
    // }
    // results.clear();

    ok = disk_ann_core_cosine->Search(top_n, search_param, vectors, nullptr, disk_ann_core_cosine_results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    // results.clear();
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }

    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_flat_l2->Search(vector_with_ids, top_n, {}, false, {}, vector_index_flat_l2_results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Search(vector_with_ids, top_n, {}, false, {}, vector_index_flat_ip_results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Search(vector_with_ids, top_n, {}, false, {}, vector_index_flat_cosine_results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // compare
  {
    DINGO_LOG(INFO) << "********************************************************************";
    DINGO_LOG(INFO) << "disk_ann_core_l2 results size: " << disk_ann_core_l2_results.size();
    for (size_t i = 0; i < disk_ann_core_l2_results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_core_l2 result: " << i << " " << disk_ann_core_l2_results[i].DebugString();
    }
    DINGO_LOG(INFO) << "vector_index_flat_l2 results size: " << vector_index_flat_l2_results.size();
    for (size_t i = 0; i < vector_index_flat_l2_results.size(); i++) {
      DINGO_LOG(INFO) << "vector_index_flat_l2 result: " << i << " " << vector_index_flat_l2_results[i].DebugString();
    }
    DINGO_LOG(INFO) << "********************************************************************";
    DINGO_LOG(INFO) << "disk_ann_core_ip results size: " << disk_ann_core_ip_results.size();

    for (size_t i = 0; i < disk_ann_core_ip_results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_core_ip result: " << i << " " << disk_ann_core_ip_results[i].DebugString();
    }
    DINGO_LOG(INFO) << "vector_index_flat_ip results size: " << vector_index_flat_ip_results.size();
    for (size_t i = 0; i < vector_index_flat_ip_results.size(); i++) {
      DINGO_LOG(INFO) << "vector_index_flat_ip result: " << i << " " << vector_index_flat_ip_results[i].DebugString();
    }

    DINGO_LOG(INFO) << "********************************************************************";
    DINGO_LOG(INFO) << "disk_ann_core_cosine results size: " << disk_ann_core_cosine_results.size();
    for (size_t i = 0; i < disk_ann_core_cosine_results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_core_cosine result: " << i << " " << disk_ann_core_cosine_results[i].DebugString();
    }
    DINGO_LOG(INFO) << "vector_index_flat_cosine results size: " << vector_index_flat_cosine_results.size();
    for (size_t i = 0; i < vector_index_flat_cosine_results.size(); i++) {
      DINGO_LOG(INFO) << "vector_index_flat_cosine result: " << i << " "
                      << vector_index_flat_cosine_results[i].DebugString();
    }
  }
}

TEST_F(DiskANNCoreTest, Reset) {
  butil::Status ok;
  DiskANNCoreState state;

  {
    bool is_delete_files = false;
    bool is_force = false;
    ok = disk_ann_core_l2->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_ip->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_cosine->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNCoreTest, Init) {
  butil::Status ok;

  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_l2");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_ip");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                               "./diskann_index_cosine");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNCoreTest, TryLoad) {
  butil::Status status;
  DiskANNCoreState state;

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);

    std::vector<std::thread> threads;

    for (size_t i = 0; i < 1000; i++) {
      threads.emplace_back([load_param]() {
        DiskANNCoreState state;
        disk_ann_core_l2->TryLoad(load_param, state);
        disk_ann_core_ip->TryLoad(load_param, state);
        disk_ann_core_cosine->TryLoad(load_param, state);
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    status = disk_ann_core_l2->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_ip->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_cosine->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }
}

TEST_F(DiskANNCoreTest, SearchAgain) {
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

TEST_F(DiskANNCoreTest, ResetAndInitAndTryLoad) {
  butil::Status ok, status;
  DiskANNCoreState state;

  {
    bool is_delete_files = false;
    bool is_force = false;
    ok = disk_ann_core_l2->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_ip->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_cosine->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                               "./diskann_index");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_core_l2->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
    status = disk_ann_core_ip->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
    status = disk_ann_core_cosine->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
  }

  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_l2");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_ip");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                               "./diskann_index_cosine");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(0);
    load_param.set_warmup(false);
    status = disk_ann_core_l2->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_ip->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_core_cosine->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }

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
    EXPECT_EQ(results.size(), data_base_size);
    results.clear();

    std::vector<std::thread> threads;
    for (size_t i = 0; i < 1000; i++) {
      threads.emplace_back([top_n, search_param, vectors, disk_ann_core = disk_ann_core_l2]() {
        std::vector<pb::index::VectorWithDistanceResult> results;
        DiskANNCoreState state;
        butil::Status ok = disk_ann_core->Search(top_n, search_param, vectors, nullptr, results, state);
        EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
        EXPECT_EQ(results.size(), data_base_size);
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    threads.clear();

    ok = disk_ann_core_ip->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(results.size(), data_base_size);
    results.clear();

    for (size_t i = 0; i < 1000; i++) {
      threads.emplace_back([top_n, search_param, vectors, disk_ann_core = disk_ann_core_ip]() {
        std::vector<pb::index::VectorWithDistanceResult> results;
        DiskANNCoreState state;
        butil::Status ok = disk_ann_core->Search(top_n, search_param, vectors, nullptr, results, state);
        EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
        EXPECT_EQ(results.size(), data_base_size);
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    threads.clear();

    ok = disk_ann_core_cosine->Search(top_n, search_param, vectors, nullptr, results, state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(results.size(), data_base_size);
    results.clear();

    for (size_t i = 0; i < 1000; i++) {
      threads.emplace_back([top_n, search_param, vectors, disk_ann_core = disk_ann_core_cosine]() {
        std::vector<pb::index::VectorWithDistanceResult> results;
        DiskANNCoreState state;
        butil::Status ok = disk_ann_core->Search(top_n, search_param, vectors, nullptr, results, state);
        EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
        EXPECT_EQ(results.size(), data_base_size);
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    threads.clear();
  }

  {
    bool is_delete_files = true;
    bool is_force = true;
    ok = disk_ann_core_l2->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_ip->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_core_cosine->Reset(is_delete_files, state, is_force);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNCoreTest, Misc) {
  butil::Status ok, status;
  DiskANNCoreState state;

  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    disk_ann_core_l2->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_l2");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_ip->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                           "./diskann_index_ip");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
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

    disk_ann_core_cosine->Init(id, index_parameter, num_threads, search_dram_budget_gb, build_dram_budget_gb, data_path,
                               "./diskann_index_cosine");
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_core_l2->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
    status = disk_ann_core_ip->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
    status = disk_ann_core_cosine->TryLoad(load_param, state);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    EXPECT_EQ(state, DiskANNCoreState::kLoading);
  }
}

}  // namespace dingodb
