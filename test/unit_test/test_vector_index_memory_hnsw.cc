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
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "faiss/MetricType.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class VectorIndexMemoryHnswTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4); }

  static void TearDownTestSuite() { vector_index_hnsw.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_hnsw;
  inline static faiss::idx_t dimension = 1024;
  inline static int data_base_size = 100000;
  inline static std::vector<float> data_base;
  inline static uint32_t efconstruction = 40;
  inline static uint32_t max_elements = data_base_size;
  inline static int32_t nlinks = 5;
  inline static int step_count = 1000;

  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexMemoryHnswTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexMemoryHnswTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch k_epoch;
  k_epoch.set_conf_version(1);
  k_epoch.set_version(10);

  // valid param L2
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);

    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);

    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw = VectorIndexFactory::NewHnsw(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_hnsw.get(), nullptr);
  }
}

TEST_F(VectorIndexMemoryHnswTest, Add) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) {
        data_base[dimension * i + j] = distrib(rng);
      }
      data_base[dimension * i] += i / 1000.;
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    LOG(INFO) << "create data index : span : " << diff << " s  ";

    // for (size_t i = 0; i < data_base_size_; i++) {
    //   // LOG(INFO) << "[" << i << "]"
    //   //           << " [";
    //   for (faiss::idx_t j = 0; j < dimension_; j++) {
    //     if (0 != j) {
    //       // LOG(INFO) << ",";
    //     }
    //     // LOG(INFO) << std::setw(10) << data_base_[i * dimension_ + j];
    //   }

    //   // LOG(INFO) << "]" << std::endl;
    // }
  }

  // insert
  {
    int cnt = data_base_size / step_count;
    int left_step_count = data_base_size % step_count;
    size_t id = 0;

    int64_t total = 0;
    int64_t total_another = 0;

    for (int c = 0; c < cnt; c++) {
      auto start_another = std::chrono::steady_clock::now();
      std::vector<pb::common::VectorWithId> vector_with_ids;
      for (int j = 0; j < step_count; j++) {
        pb::common::VectorWithId vector_with_id;
        id = c * step_count + j;
        vector_with_id.set_id(id);
        for (size_t i = 0; i < dimension; i++) {
          vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
        }
        vector_with_ids.push_back(vector_with_id);
      }
      auto start = std::chrono::steady_clock::now();
      ok = vector_index_hnsw->Upsert(vector_with_ids);
      auto end = std::chrono::steady_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      auto diff_another = std::chrono::duration_cast<std::chrono::microseconds>(end - start_another).count();
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      DINGO_LOG(INFO) << "step_count : " << step_count << " dimension : " << dimension << " index :" << c
                      << " diff : " << diff << " us"
                      << " diff_another : " << diff_another << " us";
      total += diff;
      total_another += diff_another;
    }

    if (left_step_count > 0) {
      auto start_another = std::chrono::steady_clock::now();
      std::vector<pb::common::VectorWithId> vector_with_ids;
      for (int j = 0; j < left_step_count; j++) {
        pb::common::VectorWithId vector_with_id;
        id = cnt * step_count + j;
        vector_with_id.set_id(id);
        for (size_t i = 0; i < dimension; i++) {
          vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
        }
        vector_with_ids.push_back(vector_with_id);
      }
      auto start = std::chrono::steady_clock::now();
      ok = vector_index_hnsw->Upsert(vector_with_ids);
      auto end = std::chrono::steady_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      auto diff_another = std::chrono::duration_cast<std::chrono::microseconds>(end - start_another).count();
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      DINGO_LOG(INFO) << "step_count : " << step_count << " dimension : " << dimension << " index :" << cnt
                      << " diff : " << diff << " us"
                      << " diff_another : " << diff_another << " us";
      total += diff;
      total_another += diff_another;
    }

    DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", data_base_size, total,
                                   static_cast<long double>(total) / data_base_size);
    DINGO_LOG(INFO) << fmt::format("total_another : {} cost : {} (us) avg : {} us", data_base_size, total_another,
                                   static_cast<long double>(total_another) / data_base_size);
  }  // insert end

  // stop
  // {
  //   LOG(INFO) << "program will exit !!! pid : " << getpid();
  //   // system("pause");
  //   int c = getchar();
  //   (void)c;  // to silence the unused variable warning
  // }
}

}  // namespace dingodb
