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
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "faiss/MetricType.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class VectorIndexMemoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() { vector_index_hnsw_.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_;
  inline static faiss::idx_t dimension_ = 1024;
  inline static int data_base_size_ = 1000000;
  inline static std::vector<float> data_base_;
  inline static uint32_t efconstruction = 40;
  inline static uint32_t max_elements = data_base_size_;
  inline static int32_t nlinks = 5;
  inline static int step_count = 2048;
};

TEST_F(VectorIndexMemoryTest, Create) {
  // valid param L2
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter()->set_dimension(dimension_);
    index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter()->set_efconstruction(efconstruction);

    index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter()->set_max_elements(max_elements);

    index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_NE(vector_index_hnsw_.get(), nullptr);
  }
}

TEST_F(VectorIndexMemoryTest, Add) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base_.resize(dimension_ * data_base_size_, 0.0f);

    std::cerr << "create data index : ";
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < data_base_size_; i++) {
      for (int j = 0; j < dimension_; j++) {
        data_base_[dimension_ * i + j] = distrib(rng);
      }
      data_base_[dimension_ * i] += i / 1000.;
      if (0 == i % 100000) {
        std::cerr << ".";
      }
    }
    std::cerr << std::endl;
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "create data index : span : " << diff << " s  ";

    // for (size_t i = 0; i < data_base_size_; i++) {
    //   // std::cout << "[" << i << "]"
    //   //           << " [";
    //   for (faiss::idx_t j = 0; j < dimension_; j++) {
    //     if (0 != j) {
    //       // std::cout << ",";
    //     }
    //     // std::cout << std::setw(10) << data_base_[i * dimension_ + j];
    //   }

    //   // std::cout << "]" << std::endl;
    // }
  }

  // insert
  {
    int cnt = data_base_size_ / step_count;
    int left_step_count = data_base_size_ % step_count;
    size_t id = 0;

    int64_t total = 0;

    for (int c = 0; c < cnt; c++) {
      std::vector<pb::common::VectorWithId> vector_with_ids;
      for (int j = 0; j < step_count; j++) {
        pb::common::VectorWithId vector_with_id;
        id = c * step_count + j;
        vector_with_id.set_id(id);
        for (size_t i = 0; i < dimension_; i++) {
          vector_with_id.mutable_vector()->add_float_values(data_base_[id * dimension_ + i]);
        }
        vector_with_ids.push_back(vector_with_id);
      }
      auto start = std::chrono::steady_clock::now();
      ok = vector_index_hnsw_->Add(vector_with_ids);
      auto end = std::chrono::steady_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      DINGO_LOG(INFO) << "step_count : " << step_count << " dimension : " << dimension_ << " index :" << c
                      << " diff : " << diff << " us";
      total += diff;
    }

    if (left_step_count > 0) {
      std::vector<pb::common::VectorWithId> vector_with_ids;
      for (int j = 0; j < left_step_count; j++) {
        pb::common::VectorWithId vector_with_id;
        id = cnt * step_count + j;
        vector_with_id.set_id(id);
        for (size_t i = 0; i < dimension_; i++) {
          vector_with_id.mutable_vector()->add_float_values(data_base_[id * dimension_ + i]);
        }
        vector_with_ids.push_back(vector_with_id);
      }
      auto start = std::chrono::steady_clock::now();
      ok = vector_index_hnsw_->Add(vector_with_ids);
      auto end = std::chrono::steady_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      DINGO_LOG(INFO) << "step_count : " << left_step_count << " dimension : " << dimension_ << " index :" << cnt
                      << " diff : " << diff << " us";
      total += diff;
    }

    DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", data_base_size_, total,
                                   static_cast<long double>(total) / data_base_size_);
  }  // insert end

  // stop
  {
    std::cout << "program will exit !!! pid : " << getpid() << std::endl;
    // system("pause");
    getchar();
  }
}

}  // namespace dingodb
