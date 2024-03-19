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
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class VectorIndexRecallFlatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4); }

  static void TearDownTestSuite() {
    vector_index_flat_l2.reset();
    vector_index_flat_ip.reset();
    vector_index_flat_cosine.reset();
  }

  static void ReCreate() {
    static const pb::common::Range kRange;
    static pb::common::RegionEpoch kEpoch;  // NOLINT
    kEpoch.set_conf_version(1);
    kEpoch.set_version(10);

    // valid param L2
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
      index_parameter.mutable_flat_parameter()->set_dimension(dimension);
      index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    }

    // valid param IP
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
      index_parameter.mutable_flat_parameter()->set_dimension(dimension);
      index_parameter.mutable_flat_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      vector_index_flat_ip = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    }

    // valid param cosine
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
      index_parameter.mutable_flat_parameter()->set_dimension(dimension);
      index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      vector_index_flat_cosine =
          VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    }
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
};

ThreadPoolPtr VectorIndexRecallFlatTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexRecallFlatTest, Create) { ReCreate(); }

TEST_F(VectorIndexRecallFlatTest, Upsert) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      std::cout << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          std::cout << ",";
        }
        std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      std::cout << "]" << '\n';
    }
  }

  std::cout << "Add Vector" << '\n';

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id + 1);
      std::vector<float> vt_float;
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
        vt_float.push_back(data_base[id * dimension + i]);
      }
      std::cout << "[" << id << "]"
                << " [ ";
      for (size_t i = 0; i < vt_float.size(); i++) {
        if (0 != i) std::cout << ",";
        std::cout << std::setw(10) << vt_float[i];
      }
      std::cout << "]" << '\n';

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexRecallFlatTest, Search) {
  butil::Status ok;

  for (int64_t id = 0; id < data_base_size; id++) {
    // int64_t id = 0;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(id + 1);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[id * dimension + i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }

    std::cout << "id : " << (id + 1) << "\n";
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    bool first_flag = true;
#if 0
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto &result : results) {
      if (first_flag) {
        first_flag = false;
        auto internal_id = result.vector_with_distances().at(0).vector_with_id().id();
        EXPECT_EQ(id + 1, internal_id);
      }
      std::cout << result.DebugString();
    }
    std::cout << "\n";

    results.clear();

    first_flag = true;
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    for (const auto &result : results) {
      if (first_flag) {
        first_flag = false;
        auto internal_id = result.vector_with_distances().at(0).vector_with_id().id();
        EXPECT_EQ(id + 1, internal_id);
      }
      std::cout << result.DebugString();
    }
    std::cout << "\n";
#endif
    results.clear();

    first_flag = true;
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    for (const auto &result : results) {
      if (first_flag) {
        first_flag = false;
        auto internal_id = result.vector_with_distances().at(0).vector_with_id().id();
        EXPECT_EQ(id + 1, internal_id);
      }
      // std::cout << result.DebugString();
    }
    std::cout << "\n";

    results.clear();
  }
}

}  // namespace dingodb
