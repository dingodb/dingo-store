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

#include <algorithm>
#include <array>
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
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class VectorIndexFlatSearchParamLimitTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4); }

  static void TearDownTestSuite() {
    vector_index_flat_for_l2.reset();
    vector_index_flat_for_ip.reset();
    vector_index_flat_for_cosine.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_l2;      // id = 1;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_ip;      // id = 2;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_cosine;  // id = 3;
  inline static constexpr faiss::idx_t kDimension = 20;
  inline static int data_base_size = 20;
  inline static std::vector<float> data_base;
  inline static int64_t id_for_l2 = 1;
  inline static int64_t id_for_ip = 2;
  inline static int64_t id_for_cosine = 3;
  // include this ID
  inline static int64_t vector_id_start = 1000;

  // vector_id_end = vector_id_start + data_base_size [Do not include this ID]
  inline static int64_t vector_id_end = vector_id_start + data_base_size;

  inline static int vector_ids_search_size = 10;

  inline static int search_topk = 1;

  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexFlatSearchParamLimitTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexFlatSearchParamLimitTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  // valid param L2
  {
    int64_t id = id_for_l2;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(kDimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat_for_l2 =
        VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_flat_for_l2.get(), nullptr);
  }

  // valid param IP
  {
    int64_t id = id_for_ip;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(kDimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_flat_for_ip =
        VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_flat_for_ip.get(), nullptr);
  }

  // valid param COSINE
  {
    int64_t id = id_for_cosine;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(kDimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    vector_index_flat_for_cosine =
        VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_flat_for_cosine.get(), nullptr);
  }
}

TEST_F(VectorIndexFlatSearchParamLimitTest, Search) {
  butil::Status ok;

  auto lambda_random_function = []() {
    std::vector<int64_t> vector_ids;
    vector_ids.resize(data_base_size);

    for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
      vector_ids[i] = id;
    }

    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(vector_ids.begin(), vector_ids.end(), std::default_random_engine(seed));

    std::vector<int64_t> vector_ids_for_search;
    vector_ids_for_search.resize(vector_ids_search_size);
    for (size_t i = 0; i < vector_ids_search_size; i++) {
      vector_ids_for_search[i] = vector_ids[i];
    }

    return std::tuple<std::vector<int64_t>, std::vector<int64_t>>(vector_ids, vector_ids_for_search);
  };

  auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_flat, std::string name,
                                                       uint32_t search_topk_param, bool has_filter) {
    butil::Status ok;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(kDimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

    std::array<float, kDimension> my_value{0.560676634, 0.36369127,  0.397202849, 0.0810661539, 0.291333258,
                                           0.545552671, 0.945311069, 0.694609702, 0.158891559,  0.366846412,
                                           0.331690609, 0.189103171, 0.127474368, 0.468271106,  0.230491146,
                                           0.427249402, 0.21386537,  0.42593497,  0.132474691,  0.483059853};

    for (float value : my_value) {
      vector_with_id.mutable_vector()->add_float_values(value);
    }

    uint32_t topk = search_topk_param;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    auto [vector_ids, vector_ids_for_search] = lambda_random_function();

    auto vector_ids_for_search_copy = vector_ids_for_search;

    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
    if (has_filter) {
      filters.emplace_back(std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_ids_for_search)));
    }

    ok = vector_index_flat->Search(vector_with_ids, topk, filters, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    vector_ids_for_search = vector_ids_for_search_copy;

    std::vector<int64_t> result_vector_ids;
    {
      size_t i = 0;
      LOG(INFO) << "[" << i << "]";
      for (const auto &result : results) {
        {
          size_t j = 0;
          for (const auto &vector_with_distances : result.vector_with_distances()) {
            LOG(INFO) << "[" << j << "]";
            auto id = vector_with_distances.vector_with_id().id();
            auto distance = vector_with_distances.distance();
            auto metric_type = vector_with_distances.metric_type();

            result_vector_ids.push_back(id);

            LOG(INFO) << vector_with_distances.DebugString();
            j++;
          }
        }
        i++;
      }
    }

    LOG(INFO) << "vector_ids            : [";
    for (auto vector_id = vector_id_start; vector_id < vector_id_end; vector_id++) {
      LOG(INFO) << vector_id << " ";
    }
    LOG(INFO) << "]";

    std::sort(vector_ids_for_search.begin(), vector_ids_for_search.end());
    LOG(INFO) << "vector_ids_for_search : [";
    for (const auto vector_id : vector_ids_for_search) {
      LOG(INFO) << vector_id << " ";
    }
    LOG(INFO) << "]";

    std::sort(result_vector_ids.begin(), result_vector_ids.end());
    LOG(INFO) << "result_vector_ids     : [";
    for (const auto result_vector_id : result_vector_ids) {
      LOG(INFO) << result_vector_id << " ";
    }
    LOG(INFO) << "]";

    bool is_return_true = !result_vector_ids.empty();
    LOG(INFO) << "====================> : ";
    for (auto result_vector_id : result_vector_ids) {
      auto iter = std::find(vector_ids_for_search.begin(), vector_ids_for_search.end(), result_vector_id);
      if (iter == vector_ids_for_search.end()) {
        is_return_true = false;
        EXPECT_TRUE(false);
      } else {
        LOG(INFO) << *iter << " ";
      }
    }
    LOG(INFO);

    if (is_return_true) {
      LOG(INFO) << name << " result_vector_ids all in vector_ids";
    } else {
      LOG(INFO) << name << " result_vector_ids not all in vector_ids";
    }
    LOG(INFO) << "...........................................................................";
  };

  // l2 ok crash
  { lambda_alg_function(vector_index_flat_for_l2, "L2", 1, true); }

  // l2 ok crash
  { lambda_alg_function(vector_index_flat_for_l2, "L2", 1, false); }

  for (int i = 2; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_l2, "L2", i, true);
    lambda_alg_function(vector_index_flat_for_l2, "L2", i, false);
  }

  for (int i = 1; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_ip, "IP", i, true);
    lambda_alg_function(vector_index_flat_for_ip, "IP", i, false);
  }

  for (int i = 1; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_cosine, "cosine", i, true);
    lambda_alg_function(vector_index_flat_for_cosine, "cosine", i, false);
  }
}

TEST_F(VectorIndexFlatSearchParamLimitTest, Upsert) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(kDimension * data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < kDimension; j++) data_base[kDimension * i + j] = distrib(rng);
      data_base[kDimension * i] += i / 1000.;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[";
      LOG(INFO) << std::setiosflags(std::ios::right) << std::setw(3) << std::setfill('0') << i;
      LOG(INFO) << "] [";
      for (faiss::idx_t j = 0; j < kDimension; j++) {
        if (0 != j) {
          // LOG(INFO) << ",";
        }
        // LOG(INFO) << std::setw(10) << data_base[i * dimension + j];
        LOG(INFO) << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ')
                  << data_base[i * kDimension + j] << " ";
      }

      LOG(INFO) << "]";
    }
  }

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = vector_id_start, j = 0; id < vector_id_end; id++, j++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < kDimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[j * kDimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_for_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_flat_for_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_flat_for_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatSearchParamLimitTest, SearchAfterInsert) {
  butil::Status ok;

  auto lambda_random_function = []() {
    std::vector<int64_t> vector_ids;
    vector_ids.resize(data_base_size);

    for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
      vector_ids[i] = id;
    }

    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(vector_ids.begin(), vector_ids.end(), std::default_random_engine(seed));

    std::vector<int64_t> vector_ids_for_search;
    vector_ids_for_search.resize(vector_ids_search_size);
    for (size_t i = 0; i < vector_ids_search_size; i++) {
      vector_ids_for_search[i] = vector_ids[i];
    }

    return std::tuple<std::vector<int64_t>, std::vector<int64_t>>(vector_ids, vector_ids_for_search);
  };

  auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_flat, std::string,
                                                       uint32_t search_topk_param, bool has_filter) {
    butil::Status ok;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(kDimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

    std::array<float, kDimension> my_value{0.560676634, 0.36369127,  0.397202849, 0.0810661539, 0.291333258,
                                           0.545552671, 0.945311069, 0.694609702, 0.158891559,  0.366846412,
                                           0.331690609, 0.189103171, 0.127474368, 0.468271106,  0.230491146,
                                           0.427249402, 0.21386537,  0.42593497,  0.132474691,  0.483059853};

    for (float value : my_value) {
      vector_with_id.mutable_vector()->add_float_values(value);
    }

    uint32_t topk = search_topk_param;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    auto [vector_ids, vector_ids_for_search] = lambda_random_function();

    auto vector_ids_for_search_copy = vector_ids_for_search;

    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
    if (has_filter) {
      filters.emplace_back(std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_ids_for_search)));
    }

    ok = vector_index_flat->Search(vector_with_ids, topk, filters, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  };

  // l2 ok crash
  { lambda_alg_function(vector_index_flat_for_l2, "L2", 1, true); }

  // l2 ok crash
  { lambda_alg_function(vector_index_flat_for_l2, "L2", 1, false); }

  for (int i = 1; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_l2, "L2", i, true);
    lambda_alg_function(vector_index_flat_for_l2, "L2", i, false);
  }

  for (int i = 1; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_ip, "IP", i, true);
    lambda_alg_function(vector_index_flat_for_ip, "IP", i, false);
  }

  for (int i = 1; i <= 200; i++) {
    lambda_alg_function(vector_index_flat_for_cosine, "cosine", i, true);
    lambda_alg_function(vector_index_flat_for_cosine, "cosine", i, false);
  }
}

}  // namespace dingodb
