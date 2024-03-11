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
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

DECLARE_int64(hnsw_need_save_count);

class VectorIndexHnswSearchParamTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { LOG(INFO) << "hnsw_save_threshold_write_key_num : " << FLAGS_hnsw_need_save_count; }

  static void TearDownTestSuite() {
    vector_index_hnsw_for_l2.reset();
    vector_index_hnsw_for_ip.reset();
    vector_index_hnsw_for_cosine.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_cosine;
  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 20;
  inline static std::vector<float> data_base;
  inline static uint32_t efconstruction = 200;
  inline static uint32_t max_elements = data_base_size;
  inline static int32_t nlinks = 2;
  inline static int step_count = 10;
  inline static int64_t id_for_l2 = 1;
  inline static int64_t id_for_ip = 2;
  inline static int64_t id_for_cosine = 3;
  // include this ID
  inline static int64_t vector_id_start = 1000;

  // vector_id_end = vector_id_start + data_base_size [Do not include this ID]
  inline static int64_t vector_id_end = vector_id_start + data_base_size;

  inline static int vector_ids_search_size = 10;

  inline static int search_topk = 30;
};

TEST_F(VectorIndexHnswSearchParamTest, Create) {
  static const pb::common::Range kRange;
  pb::common::RegionEpoch epoch;
  epoch.set_conf_version(1);
  epoch.set_version(10);

  // valid param L2
  {
    int64_t id = id_for_l2;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_l2 = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw_for_l2.get(), nullptr);
  }

  // IP
  {
    int64_t id = id_for_ip;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_ip = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw_for_ip.get(), nullptr);
  }

  // cosine
  {
    int64_t id = id_for_cosine;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_cosine = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw_for_cosine.get(), nullptr);
  }
}

TEST_F(VectorIndexHnswSearchParamTest, Upsert) {
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
      LOG(INFO) << "[";
      LOG(INFO) << std::setiosflags(std::ios::right) << std::setw(3) << std::setfill('0') << i;
      LOG(INFO) << "] [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // LOG(INFO) << ",";
        }
        // LOG(INFO) << std::setw(10) << data_base[i * dimension + j];
        LOG(INFO) << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ')
                  << data_base[i * dimension + j] << " ";
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
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[j * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_hnsw_for_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_hnsw_for_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_hnsw_for_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexHnswSearchParamTest, Search) {
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

  auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_hnsw,
                                                       std::string name) {
    butil::Status ok;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = search_topk;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    auto [vector_ids, vector_ids_for_search] = lambda_random_function();

    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
    filters.push_back(std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids_for_search));
    ok = vector_index_hnsw->Search(vector_with_ids, topk, filters, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

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

  // l2 ok
  { lambda_alg_function(vector_index_hnsw_for_l2, "L2"); }

  // ip ok
  { lambda_alg_function(vector_index_hnsw_for_ip, "IP"); }

  // cosine ok
  { lambda_alg_function(vector_index_hnsw_for_cosine, "cosine"); }
}

TEST_F(VectorIndexHnswSearchParamTest, SearchOrder) {
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

  auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_hnsw, std::string) {
    butil::Status ok;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = search_topk;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    // auto [vector_ids, vector_ids_for_search] = lambda_random_function();

    // std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
    // filters.push_back(std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids_for_search));
    ok = vector_index_hnsw->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      size_t i = 0;
      LOG(INFO) << "[" << i << "]";
      for (const auto &result : results) {
        {
          size_t j = 0;
          for (const auto &vector_with_distances : result.vector_with_distances()) {
            LOG(INFO) << "[" << j << "]";
            LOG(INFO) << vector_with_distances.DebugString();
            j++;
          }
        }
        i++;
      }
    }

    LOG(INFO) << "...........................................................................";
  };

  // l2 ok
  { lambda_alg_function(vector_index_hnsw_for_l2, "L2"); }

  // ip ok
  { lambda_alg_function(vector_index_hnsw_for_ip, "IP"); }

  // cosine ok
  { lambda_alg_function(vector_index_hnsw_for_cosine, "cosine"); }
}

}  // namespace dingodb
