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
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_ivf_pq.h"

namespace dingodb {

class VectorIndexIvfPqTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {
    vector_index_ivf_pq_l2.reset();
    vector_index_ivf_pq_ip.reset();
    vector_index_ivf_pq_cosine.reset();
  }

  static void ReCreate() {
    static const pb::common::Range kRange;
    // valid param IP
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_ivf_pq_ip = std::make_shared<VectorIndexIvfPq>(id, index_parameter, kRange);
    }

    // valid param L2
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_ivf_pq_l2 = std::make_shared<VectorIndexIvfPq>(id, index_parameter, kRange);
    }

    // valid param cosine
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_ivf_pq_cosine = std::make_shared<VectorIndexIvfPq>(id, index_parameter, kRange);
    }
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_ivf_pq_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_ivf_pq_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_ivf_pq_cosine;
  inline static faiss::idx_t dimension = 64;
  // inline static int data_base_size = 100000;
  inline static int data_base_size = 1000;
  inline static int32_t ncentroids = 100;
  inline static size_t nsubvector = 8;
  inline static int32_t nbits_per_idx = 8;
  inline static std::vector<float> data_base;
  inline static int32_t start_id = 1000;
  inline static std::string path_l2 = "./l2_ivf_pq";
  inline static std::string path_ip = "./ip_ivf_pq";
  inline static std::string path_cosine = "./cosine_ivf_pq";
};

TEST_F(VectorIndexIvfPqTest, Create) {
  static const pb::common::Range kRange;
  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(64);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(64);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // valid param L2  ncentroids = 0
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(-1);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);

    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(100);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(3);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);

    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(100);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(1);
    index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(-1);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // invalid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);

    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(100);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(3);
    index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(1);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_EQ(vector_index_ivf_pq_l2.get(), nullptr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // valid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
    index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
    vector_index_ivf_pq_ip = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_ivf_pq_ip.get(), nullptr);
  }

  // valid param L2
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
    index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
    vector_index_ivf_pq_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_ivf_pq_l2.get(), nullptr);
  }

  // valid param cosine
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
    index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
    index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
    index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
    vector_index_ivf_pq_cosine = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_ivf_pq_cosine.get(), nullptr);
  }
}

TEST_F(VectorIndexIvfPqTest, DeleteNoData) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_ivf_pq_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_ivf_pq_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_ivf_pq_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_ivf_pq_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, SearchNotTrain) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, RangeSearchNotTrain) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->RangeSearch(vector_with_ids, radius, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->RangeSearch(vector_with_ids, radius, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->RangeSearch(vector_with_ids, radius, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, AddNotTrain) {
  butil::Status ok;

  std::vector<float> internal_data_base;
  auto internal_data_base_size = 65536 + 1;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    internal_data_base.resize(dimension * internal_data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < internal_data_base_size; i++) {
      for (int j = 0; j < dimension; j++) internal_data_base[dimension * i + j] = distrib(rng);
      internal_data_base[dimension * i] += i / 1000.;
    }
    std::cout << "create random data complete!!!" << '\n';
  }

  auto internal_start_id = start_id;

  // one ok flat
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(internal_start_id);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(internal_data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  ReCreate();

  // ivf_pq
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < internal_data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(internal_start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(internal_data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  ReCreate();
}

TEST_F(VectorIndexIvfPqTest, NeedToSave) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
}

TEST_F(VectorIndexIvfPqTest, TrainVectorWithId) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // invalid.  no data
  {
    ok = vector_index_ivf_pq_l2->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // invalid . not align.
  {
    std::vector<float> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    data_base_not_align.insert(data_base_not_align.end(), data_base.begin(), data_base.end() - 1);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.mutable_vector()->mutable_float_values()->Add(
          data_base_not_align.begin() + (i * static_cast<int>(dimension)),
          data_base_not_align.begin() + ((i + 1) * static_cast<int>(dimension)));

      vector_with_ids.push_back(vector_with_id);
    }

    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->mutable_float_values()->Add(
        data_base_not_align.begin() + ((data_base_size - 1) * static_cast<int>(dimension)),
        data_base_not_align.begin() + (data_base_size * static_cast<int>(dimension) - 1));
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<float> data_base_too_small = data_base;
    data_base_too_small.insert(data_base_too_small.end(), data_base.begin(), data_base.end() - dimension);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.mutable_vector()->mutable_float_values()->Add(
          data_base_too_small.begin() + (i * static_cast<int>(dimension)),
          data_base_too_small.begin() + ((i + 1) * static_cast<int>(dimension)));

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ReCreate();
  }
}

TEST_F(VectorIndexIvfPqTest, Train) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // invalid.  no data
  {
    ok = vector_index_ivf_pq_l2->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // invalid . not align.
  {
    std::vector<float> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    ok = vector_index_ivf_pq_l2->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<float> data_base_too_small = data_base;
    data_base_too_small.resize((ncentroids - 1) * dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<float> data_base_warning = data_base;
    data_base_warning.resize(38 * (ncentroids)*dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<float> data_base_warning = data_base;
    data_base_warning.resize(255 * (ncentroids)*dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning. ok
  {
    ok = vector_index_ivf_pq_l2->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    // ReCreate();
  }
}

TEST_F(VectorIndexIvfPqTest, Add) {
  butil::Status ok;
  auto internal_start_id = start_id;

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(internal_start_id);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(internal_start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, NeedToSaveAfterAdd) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
}

TEST_F(VectorIndexIvfPqTest, Delete) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist
  {
    int64_t id = start_id;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }
}

TEST_F(VectorIndexIvfPqTest, Upsert) {
  butil::Status ok;

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_ivf_pq_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results_l2) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "L2 : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "L2 : All Id in  vectors ";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_ip) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "IP : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "IP : All Id in vectors ";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_cosine) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "COSINE : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "COSINE : All Id in vectors ";
    }
  }
}

TEST_F(VectorIndexIvfPqTest, RangeSearch) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->RangeSearch(vector_with_ids, radius, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->RangeSearch(vector_with_ids, radius, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->RangeSearch(vector_with_ids, radius, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, NeedToRebuild) {
  bool b1 = vector_index_ivf_pq_l2->NeedToRebuild();
  bool b2 = vector_index_ivf_pq_ip->NeedToRebuild();
  bool b3 = vector_index_ivf_pq_cosine->NeedToRebuild();
}

TEST_F(VectorIndexIvfPqTest, NeedTrain) {
  EXPECT_TRUE(vector_index_ivf_pq_l2->NeedTrain());
  EXPECT_TRUE(vector_index_ivf_pq_ip->NeedTrain());
  EXPECT_TRUE(vector_index_ivf_pq_cosine->NeedTrain());
}

TEST_F(VectorIndexIvfPqTest, IsTrained) {
  EXPECT_TRUE(vector_index_ivf_pq_l2->IsTrained());
  EXPECT_TRUE(vector_index_ivf_pq_ip->IsTrained());
  EXPECT_TRUE(vector_index_ivf_pq_cosine->IsTrained());
}

TEST_F(VectorIndexIvfPqTest, GetCount) {
  int64_t count = 0;
  vector_index_ivf_pq_l2->GetCount(count);
  vector_index_ivf_pq_ip->GetCount(count);
  vector_index_ivf_pq_cosine->GetCount(count);
}

TEST_F(VectorIndexIvfPqTest, GetMemorySize) {
  int64_t memory_size = 0;
  vector_index_ivf_pq_l2->GetMemorySize(memory_size);
  vector_index_ivf_pq_ip->GetMemorySize(memory_size);
  vector_index_ivf_pq_cosine->GetMemorySize(memory_size);
}

TEST_F(VectorIndexIvfPqTest, Save) {
  butil::Status ok;

  ok = vector_index_ivf_pq_l2->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_ivf_pq_l2->Save("/var/ivf_pq");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Save(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_ivf_pq_ip->Save(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_ivf_pq_cosine->Save(path_cosine);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexIvfPqTest, Load) {
  butil::Status ok;

  ok = vector_index_ivf_pq_l2->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Load("/var/ivf_pq");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_l2->Load(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_ip->Load(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_ip->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_cosine->Load(path_cosine);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_cosine->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
}

TEST_F(VectorIndexIvfPqTest, SearchAfterLoad) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results_l2) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "L2 : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "L2 : All Id in  vectors ";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_ip) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "IP : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "IP : All Id in vectors ";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_cosine) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "COSINE : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "COSINE : All Id in vectors ";
    }
  }
}

TEST_F(VectorIndexIvfPqTest, RangeSearchAfterLoad) {
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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->RangeSearch(vector_with_ids, radius, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->RangeSearch(vector_with_ids, radius, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->RangeSearch(vector_with_ids, radius, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

///////////////////////////--------------------------------------------------------------------
////////////////////////////////////ivf pq test //////////////////////////////////////////////

TEST_F(VectorIndexIvfPqTest, TrainVectorWithIdForIvfPq) {
  butil::Status ok;

  ReCreate();

  {
    faiss::ClusteringParameters clustering_parameters;

    faiss::idx_t train_nlist_size = clustering_parameters.max_points_per_centroid * ncentroids;

    faiss::ProductQuantizer pq = faiss::ProductQuantizer(dimension, nsubvector, nbits_per_idx);

    faiss::idx_t train_subvector_size = pq.cp.max_points_per_centroid * (1 << nbits_per_idx);

    data_base_size = std::max(train_nlist_size, train_subvector_size);

    data_base_size += 100;
  }

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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // invalid.  no data
  {
    ok = vector_index_ivf_pq_l2->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // invalid . not align.
  {
    std::vector<float> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    data_base_not_align.insert(data_base_not_align.end(), data_base.begin(), data_base.end() - 1);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.mutable_vector()->mutable_float_values()->Add(
          data_base_not_align.begin() + (i * static_cast<int>(dimension)),
          data_base_not_align.begin() + ((i + 1) * static_cast<int>(dimension)));

      vector_with_ids.push_back(vector_with_id);
    }

    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->mutable_float_values()->Add(
        data_base_not_align.begin() + ((data_base_size - 1) * static_cast<int>(dimension)),
        data_base_not_align.begin() + (data_base_size * static_cast<int>(dimension) - 1));
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<float> data_base_too_small = data_base;
    data_base_too_small.insert(data_base_too_small.end(), data_base.begin(), data_base.end() - dimension);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.mutable_vector()->mutable_float_values()->Add(
          data_base_too_small.begin() + (i * static_cast<int>(dimension)),
          data_base_too_small.begin() + ((i + 1) * static_cast<int>(dimension)));

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ReCreate();
  }
}

TEST_F(VectorIndexIvfPqTest, NeedToSaveIvfPq) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
}

TEST_F(VectorIndexIvfPqTest, TrainForIvfPq) {
  butil::Status ok;

  ReCreate();

  {
    faiss::ClusteringParameters clustering_parameters;

    faiss::idx_t train_nlist_size = clustering_parameters.max_points_per_centroid * ncentroids;

    faiss::ProductQuantizer pq = faiss::ProductQuantizer(dimension, nsubvector, nbits_per_idx);

    faiss::idx_t train_subvector_size = pq.cp.max_points_per_centroid * (1 << nbits_per_idx);

    data_base_size = std::max(train_nlist_size, train_subvector_size);
    data_base_size += 100;
  }

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
      // std::cout << "[" << i << "]"
      //           << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          // std::cout << ",";
        }
        // std::cout << std::setw(10) << data_base[i * dimension + j];
      }

      // std::cout << "]" << '\n';
    }
  }

  std::cout << "create random data complete!!!" << '\n';

  // invalid.  no data
  {
    ok = vector_index_ivf_pq_l2->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(std::vector<float>{});
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // invalid . not align.
  {
    std::vector<float> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    ok = vector_index_ivf_pq_l2->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_ip->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
    ok = vector_index_ivf_pq_cosine->Train(data_base_not_align);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<float> data_base_too_small = data_base;
    data_base_too_small.resize((ncentroids - 1) * dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<float> data_base_warning = data_base;
    data_base_warning.resize(38 * (ncentroids)*dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<float> data_base_warning = data_base;
    data_base_warning.resize(255 * (ncentroids)*dimension);
    ok = vector_index_ivf_pq_l2->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning. ok
  {
    ok = vector_index_ivf_pq_l2->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    // ReCreate();
  }
}

TEST_F(VectorIndexIvfPqTest, AddForIvfPq) {
  butil::Status ok;
  auto internal_start_id = start_id;

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(internal_start_id);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(internal_start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, DeleteForIvfPq) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist
  {
    int64_t id = start_id;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }
}

TEST_F(VectorIndexIvfPqTest, UpsertForIvfPq) {
  butil::Status ok;

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_ivf_pq_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_ivf_pq_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_ivf_pq_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexIvfPqTest, NeedToSaveAfterAddIvfPq) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_ivf_pq_l2->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
}

TEST_F(VectorIndexIvfPqTest, SearchForIvfPq) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results_l2) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "L2 : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "L2 : All Id in  vectors ";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_ip) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "IP : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "IP : All Id in vectors ";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_cosine) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "COSINE : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "COSINE : All Id in vectors ";
    }
  }
}

TEST_F(VectorIndexIvfPqTest, NeedToRebuildForIvfPq) {
  bool b1 = vector_index_ivf_pq_l2->NeedToRebuild();
  bool b2 = vector_index_ivf_pq_ip->NeedToRebuild();
  bool b3 = vector_index_ivf_pq_cosine->NeedToRebuild();
}

TEST_F(VectorIndexIvfPqTest, NeedTrainForIvfPq) {
  EXPECT_TRUE(vector_index_ivf_pq_l2->NeedTrain());
  EXPECT_TRUE(vector_index_ivf_pq_ip->NeedTrain());
  EXPECT_TRUE(vector_index_ivf_pq_cosine->NeedTrain());
}

TEST_F(VectorIndexIvfPqTest, IsTrainedForIvfPq) {
  EXPECT_TRUE(vector_index_ivf_pq_l2->IsTrained());
  EXPECT_TRUE(vector_index_ivf_pq_ip->IsTrained());
  EXPECT_TRUE(vector_index_ivf_pq_cosine->IsTrained());
}

TEST_F(VectorIndexIvfPqTest, GetCountForIvfPq) {
  int64_t count = 0;
  vector_index_ivf_pq_l2->GetCount(count);
  vector_index_ivf_pq_ip->GetCount(count);
  vector_index_ivf_pq_cosine->GetCount(count);
}

TEST_F(VectorIndexIvfPqTest, GetMemorySizeForIvfPq) {
  int64_t memory_size = 0;
  vector_index_ivf_pq_l2->GetMemorySize(memory_size);
  vector_index_ivf_pq_ip->GetMemorySize(memory_size);
  vector_index_ivf_pq_cosine->GetMemorySize(memory_size);
}

TEST_F(VectorIndexIvfPqTest, SaveForIvfPqForIvfPq) {
  butil::Status ok;

  ok = vector_index_ivf_pq_l2->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_ivf_pq_l2->Save("/var/ivf_pq");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Save(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_ivf_pq_ip->Save(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_ivf_pq_cosine->Save(path_cosine);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexIvfPqTest, LoadForIvfPq) {
  butil::Status ok;

  ok = vector_index_ivf_pq_l2->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Load("/var/ivf_pq");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_l2->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_l2->Load(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_ip->Load(path_ip);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_ip->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_ivf_pq_cosine->Load(path_cosine);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_ivf_pq_cosine->Load(path_l2);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
}

TEST_F(VectorIndexIvfPqTest, SearchAfterLoadForIvfPq) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {}, results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i + start_id);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
        std::make_shared<VectorIndex::IvfPqListFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_pq()->set_nprobe(10);
    ok = vector_index_ivf_pq_l2->Search(vector_with_ids, topk, {filter}, results_l2, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_ip->Search(vector_with_ids, topk, {filter}, results_ip, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = vector_index_ivf_pq_cosine->Search(vector_with_ids, topk, {filter}, results_cosine, false, parameter);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results_l2) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results_l2) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "L2 : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "L2 : All Id in  vectors ";
    }

    for (const auto& result : results_ip) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_ip) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "IP : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "IP : All Id in vectors ";
    }

    for (const auto& result : results_cosine) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results_cosine) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "COSINE : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "COSINE : All Id in vectors ";
    }
  }
}

TEST_F(VectorIndexIvfPqTest, DeleteNoDataForIvfPq) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_ivf_pq_l2->Delete(ids);
    vector_index_ivf_pq_ip->Delete(ids);
    vector_index_ivf_pq_cosine->Delete(ids);
  }
}

}  // namespace dingodb
