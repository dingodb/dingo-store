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
#include <vector>

#include "butil/status.h"
#include "faiss/MetricType.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_hnsw.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

class VectorIndexHnswTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() { vector_index_hnsw.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_hnsw;
  inline static faiss::idx_t dimension = 16;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;
  inline static uint32_t efconstruction = 200;
  inline static uint32_t max_elements = data_base_size;
  inline static int32_t nlinks = 2;
  inline static int step_count = 10;
};

TEST_F(VectorIndexHnswTest, Create) {
  static const pb::common::Range kRange;
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

    pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(10);

    vector_index_hnsw = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw.get(), nullptr);
  }

#if 10  // NOLINT
  // IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);

    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);

    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(10);

    vector_index_hnsw = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw.get(), nullptr);
  }
#endif
}

TEST_F(VectorIndexHnswTest, DeleteNoData) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_hnsw->Delete(ids);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_hnsw->Delete(ids);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_hnsw->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_hnsw->Delete(ids);
  }
}

TEST_F(VectorIndexHnswTest, Upsert) {
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
      LOG(INFO) << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dimension + j];
      }

      LOG(INFO) << "]";
    }
  }

  // write data
  {
    data_base.clear();
    data_base.resize(10 * 16);
    std::array<float, 16> array1 = {0.0, 1.0, 2.0,  3.0,  4.0,  5.0,  6.0,  7.0,
                                    8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0};
    std::array<float, 16> array2 = {0.0,  3.0,  6.0,  9.0,  12.0, 15.0, 18.0, 21.0,
                                    24.0, 27.0, 30.0, 33.0, 36.0, 39.0, 42.0, 45.0};

    std::array<float, 16> array3 = {0.0,  4.0,  8.0,  12.0, 16.0, 20.0, 24.0, 28.0,
                                    32.0, 36.0, 40.0, 44.0, 48.0, 52.0, 56.0, 60.0};
    std::array<float, 16> array4 = {0.0,  5.0,  10.0, 15.0, 20.0, 25.0, 30.0, 35.0,
                                    40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0};
    std::array<float, 16> array5 = {0.0,  6.0,  12.0, 18.0, 24.0, 30.0, 36.0, 42.0,
                                    48.0, 54.0, 60.0, 66.0, 72.0, 78.0, 84.0, 90.0};

    std::array<float, 16> array6 = {0.0,  7.0,  14.0, 21.0, 28.0, 35.0, 42.0, 49.0,
                                    56.0, 63.0, 70.0, 77.0, 84.0, 91.0, 98.0, 105.0};

    std::array<float, 16> array7 = {0.0,  8.0,  16.0, 24.0, 32.0, 40.0,  48.0,  56.0,
                                    64.0, 72.0, 80.0, 88.0, 96.0, 104.0, 112.0, 120.0};
    std::array<float, 16> array8 = {0.0,  9.0,  18.0, 27.0, 36.0,  45.0,  54.0,  63.0,
                                    72.0, 81.0, 90.0, 99.0, 108.0, 117.0, 126.0, 135.0};
    std::array<float, 16> array9 = {0.0,  10.0, 20.0,  30.0,  40.0,  50.0,  60.0,  70.0,
                                    80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0};
    std::array<float, 16> array10 = {0.0,  11.0, 22.0,  33.0,  44.0,  55.0,  66.0,  77.0,
                                     88.0, 99.0, 110.0, 121.0, 132.0, 143.0, 154.0, 165.0};

    size_t i = 0;

    auto lambda_func = [&i](const std::array<float, 16> &array) {
      for (size_t j = 0; j < array.size(); j++) {
        data_base[i * 16 + j] = array[j];
      }
      i++;
    };

    lambda_func(array1);
    lambda_func(array2);
    lambda_func(array3);
    lambda_func(array4);
    lambda_func(array5);
    lambda_func(array6);
    lambda_func(array7);
    lambda_func(array8);
    lambda_func(array9);
    lambda_func(array10);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_hnsw->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    vector_index_hnsw->GetCount(count);

    LOG(INFO) << "count : " << count;

    auto *p = vector_index_hnsw.get();

    auto *pp = dynamic_cast<VectorIndexHnsw *>(p);

    for (size_t i = 0; i < 10; i++) {
      auto vt = pp->GetHnswIndex()->getDataByLabel<float>(i);
      LOG(INFO) << "i : " << i << " size : " << vt.size() << " [";
      for (auto v : vt) {
        LOG(INFO) << " " << v;
      }
      LOG(INFO) << " ]";
    }
  }
}

TEST_F(VectorIndexHnswTest, Search) {
  butil::Status ok;

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i + dimension];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 10;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    auto filter = std::make_shared<VectorIndex::RangeFilterFunctor>(0, INT64_MAX);
    ok = vector_index_hnsw->Search(vector_with_ids, topk, {filter}, true, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto &result : results) {
      for (const auto &vector_with_distances : result.vector_with_distances()) {
        LOG(INFO) << "id : " << vector_with_distances.vector_with_id().id() << " ";
        // LOG(INFO) << "float size : " << vector_with_distances.vector_with_id().vector().float_values().size() << " ";
        // LOG(INFO) << "value : [";
        LOG(INFO) << "distance : " << vector_with_distances.distance();
        LOG(INFO) << " [";
        for (const auto &value : vector_with_distances.vector_with_id().vector().float_values()) {
          LOG(INFO) << value << " ";
        }
        LOG(INFO) << "] ";

        LOG(INFO);
      }
      EXPECT_EQ(result.vector_with_distances_size(), 10);
    }
  }
}

TEST_F(VectorIndexHnswTest, EfSearch) {
  butil::Status ok;

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i + dimension];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 10;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    parameter.mutable_hnsw()->set_efsearch(400);

    auto filter = std::make_shared<VectorIndex::RangeFilterFunctor>(0, INT64_MAX);
    ok = vector_index_hnsw->Search(vector_with_ids, topk, {filter}, true, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto &result : results) {
      for (const auto &vector_with_distances : result.vector_with_distances()) {
        LOG(INFO) << "id : " << vector_with_distances.vector_with_id().id() << " ";
        // LOG(INFO) << "float size : " << vector_with_distances.vector_with_id().vector().float_values().size() << " ";
        // LOG(INFO) << "value : [";
        LOG(INFO) << "distance : " << vector_with_distances.distance();
        LOG(INFO) << " [";
        for (const auto &value : vector_with_distances.vector_with_id().vector().float_values()) {
          LOG(INFO) << value << " ";
        }
        LOG(INFO) << "] ";

        LOG(INFO);
      }
      EXPECT_EQ(result.vector_with_distances_size(), 10);
    }
  }
}

TEST_F(VectorIndexHnswTest, CreateCosine) {
  static const pb::common::Range kRange;
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

    pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(10);

    vector_index_hnsw = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw.get(), nullptr);
  }

#if 10  // NOLINT
  // IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);

    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);

    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(10);

    vector_index_hnsw = VectorIndexFactory::NewHnsw(id, index_parameter, epoch, kRange, nullptr);
    EXPECT_NE(vector_index_hnsw.get(), nullptr);
  }
#endif
}

TEST_F(VectorIndexHnswTest, UpsertCosine) {
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
      LOG(INFO) << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dimension + j];
      }

      LOG(INFO) << "]";
    }
  }

  // write data
  {
    data_base.clear();
    data_base.resize(10 * 16);
    std::array<float, 16> array1 = {1.0, 1.0, 2.0,  3.0,  4.0,  5.0,  6.0,  7.0,
                                    8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0};
    std::array<float, 16> array2 = {2.0,  3.0,  6.0,  9.0,  12.0, 15.0, 18.0, 21.0,
                                    24.0, 27.0, 30.0, 33.0, 36.0, 39.0, 42.0, 45.0};

    std::array<float, 16> array3 = {3.0,  4.0,  8.0,  12.0, 16.0, 20.0, 24.0, 28.0,
                                    32.0, 36.0, 40.0, 44.0, 48.0, 52.0, 56.0, 60.0};
    std::array<float, 16> array4 = {0.0,  5.0,  10.0, 15.0, 20.0, 25.0, 30.0, 35.0,
                                    40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0};
    std::array<float, 16> array5 = {0.0,  6.0,  12.0, 18.0, 24.0, 30.0, 36.0, 42.0,
                                    48.0, 54.0, 60.0, 66.0, 72.0, 78.0, 84.0, 90.0};

    std::array<float, 16> array6 = {4.0,  7.0,  14.0, 21.0, 28.0, 35.0, 42.0, 49.0,
                                    56.0, 63.0, 70.0, 77.0, 84.0, 91.0, 98.0, 105.0};

    std::array<float, 16> array7 = {5.0,  8.0,  16.0, 24.0, 32.0, 40.0,  48.0,  56.0,
                                    64.0, 72.0, 80.0, 88.0, 96.0, 104.0, 112.0, 120.0};
    std::array<float, 16> array8 = {6.0,  9.0,  18.0, 27.0, 36.0,  45.0,  54.0,  63.0,
                                    72.0, 81.0, 90.0, 99.0, 108.0, 117.0, 126.0, 135.0};
    std::array<float, 16> array9 = {7.0,  10.0, 20.0,  30.0,  40.0,  50.0,  60.0,  70.0,
                                    80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0};
    std::array<float, 16> array10 = {8.0,  11.0, 22.0,  33.0,  44.0,  55.0,  66.0,  77.0,
                                     88.0, 99.0, 110.0, 121.0, 132.0, 143.0, 154.0, 165.0};

    size_t i = 0;

    auto lambda_func = [&i](const std::array<float, 16> &array) {
      for (size_t j = 0; j < array.size(); j++) {
        data_base[i * 16 + j] = array[j];
      }
      i++;
    };

    lambda_func(array1);
    lambda_func(array2);
    lambda_func(array3);
    lambda_func(array4);
    lambda_func(array5);
    lambda_func(array6);
    lambda_func(array7);
    lambda_func(array8);
    lambda_func(array9);
    lambda_func(array10);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      std::vector<float> norm_array(dimension);
      auto hnsw_index = std::dynamic_pointer_cast<VectorIndexHnsw>(vector_index_hnsw);

      VectorIndexUtils::NormalizeVectorForHnsw(data_base.data() + id * dimension, dimension, norm_array.data());
      LOG(INFO) << "normalized [";
      for (size_t i = 0; i < dimension; i++) {
        LOG(INFO) << norm_array[i] << ", ";
      }
      LOG(INFO) << "]";

      LOG(INFO) << "orignal [";
      for (size_t i = 0; i < dimension; i++) {
        LOG(INFO) << data_base[id * dimension + i] << ", ";
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
        // vector_with_id.mutable_vector()->add_float_values(data_base[1 * dimension + i]);
      }
      LOG(INFO) << "]";

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_hnsw->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    vector_index_hnsw->GetCount(count);

    LOG(INFO) << "count : " << count;

    auto *p = vector_index_hnsw.get();

    auto *pp = dynamic_cast<VectorIndexHnsw *>(p);

    for (size_t i = 0; i < 10; i++) {
      auto vt = pp->GetHnswIndex()->getDataByLabel<float>(i);
      LOG(INFO) << "i : " << i << " size : " << vt.size() << " [";
      for (auto v : vt) {
        LOG(INFO) << " " << v;
      }
      LOG(INFO) << " ]";
    }
  }
}

TEST_F(VectorIndexHnswTest, SearchCosine) {
  butil::Status ok;

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
    uint32_t topk = 5;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    auto filter = std::make_shared<VectorIndex::RangeFilterFunctor>(0, INT64_MAX);
    ok = vector_index_hnsw->Search(vector_with_ids, topk, {filter}, true, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto &result : results) {
      for (const auto &vector_with_distances : result.vector_with_distances()) {
        LOG(INFO) << "id : " << vector_with_distances.vector_with_id().id() << " ";
        // LOG(INFO) << "float size : " << vector_with_distances.vector_with_id().vector().float_values().size() << " ";
        // LOG(INFO) << "value : [";
        LOG(INFO) << "distance : " << vector_with_distances.distance();
        LOG(INFO) << " [";
        for (const auto &value : vector_with_distances.vector_with_id().vector().float_values()) {
          LOG(INFO) << fmt::format("{}", value) << " ";
        }
        LOG(INFO) << "] ";

        LOG(INFO);
      }
      EXPECT_EQ(result.vector_with_distances_size(), topk);
    }
  }
}

TEST_F(VectorIndexHnswTest, EfSearchCosine) {
  butil::Status ok;

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
    uint32_t topk = 5;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    auto filter = std::make_shared<VectorIndex::RangeFilterFunctor>(0, INT64_MAX);

    pb::common::VectorSearchParameter parameter;
    parameter.mutable_hnsw()->set_efsearch(400);

    ok = vector_index_hnsw->Search(vector_with_ids, topk, {filter}, true, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto &result : results) {
      for (const auto &vector_with_distances : result.vector_with_distances()) {
        LOG(INFO) << "id : " << vector_with_distances.vector_with_id().id() << " ";
        // LOG(INFO) << "float size : " << vector_with_distances.vector_with_id().vector().float_values().size() << " ";
        // LOG(INFO) << "value : [";
        LOG(INFO) << "distance : " << vector_with_distances.distance();
        LOG(INFO) << " [";
        for (const auto &value : vector_with_distances.vector_with_id().vector().float_values()) {
          LOG(INFO) << fmt::format("{}", value) << " ";
        }
        LOG(INFO) << "] ";

        LOG(INFO);
      }
      EXPECT_EQ(result.vector_with_distances_size(), topk);
    }
  }
}

}  // namespace dingodb
