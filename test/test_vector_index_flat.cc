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
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_flat.h"

namespace dingodb {

class VectorIndexMemoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() { vector_index_flat.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat;
  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;
};

TEST_F(VectorIndexMemoryTest, Create) {
  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(64);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(64);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat.get(), nullptr);
  }

  // valid param IP
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_NE(vector_index_flat.get(), nullptr);
  }

  // valid param L2
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat = VectorIndexFactory::New(id, index_parameter);
    EXPECT_NE(vector_index_flat.get(), nullptr);
  }
}

TEST_F(VectorIndexMemoryTest, Add) {
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

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexMemoryTest, Delete) {
  butil::Status ok;

  // id not found
  {
    uint64_t id = 10000000;
    std::vector<uint64_t> ids;
    ids.push_back(id);
    vector_index_flat->Delete(ids);
  }

  // id exist
  {
    uint64_t id = 0;
    std::vector<uint64_t> ids;
    ids.push_back(id);
    vector_index_flat->Delete(ids);
  }

  // id exist batch
  {
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_flat->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_flat->Delete(ids);
  }
}

TEST_F(VectorIndexMemoryTest, Upsert) {
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

  // add all data
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

    ok = vector_index_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // // must be delete .................................................................
  // {
  //   vector_index_flat_.reset();

  //   uint64_t id = 1;
  //   pb::common::IndexParameter index_parameter;
  //   index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
  //   index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
  //       ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  //   index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(dimension_);
  //   index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
  //       ::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
  //   vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
  //   EXPECT_NE(vector_index_flat_.get(), nullptr);
  // }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // create random data again
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

  // update all data
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

    ok = vector_index_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexMemoryTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_flat->Search(vector_with_ids, topk, results);
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
    ok = vector_index_flat->Search(vector_with_ids, topk, results);
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
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_flat->Search(vector_with_ids, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // id exist batch
  {
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    vector_index_flat->Delete(ids);
  }
}

}  // namespace dingodb
