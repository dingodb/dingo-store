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
#include "vector/vector_index_factory.h"
#include "vector/vector_index_flat.h"

namespace dingodb {

class VectorIndexFlatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() { vector_index_flat_.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat_;
  inline static faiss::idx_t dimension_ = 8;
  inline static int data_base_size_ = 10;
  inline static std::vector<float> data_base_;
};

TEST_F(VectorIndexFlatTest, Create) {
  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
  }

  // invalid param
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(64);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
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
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_EQ(vector_index_flat_.get(), nullptr);
  }

  // valid param IP
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(dimension_);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_NE(vector_index_flat_.get(), nullptr);
  }

  // valid param L2
  {
    uint64_t id = 1;
    pb::common::IndexParameter index_parameter;
    index_parameter.set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
    index_parameter.mutable_vector_index_parameter()->set_vector_index_type(
        ::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_dimension(dimension_);
    index_parameter.mutable_vector_index_parameter()->mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat_ = VectorIndexFactory::New(id, index_parameter);
    EXPECT_NE(vector_index_flat_.get(), nullptr);
  }
}

TEST_F(VectorIndexFlatTest, Add) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base_.resize(dimension_ * data_base_size_, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size_; i++) {
      for (int j = 0; j < dimension_; j++) data_base_[dimension_ * i + j] = distrib(rng);
      data_base_[dimension_ * i] += i / 1000.;
    }

    for (size_t i = 0; i < data_base_size_; i++) {
      std::cout << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension_; j++) {
        if (0 != j) {
          std::cout << ",";
        }
        std::cout << std::setw(10) << data_base_[i * dimension_ + j];
      }

      std::cout << "]" << std::endl;
    }
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_flat_->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0);
    for (size_t i = 0; i < dimension_; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base_[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size_; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension_; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base_[id * dimension_ + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatTest, Delete) {
  butil::Status ok;

  // id not found
  {
    uint64_t id = 10000000;
    vector_index_flat_->Delete(id);
  }

  // id exist
  {
    uint64_t id = 0;
    vector_index_flat_->Delete(id);
  }

  // id exist batch
  {
    for (size_t i = 0; i < data_base_size_; i++) {
      uint64_t id = i;
      vector_index_flat_->Delete(id);
    }
  }

  // id exist batch again
  {
    for (size_t i = 0; i < data_base_size_; i++) {
      uint64_t id = i;
      vector_index_flat_->Delete(id);
    }
  }
}

TEST_F(VectorIndexFlatTest, Upsert) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base_.resize(dimension_ * data_base_size_, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size_; i++) {
      for (int j = 0; j < dimension_; j++) data_base_[dimension_ * i + j] = distrib(rng);
      data_base_[dimension_ * i] += i / 1000.;
    }

    for (size_t i = 0; i < data_base_size_; i++) {
      std::cout << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension_; j++) {
        if (0 != j) {
          std::cout << ",";
        }
        std::cout << std::setw(10) << data_base_[i * dimension_ + j];
      }

      std::cout << "]" << std::endl;
    }
  }

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size_; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension_; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base_[id * dimension_ + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_->Add(vector_with_ids);
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
    ok = vector_index_flat_->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // create random data again
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base_.resize(dimension_ * data_base_size_, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size_; i++) {
      for (int j = 0; j < dimension_; j++) data_base_[dimension_ * i + j] = distrib(rng);
      data_base_[dimension_ * i] += i / 1000.;
    }

    for (size_t i = 0; i < data_base_size_; i++) {
      std::cout << "[" << i << "]"
                << " [";
      for (faiss::idx_t j = 0; j < dimension_; j++) {
        if (0 != j) {
          std::cout << ",";
        }
        std::cout << std::setw(10) << data_base_[i * dimension_ + j];
      }

      std::cout << "]" << std::endl;
    }
  }

  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size_; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension_; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base_[id * dimension_ + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatTest, Search) {
  butil::Status ok;

  // invalid param failed
  {
    pb::common::VectorWithId vector_with_id;
    uint32_t topk = 0;
    std::vector<pb::common::VectorWithDistance> results;
    ok = vector_index_flat_->Search(vector_with_id, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // invalid param failed
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension_);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension_; i++) {
      float value = data_base_[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 0;
    std::vector<pb::common::VectorWithDistance> results;
    ok = vector_index_flat_->Search(vector_with_id, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension_);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension_; i++) {
      float value = data_base_[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = 3;
    std::vector<pb::common::VectorWithDistance> results;
    ok = vector_index_flat_->Search(vector_with_id, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    uint32_t topk = 3;
    std::vector<pb::common::VectorWithDistance> results;
    std::vector<float> my_vector;
    for (size_t i = 0; i < dimension_; i++) {
      float value = data_base_[i];
      my_vector.push_back(value);
    }
    ok = vector_index_flat_->Search(my_vector, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // id exist batch
  {
    for (size_t i = 0; i < data_base_size_; i++) {
      uint64_t id = i;
      vector_index_flat_->Delete(id);
    }
  }

  // ok
  {
    uint32_t topk = 3;
    std::vector<pb::common::VectorWithDistance> results;
    std::vector<float> my_vector;
    for (size_t i = 0; i < dimension_; i++) {
      float value = data_base_[i];
      my_vector.push_back(value);
    }
    ok = vector_index_flat_->Search(my_vector, topk, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

}  // namespace dingodb
