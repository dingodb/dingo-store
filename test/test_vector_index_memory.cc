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
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_flat.h"

namespace dingodb {

DEFINE_string(vector_index_type, "", "vector_index_type like hnsw or flat");
DEFINE_int64(max_elements, 0, "max_elements");
DEFINE_int64(dimension, 0, "dimension default 1024");
DEFINE_int64(efconstruction, 0, "efconstruction default 40");
DEFINE_int64(nlinks, 0, "nlinks default 32");
DEFINE_string(metric_type, "L2", "L2 or IP");

class VectorIndexMemoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {
    // google::ParseCommandLineFlags(&argc, &argv, false);
    std::cout << "FLAGS_vector_index_type : " << FLAGS_vector_index_type << std::endl;
    vector_index_.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_;
  inline static faiss::idx_t dimension_ = 1024;
  inline static int data_base_size_ = 1024000;
  inline static std::vector<float> data_base_;
};

TEST_F(VectorIndexMemoryTest, Create) {
  static const pb::common::Range kRange;
  // valid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension_);
    index_parameter.mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_ = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_.get(), nullptr);
  }

  // valid param L2
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension_);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_ = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_.get(), nullptr);
  }
}

TEST_F(VectorIndexMemoryTest, Add) {
  butil::Status ok;

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0);
    for (size_t i = 0; i < dimension_; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base_[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_->Add(vector_with_ids);
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

    ok = vector_index_->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

}  // namespace dingodb
