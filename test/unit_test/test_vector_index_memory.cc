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
#include <vector>

#include "butil/status.h"
#include "faiss/MetricType.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

DEFINE_string(vector_index_type, "", "vector_index_type like hnsw or flat");
DEFINE_int64(max_elements, 0, "max_elements");
DEFINE_int64(dimension, 0, "dimension default 1024");
DEFINE_int64(efconstruction, 0, "efconstruction default 40");
DEFINE_int64(nlinks, 0, "nlinks default 32");
DEFINE_string(metric_type, "L2", "L2 or IP");

class VectorIndexMemoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    data_base.reserve(data_base_size);
    for (int i = 0; i < data_base_size; ++i) {
      data_base.push_back((float)i / 3.0);
    }

    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4);
  }

  static void TearDownTestSuite() { vector_index.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index;
  inline static faiss::idx_t dimension = 1024;
  inline static int data_base_size = 1024000;
  inline static std::vector<float> data_base;

  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexMemoryTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexMemoryTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch k_epoch;
  k_epoch.set_conf_version(1);
  k_epoch.set_version(10);

  // valid param IP
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index = VectorIndexFactory::NewFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index.get(), nullptr);
  }

  // valid param L2
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index = VectorIndexFactory::NewFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index.get(), nullptr);
  }
}

TEST_F(VectorIndexMemoryTest, Add) {
  butil::Status ok;

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

}  // namespace dingodb
