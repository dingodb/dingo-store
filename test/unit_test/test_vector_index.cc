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

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "vector/codec.h"
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

class VectorIndexWrapperTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4); }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexWrapperTest::vector_index_thread_pool = nullptr;

pb::common::RegionEpoch GenEpoch(int version) {
  pb::common::RegionEpoch epoch;
  epoch.set_conf_version(1);
  epoch.set_version(version);
  return epoch;
}

pb::common::Range GenRange(int start_vector_id, int end_vector_id) {
  pb::common::Range range;
  std::string start_key;
  VectorCodec::EncodeVectorKey('r', 1000, start_vector_id, start_key);
  range.set_start_key(start_key);
  std::string end_key;
  VectorCodec::EncodeVectorKey('r', 1000, end_vector_id, end_key);
  range.set_end_key(end_key);

  return range;
}

std::vector<pb::common::VectorWithId> GenVectorWithIds(int start_vector_id, int end_vector_id, int dimension) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib;

  std::vector<pb::common::VectorWithId> vector_with_ids;

  for (int id = start_vector_id; id < end_vector_id; ++id) {
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(id);
    for (int i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }

    vector_with_ids.push_back(vector_with_id);
  }

  return vector_with_ids;
}

TEST_F(VectorIndexWrapperTest, Add) {
  GTEST_SKIP() << "Skip Add...";
  int64_t id = 1;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
  index_parameter.mutable_hnsw_parameter()->set_dimension(256);
  index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
  index_parameter.mutable_hnsw_parameter()->set_efconstruction(200);
  index_parameter.mutable_hnsw_parameter()->set_max_elements(100000);
  index_parameter.mutable_hnsw_parameter()->set_nlinks(2);

  auto vector_index_hnsw =
      VectorIndexFactory::NewHnsw(id, index_parameter, GenEpoch(10), GenRange(1, 1000), vector_index_thread_pool);

  auto vector_index_wrapper = VectorIndexWrapper::New(id, index_parameter);

  vector_index_wrapper->UpdateVectorIndex(vector_index_hnsw, "unit test");

  // Add vector data
  std::vector<pb::common::VectorWithId> vector_with_ids = GenVectorWithIds(1, 1000, 256);
  auto status = vector_index_wrapper->Add(vector_with_ids);
  EXPECT_EQ(true, status.ok());
}

TEST_F(VectorIndexWrapperTest, Add_Sibling) {
  int64_t id = 1;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
  index_parameter.mutable_hnsw_parameter()->set_dimension(256);
  index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
  index_parameter.mutable_hnsw_parameter()->set_efconstruction(200);
  index_parameter.mutable_hnsw_parameter()->set_max_elements(100000);
  index_parameter.mutable_hnsw_parameter()->set_nlinks(2);

  auto vector_index =
      VectorIndexFactory::NewHnsw(id, index_parameter, GenEpoch(10), GenRange(1, 1000), vector_index_thread_pool);
  auto sibling_vector_index =
      VectorIndexFactory::NewHnsw(id, index_parameter, GenEpoch(10), GenRange(1000, 2000), vector_index_thread_pool);

  auto vector_index_wrapper = VectorIndexWrapper::New(id, index_parameter);

  vector_index_wrapper->UpdateVectorIndex(vector_index, "unit test");

  vector_index_wrapper->SetSiblingVectorIndex(sibling_vector_index);

  // Add vector data
  std::vector<pb::common::VectorWithId> vector_with_ids = GenVectorWithIds(1, 1500, 256);
  auto status = vector_index_wrapper->Add(vector_with_ids);
  EXPECT_EQ(true, status.ok());

  int64_t count = 0;
  vector_index_wrapper->GetCount(count);
  EXPECT_EQ(1499, count);
}

static void MergeSearchResult(uint32_t topk, pb::index::VectorWithDistanceResult& input_1,
                              pb::index::VectorWithDistanceResult& input_2,
                              pb::index::VectorWithDistanceResult& results) {
  int input_1_size = input_1.vector_with_distances_size();
  int input_2_size = input_2.vector_with_distances_size();
  auto* vector_with_distances_1 = input_1.mutable_vector_with_distances();
  auto* vector_with_distances_2 = input_2.mutable_vector_with_distances();

  int i = 0, j = 0;
  while (i < input_1_size && j < input_2_size) {
    auto& distance_1 = vector_with_distances_1->at(i);
    auto& distance_2 = vector_with_distances_2->at(j);
    if (distance_1.distance() <= distance_2.distance()) {
      ++i;
      results.add_vector_with_distances()->Swap(&distance_1);
    } else {
      ++j;
      results.add_vector_with_distances()->Swap(&distance_2);
    }

    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }

  for (; i < input_1_size; ++i) {
    auto& distance = vector_with_distances_1->at(i);
    results.add_vector_with_distances()->Swap(&distance);
    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }

  for (; j < input_2_size; ++j) {
    auto& distance = vector_with_distances_2->at(j);
    results.add_vector_with_distances()->Swap(&distance);
    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }
}

pb::index::VectorWithDistanceResult GenVectorWithDistanceResult(int start_vector_id, int end_vector_id, int dimension) {
  pb::index::VectorWithDistanceResult result;

  float distance = 0.01;
  auto vector_with_ids = GenVectorWithIds(start_vector_id, end_vector_id, dimension);
  for (auto& vector_with_id : vector_with_ids) {
    pb::common::VectorWithDistance vector_with_distance;
    vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
    vector_with_distance.set_distance(distance);
    result.add_vector_with_distances()->Swap(&vector_with_distance);

    distance += 0.001;
  }

  return result;
}

TEST_F(VectorIndexWrapperTest, MergeSearchResult) {
  {
    pb::index::VectorWithDistanceResult result_1 = GenVectorWithDistanceResult(1, 10, 64);
    pb::index::VectorWithDistanceResult result_2 = GenVectorWithDistanceResult(10, 20, 64);
    pb::index::VectorWithDistanceResult result;

    MergeSearchResult(10, result_1, result_2, result);
    EXPECT_EQ(10, result.vector_with_distances_size());
    for (const auto& vector_with_distance : result.vector_with_distances()) {
      EXPECT_LE(vector_with_distance.distance(), 0.016);
    }
  }

  {
    pb::index::VectorWithDistanceResult result_1 = GenVectorWithDistanceResult(1, 10, 64);
    pb::index::VectorWithDistanceResult result_2 = GenVectorWithDistanceResult(10, 20, 64);
    pb::index::VectorWithDistanceResult result;

    MergeSearchResult(30, result_1, result_2, result);
    EXPECT_EQ(19, result.vector_with_distances_size());
  }

  {
    pb::index::VectorWithDistanceResult result_1 = GenVectorWithDistanceResult(1, 5, 64);
    pb::index::VectorWithDistanceResult result_2 = GenVectorWithDistanceResult(10, 20, 64);
    pb::index::VectorWithDistanceResult result;

    MergeSearchResult(12, result_1, result_2, result);
    EXPECT_EQ(12, result.vector_with_distances_size());
  }
}

}  // namespace dingodb
