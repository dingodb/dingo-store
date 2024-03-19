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
#include "common/helper.h"
#include "common/logging.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

static const std::string kTempDataDirectory = "./unit_test/vector_index_flat";

class VectorIndexFlatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kTempDataDirectory);
    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4);
  }

  static void TearDownTestSuite() {
    vector_index_flat_l2.reset();
    vector_index_flat_ip.reset();
    vector_index_flat_cosine.reset();

    Helper::RemoveAllFileOrDirectory(kTempDataDirectory);
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

ThreadPoolPtr VectorIndexFlatTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexFlatTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_flat_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(64);
    vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_flat_l2.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(64);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_flat_l2.get(), nullptr);
  }

  // valid param L2
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat_l2 = VectorIndexFactory::NewFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_flat_l2.get(), nullptr);
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
    EXPECT_NE(vector_index_flat_ip.get(), nullptr);
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
    EXPECT_NE(vector_index_flat_cosine.get(), nullptr);
  }
}

TEST_F(VectorIndexFlatTest, DeleteNoData) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }
}

TEST_F(VectorIndexFlatTest, SearchNoData) {
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
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatTest, RangeSearchNoData) {
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

  // ok add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_l2) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_ip) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_cosine) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
  }

  // ok  all with filter
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;

    std::vector<int64_t> vector_ids;

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_l2) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_ip) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {},
                                               results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_cosine) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
  }
}

TEST_F(VectorIndexFlatTest, NeedToSave) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
}

TEST_F(VectorIndexFlatTest, Add) {
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

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0 + data_base_size);
    for (size_t i = 0; i < dimension; i++) {
      vector_with_id.mutable_vector()->add_float_values(data_base[i]);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1 + data_base_size; id < data_base_size + data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

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

TEST_F(VectorIndexFlatTest, NeedToSaveAfterAdd) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_flat_l2->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
  ok = vector_index_flat_ip->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
  ok = vector_index_flat_cosine->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
}

TEST_F(VectorIndexFlatTest, Delete) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
  }

  // id exist
  {
    int64_t id = 0 + data_base_size;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0 + data_base_size; i < data_base_size + data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0 + data_base_size; i < data_base_size + data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_flat_l2->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
    ok = vector_index_flat_ip->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::EVECTOR_INVALID);
  }
}

TEST_F(VectorIndexFlatTest, Upsert) {
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

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0 + data_base_size; id < data_base_size + data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->Upsert(vector_with_ids);
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

  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0 + data_base_size; id < data_base_size + data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_l2->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
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

    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
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
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // // id exist batch
  // {
  //   std::vector<int64_t> ids;
  //   for (size_t i = 0 + data_base_size; i < data_base_size + data_base_size; i++) {
  //     ids.push_back(i);
  //   }
  //   vector_index_flat->Delete(ids);
  // }
}

TEST_F(VectorIndexFlatTest, RangeSearch) {
  butil::Status ok;

  // invalid param failed,
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
    results.clear();

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
    results.clear();
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();

    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  // ok add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }

    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  // ok  all with filter
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;

    std::vector<int64_t> vector_ids;

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_l2) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_ip) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {},
                                               results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_cosine) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
  }
}

TEST_F(VectorIndexFlatTest, Save) {
  butil::Status ok;

  ok = vector_index_flat_l2->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  ok = vector_index_flat_ip->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  ok = vector_index_flat_cosine->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_flat_l2->Save(kTempDataDirectory + "/flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_ip->Save(kTempDataDirectory + "/flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_cosine->Save(kTempDataDirectory + "/flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_flat_l2->Save(kTempDataDirectory + "/flat_l2");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_ip->Save(kTempDataDirectory + "/flat_ip");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_cosine->Save(kTempDataDirectory + "/flat_cosine");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexFlatTest, Load) {
  butil::Status ok;

  ok = vector_index_flat_l2->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  ok = vector_index_flat_ip->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  ok = vector_index_flat_cosine->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_flat_l2->Load(kTempDataDirectory + "/ivf_flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
  ok = vector_index_flat_ip->Load(kTempDataDirectory + "/ivf_flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
  ok = vector_index_flat_cosine->Load(kTempDataDirectory + "/ivf_flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_flat_l2->Load(kTempDataDirectory + "/flat_l2");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_ip->Load(kTempDataDirectory + "/flat_ip");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  ok = vector_index_flat_cosine->Load(kTempDataDirectory + "/flat_cosine");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexFlatTest, SearchAfterLoad) {
  butil::Status ok;

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
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
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    results.clear();
    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    results.clear();
    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
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

    std::vector<int64_t> vector_ids;
    for (int64_t i = 0; i < data_base_size; i++) {
      vector_ids.emplace_back(i);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vector_ids.begin(), vector_ids.end(), g);

    std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
    std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

    std::shared_ptr<VectorIndex::ConcreteFilterFunctor> filter =
        std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_flat()->set_nprobe(10);
    ok = vector_index_flat_l2->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "L2:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results) {
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

    ok = vector_index_flat_ip->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "IP:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results) {
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
      DINGO_LOG(INFO) << "IP : All Id in  vectors ";
    }

    ok = vector_index_flat_cosine->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "COSINE:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    is_all_in_vector = true;
    for (const auto& result : results) {
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
      DINGO_LOG(INFO) << "COSINE : All Id in  vectors ";
    }
  }
}

TEST_F(VectorIndexFlatTest, RangeSearchAfterLoad) {
  butil::Status ok;

  // invalid param failed,
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
    results.clear();

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
    results.clear();
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();

    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  // ok add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    results.clear();
    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }

    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
      LOG(INFO);
    }
  }

  // ok  all with filter
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[cnt * dimension + i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results_l2;
    std::vector<pb::index::VectorWithDistanceResult> results_ip;
    std::vector<pb::index::VectorWithDistanceResult> results_cosine;

    std::vector<int64_t> vector_ids;

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_flat_l2->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_l2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_l2) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);

    ok = vector_index_flat_ip->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results_ip);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_ip) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }

    LOG(INFO);
    ok = vector_index_flat_cosine->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {},
                                               results_cosine);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    {
      int i = 0;
      for (const auto& result : results_cosine) {
        int j = 0;
        LOG(INFO) << "i : " << i;
        for (const auto& vector_with_distance : result.vector_with_distances()) {
          LOG(INFO) << "\tj : " << j << " ";
          LOG(INFO) << vector_with_distance.ShortDebugString();
          j++;
        }
        i++;
      }
    }
    LOG(INFO);
  }
}

}  // namespace dingodb
