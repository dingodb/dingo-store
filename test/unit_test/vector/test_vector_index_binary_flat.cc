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

#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
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

static const std::string kTempDataDirectory = "./unit_test/vector_index_binary_flat";

class VectorIndexBinaryFlatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kTempDataDirectory);
    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4);
  }

  static void TearDownTestSuite() {
    vector_index_binary_flat.reset();
    Helper::RemoveAllFileOrDirectory(kTempDataDirectory);
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_binary_flat;
  inline static faiss::idx_t dimension = 16;
  inline static faiss::idx_t dim = 2;

  inline static int data_base_size = 10;
  inline static std::vector<uint8_t> data_base;

  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexBinaryFlatTest::vector_index_thread_pool =
    std::make_shared<ThreadPool>("vector_index_binary_flat", 4);

TEST_F(VectorIndexBinaryFlatTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT);
    vector_index_binary_flat =
        VectorIndexFactory::NewBinaryFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_flat.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT);
    index_parameter.mutable_binary_flat_parameter()->set_dimension(dimension);
    vector_index_binary_flat =
        VectorIndexFactory::NewBinaryFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_flat.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT);
    index_parameter.mutable_binary_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_binary_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_binary_flat =
        VectorIndexFactory::NewBinaryFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_flat.get(), nullptr);
  }

  // valid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT);
    index_parameter.mutable_binary_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_binary_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_HAMMING);
    vector_index_binary_flat =
        VectorIndexFactory::NewBinaryFlat(id, index_parameter, kEpoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_binary_flat.get(), nullptr);
  }
}

TEST_F(VectorIndexBinaryFlatTest, DeleteNoData) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryFlatTest, SearchNoData) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
      }

      LOG(INFO) << "]";
    }
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryFlatTest, RangeSearchNoData) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
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
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[cnt * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  // ok  all with filter
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[cnt * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<int64_t> vector_ids;

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results);
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
  }
}

TEST_F(VectorIndexBinaryFlatTest, NeedToSave) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
}

TEST_F(VectorIndexBinaryFlatTest, Add) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
      }

      LOG(INFO) << "]";
    }
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(0 + data_base_size);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1 + data_base_size; id < data_base_size + data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[id * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryFlatTest, NeedToSaveAfterAdd) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_binary_flat->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
}

TEST_F(VectorIndexBinaryFlatTest, Delete) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist
  {
    int64_t id = 0 + data_base_size;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0 + data_base_size; i < data_base_size + data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0 + data_base_size; i < data_base_size + data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexBinaryFlatTest, UpsertWithDuplicated) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
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
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[id * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    for (size_t id = 0 + data_base_size; id < data_base_size + data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[id * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_ID_DUPLICATED);
  }
}

TEST_F(VectorIndexBinaryFlatTest, Upsert) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dim * data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
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
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[id * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_binary_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // create random data again
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dim * data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i % 255;
    }

    for (size_t i = 0; i < data_base_size; i++) {
      LOG(INFO) << "[" << i << "]" << " [";
      for (faiss::idx_t j = 0; j < dim; j++) {
        if (0 != j) {
          LOG(INFO) << ",";
        }
        LOG(INFO) << std::setw(10) << data_base[i * dim + j];
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
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[id * dim + i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryFlatTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {}, false, {}, results);
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

TEST_F(VectorIndexBinaryFlatTest, RangeSearch) {
  butil::Status ok;

  // invalid param failed,
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  // ok add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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

    // ok  all with filter
    {
      std::vector<pb::common::VectorWithId> vector_with_ids;

      for (int cnt = 0; cnt < data_base_size; cnt++) {
        pb::common::VectorWithId vector_with_id;
        vector_with_id.set_id(cnt + data_base_size);
        vector_with_id.mutable_vector()->set_dimension(dimension);
        vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
        for (size_t i = 0; i < dim; i++) {
          uint8_t value = data_base[i];
          std::string byte_to_str(1, static_cast<char>(value));
          vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
        }

        vector_with_ids.push_back(vector_with_id);
      }

      float radius = 10.1F;
      std::vector<pb::index::VectorWithDistanceResult> results;

      std::vector<int64_t> vector_ids;

      vector_ids.reserve(data_base_size / 2);
      for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
        vector_ids.push_back(cnt + data_base_size);
      }

      auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

      ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {},
                                                 results);
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
    }
  }
}

TEST_F(VectorIndexBinaryFlatTest, Save) {
  butil::Status ok;

  ok = vector_index_binary_flat->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_binary_flat->Save(kTempDataDirectory + "/flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_binary_flat->Save(kTempDataDirectory + "/flat_bin");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexBinaryFlatTest, Load) {
  butil::Status ok;

  ok = vector_index_binary_flat->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_binary_flat->Load(kTempDataDirectory + "/ivf_flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_binary_flat->Load(kTempDataDirectory + "/flat_bin");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexBinaryFlatTest, SearchAfterLoad) {
  butil::Status ok;

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    results.clear();
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
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
    ok = vector_index_binary_flat->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << " All Id in  vectors ";
    }
  }
}

TEST_F(VectorIndexBinaryFlatTest, RangeSearchAfterLoad) {
  butil::Status ok;

  // invalid param failed,
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  //  return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0 + data_base_size);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      uint8_t value = data_base[i];
      std::string byte_to_str(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
    }
    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  // ok add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
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
  }

  // ok  all with filter
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (int cnt = 0; cnt < data_base_size; cnt++) {
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(cnt + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
      for (size_t i = 0; i < dim; i++) {
        uint8_t value = data_base[i];
        std::string byte_to_str(1, static_cast<char>(value));
        vector_with_id.mutable_vector()->add_binary_values(byte_to_str);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    float radius = 10.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<int64_t> vector_ids;

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_binary_flat->RangeSearch(vector_with_ids, radius, {flat_list_filter_functor}, false, {}, results);
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
  }
}

}  // namespace dingodb
