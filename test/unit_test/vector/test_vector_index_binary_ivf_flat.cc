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

static const std::string kTempDataDirectory = "./unit_test/vector_index_binary_ivf_flat";

class VectorIndexBinaryIvfFlatTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kTempDataDirectory);
    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4);
  }

  static void TearDownTestSuite() {
    vector_index_binary_ivf_flat.reset();

    Helper::RemoveAllFileOrDirectory(kTempDataDirectory);
  }

  static void ReCreate() {
    static const pb::common::Range kRange;
    static pb::common::RegionEpoch k_epoch;
    k_epoch.set_conf_version(1);
    k_epoch.set_version(10);
    // valid param
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
      index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(dimension);
      index_parameter.mutable_binary_ivf_flat_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_HAMMING);
      index_parameter.mutable_binary_ivf_flat_parameter()->set_ncentroids(ncentroids);
      vector_index_binary_ivf_flat =
          VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    }
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_binary_ivf_flat;
  inline static faiss::idx_t dimension = 128;
  inline static faiss::idx_t dim = dimension / 8;
  inline static int data_base_size = 100;
  inline static int32_t ncentroids = 10;
  inline static std::vector<uint8_t> data_base;
  inline static int32_t start_id = 1000;
  inline static std::string path = kTempDataDirectory + "/binary_ivf_flat";
  static ThreadPoolPtr vector_index_thread_pool;
};

ThreadPoolPtr VectorIndexBinaryIvfFlatTest::vector_index_thread_pool = nullptr;

TEST_F(VectorIndexBinaryIvfFlatTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch k_epoch;
  k_epoch.set_conf_version(1);
  k_epoch.set_version(10);

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_ivf_flat.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(64);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_ivf_flat.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(64);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_EQ(vector_index_binary_ivf_flat.get(), nullptr);
  }

  // invalid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_HAMMING);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_binary_ivf_flat.get(), nullptr);
  }

  // valid param   ncentroids = 0
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_HAMMING);
        index_parameter.mutable_binary_ivf_flat_parameter()->set_ncentroids(0);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_binary_ivf_flat.get(), nullptr);
  }

  // valid param
  {
    int64_t id = 1;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_HAMMING);
    index_parameter.mutable_binary_ivf_flat_parameter()->set_ncentroids(ncentroids);
    vector_index_binary_ivf_flat =
        VectorIndexFactory::NewBinaryIVFFlat(id, index_parameter, k_epoch, kRange, vector_index_thread_pool);
    EXPECT_NE(vector_index_binary_ivf_flat.get(), nullptr);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, DeleteNoData) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_ivf_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist
  {
    int64_t id = 0;
    std::vector<int64_t> ids;
    ids.push_back(id);
    ok = vector_index_binary_ivf_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_ivf_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(i);
    }
    ok = vector_index_binary_ivf_flat->Delete(ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, SearchNotTrain) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i / 255;
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
      std::string byte_data(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, RangeSearchNotTrain) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i / 255;
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
      std::string byte_data(1, static_cast<char>(value));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 30.0;

    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, AddNotTrain) {
  butil::Status ok;
  auto internal_start_id = start_id;

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(internal_start_id);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
  ReCreate();
}

TEST_F(VectorIndexBinaryIvfFlatTest, NeedToSave) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);
}

TEST_F(VectorIndexBinaryIvfFlatTest, TrainVectorWithId) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i / 255;
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

  // invalid.  no data
  {
    ok = vector_index_binary_ivf_flat->Train(std::vector<pb::common::VectorWithId>{});
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // invalid . not align.
  {
    std::vector<uint8_t> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    data_base_not_align.insert(data_base_not_align.end(), data_base.begin(), data_base.end() - 1);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      for (int j = 0; j < dim; j++) {
        std::string byte_data(1, static_cast<char>(data_base_not_align[i * dim + j]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }
      vector_with_ids.push_back(vector_with_id);
    }

    //not align
    pb::common::VectorWithId vector_with_id;
    for (int j = 0; j < dim-1; j++) {
      std::string byte_data(1, static_cast<char>(data_base_not_align[(data_base_size - 1) * dim + j]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EINTERNAL);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<uint8_t> data_base_too_small = data_base;
    data_base_too_small.insert(data_base_too_small.end(), data_base.begin(), data_base.end() - dim);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < 4; i++) {
      pb::common::VectorWithId vector_with_id;
      for (int j = 0; j < dim; j++) {
        std::string byte_data(1, static_cast<char>(data_base_too_small[i * dim + j]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_ivf_flat->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ReCreate();
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<uint8_t> data_base_too_small = data_base;
    data_base_too_small.insert(data_base_too_small.end(), data_base.begin(), data_base.end() - dim);

    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int i = 0; i < data_base_size - 1; i++) {
      pb::common::VectorWithId vector_with_id;
      for (int j = 0; j < dim; j++) {
        std::string byte_data(1, static_cast<char>(data_base_too_small[i * dim + j]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_ivf_flat->Train(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ReCreate();
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, Train) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0, 255);

    data_base.resize(dim * data_base_size, 0);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dim; j++) data_base[dim * i + j] = distrib(rng);
      data_base[dim * i] += i / 255;
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

  // invalid.  no data
  {
    std::vector<uint8_t> vector_value;
    ok = vector_index_binary_ivf_flat->Train(vector_value);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // invalid . not align.
  {
    std::vector<uint8_t> data_base_not_align = data_base;
    data_base_not_align.resize(data_base.size() - 1);
    ok = vector_index_binary_ivf_flat->Train(data_base_not_align);
  }

  // valid. data_base size < ncentroids . nlist = 1
  {
    std::vector<uint8_t> data_base_too_small = data_base;
    data_base_too_small.resize((ncentroids - 1) * dim);
    ok = vector_index_binary_ivf_flat->Train(data_base_too_small);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ReCreate();
  }

  // valid. but warning. ok
  {
    ok = vector_index_binary_ivf_flat->Train(data_base);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<uint8_t> data_base_warning = data_base;
    data_base_warning.resize(38 * (ncentroids)*dim);
    ok = vector_index_binary_ivf_flat->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    ReCreate();
  }

  // valid. but warning.
  {
    std::vector<uint8_t> data_base_warning = data_base;
    data_base_warning.resize(255 * (ncentroids)*dim);
    ok = vector_index_binary_ivf_flat->Train(data_base_warning);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    // ReCreate();
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, Add) {
  butil::Status ok;
  auto internal_start_id = start_id;

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // one but empty failed
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // one ok
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::VectorWithId vector_with_id;

    vector_with_id.set_id(internal_start_id);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }

    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // others
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 1; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(internal_start_id + id);
      for (size_t i = 0; i < dim; i++) {
        std::string byte_data(1, static_cast<char>(data_base[id * dim + i]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, Delete) {
  butil::Status ok;

  // id not found
  {
    int64_t id = 10000000;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_binary_ivf_flat->Delete(ids);
  }

  // id exist
  {
    int64_t id = start_id;
    std::vector<int64_t> ids;
    ids.push_back(id);
    vector_index_binary_ivf_flat->Delete(ids);
  }

  // id exist batch
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_binary_ivf_flat->Delete(ids);
  }

  // id exist batch again
  {
    std::vector<int64_t> ids;
    for (size_t i = 0; i < data_base_size; i++) {
      ids.push_back(start_id + i);
    }
    vector_index_binary_ivf_flat->Delete(ids);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, Upsert) {
  butil::Status ok;

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dim; i++) {
        std::string byte_data(1, static_cast<char>(data_base[id * dim + i]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_ivf_flat->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty OK
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    ok = vector_index_binary_ivf_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // update all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(start_id + id);
      for (size_t i = 0; i < dim; i++) {
        std::string byte_data(1, static_cast<char>(data_base[id * dim + i]));
        vector_with_id.mutable_vector()->add_binary_values(byte_data);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_binary_ivf_flat->Upsert(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;

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

    std::shared_ptr<VectorIndex::ConcreteFilterFunctor> filter =
        std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_flat()->set_nprobe(10);
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << " Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << " All Id in  vectors ";
    }
  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, RangeSearch) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results :" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }
  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
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

    std::shared_ptr<VectorIndex::ConcreteFilterFunctor> filter =
        std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_flat()->set_nprobe(10);
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
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

TEST_F(VectorIndexBinaryIvfFlatTest, NeedToSaveAfterAdd) {
  int64_t last_save_log_behind = 0;
  bool ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 1000;
  ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_FALSE(ok);

  last_save_log_behind = 10000000;
  ok = vector_index_binary_ivf_flat->NeedToSave(last_save_log_behind);
  EXPECT_TRUE(ok);
}

TEST_F(VectorIndexBinaryIvfFlatTest, NeedToRebuild) { bool b1 = vector_index_binary_ivf_flat->NeedToRebuild(); }

TEST_F(VectorIndexBinaryIvfFlatTest, NeedTrain) { EXPECT_TRUE(vector_index_binary_ivf_flat->NeedTrain()); }

TEST_F(VectorIndexBinaryIvfFlatTest, IsTrained) { EXPECT_TRUE(vector_index_binary_ivf_flat->IsTrained()); }

TEST_F(VectorIndexBinaryIvfFlatTest, Save) {
  butil::Status ok;

  ok = vector_index_binary_ivf_flat->Save("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_binary_ivf_flat->Save(kTempDataDirectory + "/ivf_flat");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

  ok = vector_index_binary_ivf_flat->Save(path);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexBinaryIvfFlatTest, Load) {
  butil::Status ok;

  ok = vector_index_binary_ivf_flat->Load("");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = vector_index_binary_ivf_flat->Load(kTempDataDirectory + "/ivf_flat_not_exist");
  EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);

  ok = vector_index_binary_ivf_flat->Load(path);
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
}

TEST_F(VectorIndexBinaryIvfFlatTest, SearchAfterLoad) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 0;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);


    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    uint32_t topk = 3;
    std::vector<pb::index::VectorWithDistanceResult> results;
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

    std::shared_ptr<VectorIndex::ConcreteFilterFunctor> filter =
        std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_flat()->set_nprobe(10);
    ok = vector_index_binary_ivf_flat->Search(vector_with_ids, topk, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
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
      DINGO_LOG(INFO) << "All Id in  vectors ";
    }

  }
}

TEST_F(VectorIndexBinaryIvfFlatTest, RangeSearchAfterLoad) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_INVALID);

  }

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;

    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);


    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

  }

  // ok with param
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    for (size_t i = 0; i < dim; i++) {
      std::string byte_data(1, static_cast<char>(data_base[i]));
      vector_with_id.mutable_vector()->add_binary_values(byte_data);
    }
    float radius = 1.1F;
    std::vector<pb::index::VectorWithDistanceResult> results;
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

    std::shared_ptr<VectorIndex::ConcreteFilterFunctor> filter =
        std::make_shared<VectorIndex::ConcreteFilterFunctor>(std::move(vector_select_ids));
    const bool reconstruct = false;
    pb::common::VectorSearchParameter parameter;
    parameter.mutable_ivf_flat()->set_nprobe(10);
    ok = vector_index_binary_ivf_flat->RangeSearch(vector_with_ids, radius, {filter}, false, parameter, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (const auto& result : results) {
      DINGO_LOG(INFO) << "results:" << result.DebugString();
      DINGO_LOG(INFO) << "";
    }

    bool is_all_in_vector = true;
    for (const auto& result : results) {
      for (const auto& distance : result.vector_with_distances()) {
        auto id = distance.vector_with_id().id();
        auto iter = std::find(vector_select_ids_clone.begin(), vector_select_ids_clone.end(), id);
        if (iter == vector_select_ids_clone.end()) {
          DINGO_LOG(INFO) << "results : Not Find id : " << id;
          is_all_in_vector = false;
        }
      }
    }
    if (is_all_in_vector) {
      DINGO_LOG(INFO) << "results : All Id in  vectors ";
    }
  }
}

}  // namespace dingodb
