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
#include <memory>
#include <random>
#include <sstream>
#include <vector>

#include "butil/status.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"

DEFINE_uint32(vector_index_flat_simd_test_dimension, 512, "vector index flat simd test dimension. default 512");
// DEFINE_uint32(vector_index_flat_simd_test_data_base_size, 1000000,
DEFINE_uint32(vector_index_flat_simd_test_data_base_size, 1000,
              "vector_index_flat_simd_test_data_base_size. default 1000000");

DEFINE_bool(vector_index_flat_simd_test_enable_simd, false, "vector_index_flat_simd_test_enable_simd. default false");

namespace dingodb {

class VectorIndexFlatSimdTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 1);
    dimension = FLAGS_vector_index_flat_simd_test_dimension;
    data_base_size = FLAGS_vector_index_flat_simd_test_data_base_size;
    if (FLAGS_vector_index_flat_simd_test_enable_simd) {
      enable_simd = FLAGS_vector_index_flat_simd_test_enable_simd;
      VectorIndex::SetSimdHookForFaiss();
    }

    LOG(INFO) << fmt::format(
        "vector_index_flat_simd_test_dimension: {} vector_index_flat_simd_test_data_base_size: {} "
        "vector_index_flat_simd_test_enable_simd : {}",
        FLAGS_vector_index_flat_simd_test_dimension, FLAGS_vector_index_flat_simd_test_data_base_size,
        FLAGS_vector_index_flat_simd_test_enable_simd ? "true" : "false");
  }

  static void TearDownTestSuite() {
    vector_index_flat_l2.reset();
    vector_index_flat_ip.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_ip;
  // inline static faiss::idx_t dimension = 512;
  // inline static int data_base_size = 1000000;
  inline static faiss::idx_t dimension = FLAGS_vector_index_flat_simd_test_dimension;
  inline static int data_base_size = FLAGS_vector_index_flat_simd_test_data_base_size;
  inline static bool enable_simd = FLAGS_vector_index_flat_simd_test_enable_simd;
  inline static std::vector<float> data_base;
  inline static ThreadPoolPtr vector_index_thread_pool;
};

TEST_F(VectorIndexFlatSimdTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

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
}

TEST_F(VectorIndexFlatSimdTest, Add) {
  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
    }

#if false
    for (size_t i = 0; i < data_base_size; i++) {
      std::stringstream ss;
      ss << "[" << i << "]"
         << " [";

      for (faiss::idx_t j = 0; j < dimension; j++) {
        if (0 != j) {
          ss << ",";
        }
        ss << std::setw(10) << data_base[i * dimension + j];
      }

      ss << "]";
      LOG(INFO) << ss.str();
    }
#endif
  }

  // add all
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;

    for (size_t id = 0; id < data_base_size; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id + data_base_size);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[id * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    ok = vector_index_flat_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_flat_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexFlatSimdTest, SearchPrint) {
  butil::Status ok;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto lambda_get_instruction_function = []() {
#if defined(__AVX512F__)
    return "avx512";
#elif defined(__AVX2__)
    return "avx2";
#else
    return "general";
#endif
  };

  auto lambda_search_function = [&lambda_time_now_function, &lambda_time_diff_microseconds_function,
                                 &lambda_get_instruction_function](std::shared_ptr<VectorIndex> vector_index_flat,
                                                                   const char *name, int n, uint32_t topk) {
    butil::Status ok;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int id = 0; id < n; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[i + id * dimension];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = vector_index_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << fmt::format("{} search {} dimension: {} total : {} topk : {:<3} n : {:<3}",
                             lambda_get_instruction_function(), name, dimension, data_base_size, topk, n);

    for (const auto &vector_with_distance_result : results) {
      for (const auto &distance : vector_with_distance_result.vector_with_distances()) {
        LOG(INFO) << distance.DebugString() << "\n";
      }
    }
  };

  auto lambda_search_function_wrapper = [&lambda_search_function](std::shared_ptr<VectorIndex> vector_index_flat,
                                                                  const char *name) {
    {
      int n = 1;
      uint32_t k = 3;
      lambda_search_function(vector_index_flat, name, n, k);
    }
  };

  lambda_search_function_wrapper(vector_index_flat_l2, "flat l2");

  lambda_search_function_wrapper(vector_index_flat_ip, "flat ip");
}

TEST_F(VectorIndexFlatSimdTest, Search) {
  butil::Status ok;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto lambda_get_instruction_function = []() {
#if defined(__AVX512F__)
    return "avx512";
#elif defined(__AVX2__)
    return "avx2";
#else
    return "general";
#endif
  };

  auto lambda_search_function = [&lambda_time_now_function, &lambda_time_diff_microseconds_function,
                                 &lambda_get_instruction_function](std::shared_ptr<VectorIndex> vector_index_flat,
                                                                   const char *name, int n, uint32_t topk) {
    butil::Status ok;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    for (int id = 0; id < n; id++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id + data_base_size);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[i + id * dimension];
        vector_with_id.mutable_vector()->add_float_values(value);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    std::vector<pb::index::VectorWithDistanceResult> results;

    auto start = lambda_time_now_function();

    ok = vector_index_flat->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    LOG(INFO) << fmt::format("{} search {} dimension: {} total : {} topk : {:<3} n : {:<3} time cost : {} (us)",
                             lambda_get_instruction_function(), name, dimension, data_base_size, topk, n,
                             lambda_time_diff_microseconds_function(start, end));
  };

  auto lambda_search_function_wrapper = [&lambda_search_function](std::shared_ptr<VectorIndex> vector_index_flat,
                                                                  const char *name) {
    {
      int n = 1;
      uint32_t k = 3;
      lambda_search_function(vector_index_flat, name, n, k);
    }

    // topk = 1, n = 20   enable blas . enable
    // exhaustive_L2sqr_blas_default_impl AVX d =[1, 16], d = AVX512[1, 32]
    // IP : 仅仅开启 blas 优化, 未使用CPU 指令级优化。
    {
      int k = 1;
      int n = 20;
      lambda_search_function(vector_index_flat, name, n, k);
    }

    {
      // topk = 99, n = 1 normal
      int k = 99;
      int n = 1;
      lambda_search_function(vector_index_flat, name, n, k);
    }

    {
      // topk = 99, n = 20 normal . enable blas
      int k = 99;
      int n = 20;
      lambda_search_function(vector_index_flat, name, n, k);
    }

    {
      // topk = 100, n = 1 normal
      int k = 100;
      int n = 1;
      lambda_search_function(vector_index_flat, name, n, k);
    }

    {
      // topk = 100, n = 20. enable blas
      int k = 100;
      int n = 20;
      lambda_search_function(vector_index_flat, name, n, k);
    }
  };

  lambda_search_function_wrapper(vector_index_flat_l2, "flat l2");

  lambda_search_function_wrapper(vector_index_flat_ip, "flat ip");
}
}  // namespace dingodb
