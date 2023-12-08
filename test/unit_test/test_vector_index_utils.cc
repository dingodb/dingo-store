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
#include <iomanip>
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
#include "vector/vector_index_utils.h"

namespace dingodb {

class VectorIndexUtilsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(VectorIndexUtilsTest, CalcDistanceEntry) {
  // ok faiss l2
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_FAISS;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_L2;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok faiss ip
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_FAISS;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok faiss cosine
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_FAISS;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_COSINE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok hnsw l2
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_L2;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok hnsw ip
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok hnsw cosine
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_COSINE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceEntry:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceEntry:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceEntry:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // failed invalid param ALGORITHM_NONE
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_NONE;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_COSINE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed invalid param METRIC_TYPE_NONE
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_FAISS;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_NONE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed invalid param METRIC_TYPE_NONE
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_NONE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceEntry:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceEntry:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // ok   op_left_vectors and  op_right_vectors emtpy
  {
    ::dingodb::pb::index::VectorCalcDistanceRequest request;
    pb::index::AlgorithmType algorithm_type = pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_L2;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    request.set_algorithm_type(algorithm_type);
    request.set_metric_type(metric_type);
    request.set_is_return_normlize(is_return_normlize);
    request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
    request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

    butil::Status ok =
        VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(VectorIndexUtilsTest, CalcDistanceByFaiss) {
  // ok metric_type = L2
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_L2;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByFaiss(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                              distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = INNER_PRODUCT
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByFaiss(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                              distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = COSINE
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_COSINE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByFaiss(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                              distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = NONE failed
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_NONE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByFaiss(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                              distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(VectorIndexUtilsTest, CalcDistanceByHnswlib) {
  // ok metric_type = L2
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_L2;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByHnswlib(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                                distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = INNER_PRODUCT
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByHnswlib(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                                distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = COSINE
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_COSINE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByHnswlib(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                                distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok metric_type = NONE failed
  {
    pb::common::MetricType metric_type = pb::common::MetricType::METRIC_TYPE_NONE;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcDistanceByHnswlib(metric_type, op_left_vectors, op_right_vectors, is_return_normlize,
                                                distances, result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(VectorIndexUtilsTest, CalcL2DistanceByFaiss) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcL2DistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcL2DistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcL2DistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcL2DistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcL2DistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcL2DistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcL2DistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcL2DistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcL2DistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcL2DistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcL2DistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcL2DistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, CalcIpDistanceByFaiss) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcIpDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcIpDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcIpDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcIpDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcIpDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcIpDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcIpDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcIpDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcIpDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcIpDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcIpDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcIpDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, CalcCosineDistanceByFaiss) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcCosineDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcCosineDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcCosineDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                    result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcCosineDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcCosineDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcCosineDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcCosineDistanceByFaiss:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcCosineDistanceByFaiss:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcCosineDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                    result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcCosineDistanceByFaiss:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcCosineDistanceByFaiss:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcCosineDistanceByFaiss:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, CalcL2DistanceByHnswlib) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcL2DistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcL2DistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcL2DistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                  result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcL2DistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcL2DistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcL2DistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcL2DistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcL2DistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcL2DistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                  result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcL2DistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcL2DistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcL2DistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, CalcIpDistanceByHnswlib) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcIpDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcIpDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcIpDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                  result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcIpDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcIpDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcIpDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcIpDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcIpDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcIpDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                  result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcIpDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcIpDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcIpDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, CalcCosineDistanceByHnswlib) {
  // ok is_return_normlize = true
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = true;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcCosineDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcCosineDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcCosineDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcCosineDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcCosineDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcCosineDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }

  // ok is_return_normlize = false
  {
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
    google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;
    bool is_return_normlize = false;
    std::vector<std::vector<float>> distances;
    std::vector<::dingodb::pb::common::Vector> result_op_left_vectors;
    std::vector<::dingodb::pb::common::Vector> result_op_right_vectors;

    constexpr uint32_t kDimension = 16;
    size_t op_left_vector_size = 2;
    size_t op_right_vector_size = 3;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    // op left assignment
    for (size_t i = 0; i < op_left_vector_size; i++) {
      ::dingodb::pb::common::Vector op_left_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_left_vector.add_float_values(distrib(rng));
      }
      op_left_vectors.Add(std::move(op_left_vector));
    }

    // op right assignment
    for (size_t i = 0; i < op_right_vector_size; i++) {
      ::dingodb::pb::common::Vector op_right_vector;
      for (uint32_t i = 0; i < kDimension; i++) {
        op_right_vector.add_float_values(distrib(rng));
      }
      op_right_vectors.Add(std::move(op_right_vector));
    }

    // print op left
    std::cout << "CalcCosineDistanceByHnswlib:op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op right
    std::cout << "CalcCosineDistanceByHnswlib:op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    butil::Status ok =
        VectorIndexUtils::CalcCosineDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // print distances
    std::cout << "CalcCosineDistanceByHnswlib:distances : " << std::endl;
    {
      size_t i = 0;
      for (const auto& distance : distances) {
        std::cout << "[" << i << "]"
                  << " ";
        for (auto dis : distance) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << dis << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result left
    std::cout << "CalcCosineDistanceByHnswlib:result_op_left_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_left_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }

    // print op result right
    std::cout << "CalcCosineDistanceByHnswlib:result_op_right_vectors : " << std::endl;
    {
      size_t i = 0;
      for (const auto& vector : result_op_right_vectors) {
        std::cout << "[" << i << "]"
                  << " ";
        for (const auto& elem : vector.float_values()) {
          std::cout << std::setiosflags(std::ios::left) << std::setw(10) << std::setfill(' ') << elem << " ";
        }
        std::cout << std::endl;
        i++;
      }
    }
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcL2DistanceByFaiss) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcL2DistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                  result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcL2DistanceByFaiss:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcL2DistanceByFaiss:left" << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcL2DistanceByFaiss:right" << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcL2DistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcIpDistanceByFaiss) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcIpDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                  result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcIpDistanceByFaiss:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcIpDistanceByFaiss:left" << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcIpDistanceByFaiss:right" << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcIpDistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcCosineDistanceByFaiss) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcCosineDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                      result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcCosineDistanceByFaiss:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcCosineDistanceByFaiss:left" << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcCosineDistanceByFaiss:right" << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcCosineDistanceByFaiss:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcL2DistanceByHnswlib) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcL2DistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                    result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcL2DistanceByHnswlib:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcL2DistanceByHnswlib:left" << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcL2DistanceByHnswlib:right" << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcL2DistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcIpDistanceByHnswlib) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcIpDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                    result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcIpDistanceByHnswlib:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcIpDistanceByHnswlib:left" << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcIpDistanceByHnswlib:right" << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcIpDistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, DoCalcCosineDistanceByHnswlib) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;
    bool is_return_normlize = true;
    float distance = 0.0f;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    butil::Status ok =
        VectorIndexUtils::DoCalcCosineDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distance,
                                                        result_op_left_vectors, result_op_right_vectors);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "DoCalcCosineDistanceByHnswlib:distance:" << distance << std::endl;

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcCosineDistanceByHnswlib:left" << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "DoCalcCosineDistanceByHnswlib:right" << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "DoCalcCosineDistanceByHnswlib:data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, ResultOpVectorAssignment) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data) {
      elem = distrib(rng);
    }

    std::cout << "data : \t\t";
    for (const auto elem : data) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_vectors;
    ::dingodb::pb::common::Vector op_vectors;

    for (const auto elem : data) {
      op_vectors.add_float_values(elem);
    }

    VectorIndexUtils::ResultOpVectorAssignment(result_op_vectors, op_vectors);

    EXPECT_EQ(result_op_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);

    std::cout << "value_type : " << result_op_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, ResultOpVectorAssignmentWrapper) {
  // is_return_normlize false. do nothing
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    bool is_return_normlize = false;

    VectorIndexUtils::ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);

    std::cout << "value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }

  // is_return_normlize true.  result_op_left_vectors not empty.
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    result_op_left_vectors = op_right_vectors;

    bool is_return_normlize = true;

    VectorIndexUtils::ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);

    std::cout << "value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }

  // is_return_normlize true.  result_op_right_vectors not empty.
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    result_op_right_vectors = op_left_vectors;

    bool is_return_normlize = true;

    VectorIndexUtils::ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);

    std::cout << "value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }

  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data_left{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data_left) {
      elem = distrib(rng);
    }

    std::cout << "left_data : \t";
    for (const auto elem : data_left) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    std::array<float, kDimension> data_right{};
    for (auto& elem : data_right) {
      elem = distrib(rng);
    }

    std::cout << "right_data : \t";
    for (const auto elem : data_right) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    ::dingodb::pb::common::Vector result_op_left_vectors;
    ::dingodb::pb::common::Vector result_op_right_vectors;
    ::dingodb::pb::common::Vector op_left_vectors;
    ::dingodb::pb::common::Vector op_right_vectors;

    for (const auto elem : data_left) {
      op_left_vectors.add_float_values(elem);
    }

    for (const auto elem : data_right) {
      op_right_vectors.add_float_values(elem);
    }

    bool is_return_normlize = true;

    VectorIndexUtils::ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize,
                                                      result_op_left_vectors, result_op_right_vectors);

    EXPECT_EQ(result_op_left_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);

    std::cout << "value_type : " << result_op_left_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_left_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_left_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;

    EXPECT_EQ(result_op_right_vectors.value_type(), ::dingodb::pb::common::ValueType::FLOAT);
    std::cout << "value_type : " << result_op_right_vectors.value_type() << std::endl;
    std::cout << "dimension : " << result_op_right_vectors.dimension() << std::endl;
    std::cout << "data : \t\t";
    for (const auto elem : result_op_right_vectors.float_values()) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, NormalizeVectorForFaiss) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data{};

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data) {
      elem = distrib(rng);
    }

    std::cout << "faiss data : \t\t";
    for (const auto elem : data) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    VectorIndexUtils::NormalizeVectorForFaiss(data.data(), kDimension);

    std::cout << "faiss data : \t\t";
    for (const auto elem : data) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

TEST_F(VectorIndexUtilsTest, NormalizeVectorForHnsw) {
  // ok
  {
    constexpr uint32_t kDimension = 16;
    std::array<float, kDimension> data{};
    auto norm_array = data;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    for (auto& elem : data) {
      elem = distrib(rng);
    }

    std::cout << "hnsw data : \t\t";
    for (const auto elem : data) {
      std::cout << std::setw(8) << elem << " ";
    }
    std::cout << std::endl;

    VectorIndexUtils::NormalizeVectorForHnsw(data.data(), kDimension, norm_array.data());

    std::cout << "hnsw norm_array : \t";
    for (const auto elem : norm_array) {
      std::cout << elem << " ";
    }
    std::cout << std::endl;
  }
}

}  // namespace dingodb
