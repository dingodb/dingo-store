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
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "butil/status.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_raw_ivf_pq.h"

namespace dingodb {

class VectorIndexRawIvfPqTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {
    vector_index_raw_ivf_pq_l2.reset();
    vector_index_raw_ivf_pq_ip.reset();
    vector_index_raw_ivf_pq_cosine.reset();
  }

  static void ReCreate() {
    static const pb::common::Range kRange;
    // valid param IP
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_raw_ivf_pq_ip = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
    }

    // valid param L2
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_raw_ivf_pq_l2 = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
    }

    // valid param cosine
    {
      int64_t id = 1;
      pb::common::VectorIndexParameter index_parameter;
      index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      index_parameter.mutable_ivf_pq_parameter()->set_dimension(dimension);
      index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
          ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
      index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(ncentroids);
      index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(nsubvector);
      index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(nbits_per_idx);
      vector_index_raw_ivf_pq_cosine = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
    }
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_raw_ivf_pq_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_raw_ivf_pq_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_raw_ivf_pq_cosine;
  inline static faiss::idx_t dimension = 4096;
  // inline static int data_base_size = 100000;
  inline static int data_base_size = 10000;
  inline static int32_t ncentroids = 100;
  inline static size_t nsubvector = 8;
  inline static int32_t nbits_per_idx = 8;
  inline static std::vector<float> data_base;
  inline static int32_t start_id = 1000;
};

TEST_F(VectorIndexRawIvfPqTest, Create) {
  butil::Status ok;

  std::ofstream outfile;
  const std::string &file_path = "./test_vector_index_raw_ivf_pq_boundary.txt";
  outfile.open(file_path);  // 打开文件
  if (!outfile) {
    std::cout << "open file failed " << file_path << std::endl;
    exit(1);
  }

  for (int internal_dimension = 1; internal_dimension <= 1024; internal_dimension++) {
    // create random data
    {
      std::mt19937 rng;
      std::uniform_real_distribution<> distrib;

      data_base.resize(internal_dimension * data_base_size, 0.0f);

      for (int i = 0; i < data_base_size; i++) {
        for (int j = 0; j < internal_dimension; j++) {
          data_base[internal_dimension * i + j] = distrib(rng);
        }
      }
    }

    std::cout << fmt::format("create random data complete!!! data_base_size:{}  dimension:{}", data_base_size,
                             internal_dimension)
              << '\n';
    for (int internal_nbits_per_idx = 1; internal_nbits_per_idx <= 64; internal_nbits_per_idx++) {
      for (int internal_nsubvector = 1; internal_nsubvector <= internal_dimension; internal_nsubvector++)
        for (int internal_ncentroids = 1; internal_ncentroids < 1000; internal_ncentroids++) {
          static const pb::common::Range kRange;

          std::array<std::pair<std::string, bool>, 3> error_flags{std::pair<std::string, bool>{"l2", false},
                                                                  std::pair<std::string, bool>{"ip", false},
                                                                  std::pair<std::string, bool>{"cosine", false}};

          std::shared_ptr<VectorIndex> raw_ivf_pq_l2;
          std::shared_ptr<VectorIndex> raw_ivf_pq_ip;
          std::shared_ptr<VectorIndex> raw_ivf_pq_cosine;

          // valid param IP
          {
            int64_t id = 1;
            pb::common::VectorIndexParameter index_parameter;
            index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
            index_parameter.mutable_ivf_pq_parameter()->set_dimension(internal_dimension);
            index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
                ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
            index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(internal_ncentroids);
            index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(internal_nsubvector);
            index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(internal_nbits_per_idx);
            raw_ivf_pq_ip = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
          }

          // valid param L2
          {
            int64_t id = 1;
            pb::common::VectorIndexParameter index_parameter;
            index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
            index_parameter.mutable_ivf_pq_parameter()->set_dimension(internal_dimension);
            index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
                ::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
            index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(internal_ncentroids);
            index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(internal_nsubvector);
            index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(internal_nbits_per_idx);
            raw_ivf_pq_l2 = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
          }

          // valid param cosine
          {
            int64_t id = 1;
            pb::common::VectorIndexParameter index_parameter;
            index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
            index_parameter.mutable_ivf_pq_parameter()->set_dimension(internal_dimension);
            index_parameter.mutable_ivf_pq_parameter()->set_metric_type(
                ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
            index_parameter.mutable_ivf_pq_parameter()->set_ncentroids(internal_ncentroids);
            index_parameter.mutable_ivf_pq_parameter()->set_nsubvector(internal_nsubvector);
            index_parameter.mutable_ivf_pq_parameter()->set_nbits_per_idx(internal_nbits_per_idx);
            raw_ivf_pq_cosine = std::make_shared<VectorIndexRawIvfPq>(id, index_parameter, kRange);
          }

          if (data_base.empty()) {
            continue;
          }

          std::vector<float> internal_data_base = data_base;
          if (!internal_data_base.empty()) {
            if (internal_dimension != 0) {
              internal_data_base.resize(data_base.size() / internal_dimension * internal_dimension);
            }
          }

          auto lambda_set_error = [&](const std::string &name, bool flag) {
            for (auto &[internal_name, internal_flag] : error_flags) {
              if (name == internal_name) {
                internal_flag = flag;
                break;
              }
            }
          };

          auto lambda_get_error = [&](const std::string &name) {
            for (auto &[internal_name, internal_flag] : error_flags) {
              if (name == internal_name) {
                return internal_flag;
              }
            }
            return true;
          };

          auto lambda_clear_error = [&]() {
            for (auto &[internal_name, internal_flag] : error_flags) {
              internal_flag = false;
            }
          };

          auto lambda_output_info = [&](const std::string &s) {
            bool all_ok = true;
            for (auto &[internal_name, internal_flag] : error_flags) {
              if (internal_flag) {
                all_ok = false;
                break;
              }
            }

            if (all_ok) {
              std::cout << s << " success "
                        << "\n"
                        << "\n";
              outfile << s << " success "
                      << "\n"
                      << "\n";
              outfile.flush();

            } else {
              std::cout << s << " failed "
                        << "\n"
                        << "\n";
              outfile << s << " failed "
                      << "\n"
                      << "\n";
              outfile.flush();
            }
          };

          lambda_clear_error();

          auto lambda_train = [&](const std::string &name, std::shared_ptr<VectorIndex> raw_ivf) {
            if (lambda_get_error(name)) {
              return;
            }
            butil::Status ok = raw_ivf->Train(internal_data_base);
            if (!ok.ok()) {
              std::cout << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} train failed",
                                       internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                       internal_ncentroids, name)
                        << "\n"
                        << "\n";

              outfile << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} train failed",
                                     internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                     internal_ncentroids, name)
                      << "\n"
                      << "\n";
              outfile.flush();
              lambda_set_error(name, true);
              return;
            }
          };

          std::vector<std::thread> vt_train_threads;

          vt_train_threads.emplace_back(lambda_train, "l2", raw_ivf_pq_l2);
          vt_train_threads.emplace_back(lambda_train, "ip", raw_ivf_pq_ip);
          vt_train_threads.emplace_back(lambda_train, "cosine", raw_ivf_pq_cosine);

          for (auto &t : vt_train_threads) {
            t.join();
          }

          std::vector<pb::common::VectorWithId> vector_with_ids;

          for (size_t id = 0; id < data_base_size; id++) {
            pb::common::VectorWithId vector_with_id;

            vector_with_id.set_id(data_base_size + id);
            for (size_t i = 0; i < internal_dimension; i++) {
              vector_with_id.mutable_vector()->add_float_values(data_base[id * internal_dimension + i]);
            }

            vector_with_ids.push_back(vector_with_id);
          }

          auto lambda_add = [&](const std::string &name, std::shared_ptr<VectorIndex> raw_ivf) {
            if (lambda_get_error(name)) {
              return;
            }
            butil::Status ok = raw_ivf->Add(vector_with_ids);
            if (!ok.ok()) {
              std::cout << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} add failed",
                                       internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                       internal_ncentroids, name)
                        << "\n"
                        << "\n";
              outfile << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} add failed",
                                     internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                     internal_ncentroids, name)
                      << "\n"
                      << "\n";
              lambda_set_error(name, true);
              return;
            }
          };

          std::vector<std::thread> vt_add_threads;

          vt_add_threads.emplace_back(lambda_add, "l2", raw_ivf_pq_l2);
          vt_add_threads.emplace_back(lambda_add, "ip", raw_ivf_pq_ip);
          vt_add_threads.emplace_back(lambda_add, "cosine", raw_ivf_pq_cosine);

          for (auto &t : vt_add_threads) {
            t.join();
          }

          uint32_t topk = 3;

          std::vector<int64_t> vector_ids;
          for (int64_t i = 0; i < data_base_size; i++) {
            vector_ids.emplace_back(i + data_base_size);
          }

          std::random_device rd;
          std::mt19937 g(rd());
          std::shuffle(vector_ids.begin(), vector_ids.end(), g);

          std::vector<int64_t> vector_select_ids(vector_ids.begin(), vector_ids.begin() + (data_base_size / 2));
          // std::vector<int64_t> vector_select_ids_clone = vector_select_ids;

          std::shared_ptr<VectorIndex::IvfPqListFilterFunctor> filter =
              std::make_shared<VectorIndex::IvfPqListFilterFunctor>(vector_select_ids);
          bool reconstruct = false;
          pb::common::VectorSearchParameter parameter;
          parameter.mutable_ivf_pq()->set_nprobe(1);
          std::vector<pb::common::VectorWithId> vector_with_ids_clone(vector_with_ids.begin(),
                                                                      vector_with_ids.begin() + 1);

          auto lambda_search = [&](const std::string &name, std::shared_ptr<VectorIndex> raw_ivf) {
            if (lambda_get_error(name)) {
              return;
            }
            std::vector<pb::index::VectorWithDistanceResult> results;
            butil::Status ok = raw_ivf->Search(vector_with_ids_clone, topk, {filter}, results, false, parameter);
            if (!ok.ok()) {
              std::cout << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} search failed",
                                       internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                       internal_ncentroids, name)
                        << "\n"
                        << "\n";

              outfile << fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{} {} search failed",
                                     internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                     internal_ncentroids, name)
                      << "\n"
                      << "\n";
              outfile.flush();
              lambda_set_error(name, true);
              return;
            }
          };
          std::vector<std::thread> vt_search_threads;

          vt_search_threads.emplace_back(lambda_search, "l2", raw_ivf_pq_l2);
          vt_search_threads.emplace_back(lambda_search, "ip", raw_ivf_pq_ip);
          vt_search_threads.emplace_back(lambda_search, "cosine", raw_ivf_pq_cosine);

          for (auto &t : vt_search_threads) {
            t.join();
          }

          lambda_output_info(fmt::format("dimension : {} nbits_per_idx : {} nsubvector:{} ncentroids:{}",
                                         internal_dimension, internal_nbits_per_idx, internal_nsubvector,
                                         internal_ncentroids));

          outfile.flush();
        }
    }
  }

  outfile << "normal exit!!!" << std::endl;
  outfile.close();
}

}  // namespace dingodb
