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
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "butil/type_traits.h"
#include "common/logging.h"
#include "diskann/diskann_item.h"
#include "diskann/diskann_utils.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

DEFINE_string(diskann_item_ann_test_type, "sift", "diskann data set type. default sift");
DEFINE_string(diskann_item_ann_test_path, "./data.fvecs", "diskann data set path. default ./data.fvecs");

namespace dingodb {

class DiskANNItemAnnTest;
static void DoPreCreate(DiskANNItemAnnTest &diskann_item_test);
static void DoCreate(DiskANNItemAnnTest &diskann_item_test);

class DiskANNItemAnnTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    DiskANNItem::SetBaseDir(base_dir);
    ctx = std::make_shared<Context>();
    brpc::Controller cntl;
    ctx->SetCntl(&cntl);
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  friend void DoPreCreate(DiskANNItemAnnTest &diskann_item_test);
  friend void DoCreate(DiskANNItemAnnTest &diskann_item_test);

  inline static faiss::idx_t dimension = 128;
  inline static int data_base_size = 1000;
  inline static int data_base_real_size = 100;
  inline static std::vector<float> data_base;

  inline static std::shared_ptr<DiskANNItem> disk_ann_item_l2;
  inline static std::shared_ptr<DiskANNItem> disk_ann_item_ip;
  inline static std::shared_ptr<DiskANNItem> disk_ann_item_cosine;
  inline static int64_t disk_ann_item_l2_id = 1;
  inline static int64_t disk_ann_item_ip_id = 2;
  inline static int64_t disk_ann_item_cosine_id = 3;
  inline static uint32_t num_threads = 64;
  inline static float search_dram_budget_gb = 50.0f;
  inline static float build_dram_budget_gb = 100.0f;
  inline static std::shared_ptr<Context> ctx;
  inline static std::string base_dir = ".";
  inline static uint32_t import_batch_num = 1024;
  inline static int64_t start_vector_id = 1;
};

TEST_F(DiskANNItemAnnTest, ClearAll) {
  DiskANNUtils::CreateDir(DiskANNItemAnnTest::base_dir);
  DiskANNUtils::RemoveDir(DiskANNItemAnnTest::base_dir + "/" + "tmp");
  DiskANNUtils::RemoveDir(DiskANNItemAnnTest::base_dir + "/" + "destroyed");
  DiskANNUtils::RemoveDir(DiskANNItemAnnTest::base_dir + "/" + "normal");
  DiskANNUtils::RemoveDir(DiskANNItemAnnTest::base_dir + "/" + "blackhole");
}

static void DoPreCreate(DiskANNItemAnnTest &diskann_item_test) {
  if (FLAGS_diskann_item_ann_test_type == std::string("sift")) {
    DINGO_LOG(INFO) << fmt::format("use sift data set");
    DINGO_LOG(INFO) << fmt::format("use file : {}", FLAGS_diskann_item_ann_test_path);
    try {
      std::ifstream reader(FLAGS_diskann_item_ann_test_path.c_str(), std::ios::binary);
      uint32_t dim = 0;

      if (reader.fail()) {
        std::string s = fmt::format("open file error : {} {}", FLAGS_diskann_item_ann_test_path, strerror(errno));
        DINGO_LOG(ERROR) << s;
        exit(-1);
      }

      reader.read(reinterpret_cast<char *>(&dim), sizeof(uint32_t));

      if (0 == dim) {
        std::string s = fmt::format("dimension is 0. not support ");
        DINGO_LOG(ERROR) << s;
        exit(-1);
      }
      diskann_item_test.dimension = dim;
      DINGO_LOG(INFO) << fmt::format("dimension : {}", dim);
      DINGO_LOG(INFO) << fmt::format("cost sift dimension : {} file : {}", dim, FLAGS_diskann_item_ann_test_path);
      reader.close();
    } catch (const std::exception &e) {
      std::string s = fmt::format("read error : {} {}", FLAGS_diskann_item_ann_test_path, e.what());
      DINGO_LOG(ERROR) << s;
      exit(-1);
    }
  } else {
    DINGO_LOG(ERROR) << "only support sift data set";
    exit(-1);
  }
}

static void DoCreate(DiskANNItemAnnTest &diskann_item_test) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  ////////////////////////////////////////////////diskann//////////////////////////////////////////////////////////////////////////
  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    diskann_item_test.disk_ann_item_l2 = std::make_shared<DiskANNItem>(
        diskann_item_test.ctx, diskann_item_test.disk_ann_item_l2_id, index_parameter, diskann_item_test.num_threads,
        diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

    EXPECT_NE(diskann_item_test.disk_ann_item_l2.get(), nullptr);
  }

  // diskann valid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    diskann_item_test.disk_ann_item_ip = std::make_shared<DiskANNItem>(
        diskann_item_test.ctx, diskann_item_test.disk_ann_item_ip_id, index_parameter, diskann_item_test.num_threads,
        diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

    EXPECT_NE(diskann_item_test.disk_ann_item_l2.get(), nullptr);
  }

  // diskann valid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(diskann_item_test.dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    diskann_item_test.disk_ann_item_cosine = std::make_shared<DiskANNItem>(
        diskann_item_test.ctx, diskann_item_test.disk_ann_item_cosine_id, index_parameter,
        diskann_item_test.num_threads, diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

    EXPECT_NE(diskann_item_test.disk_ann_item_l2.get(), nullptr);
  }
}

TEST_F(DiskANNItemAnnTest, PreCreate) { DoPreCreate(*this); }

TEST_F(DiskANNItemAnnTest, Create) { DoCreate(*this); }

static bool ReadLine(std::ifstream &reader, faiss::idx_t dimension, std::vector<float> &data, bool &is_eof) {
  std::vector<uint8_t> buffer;
  int64_t already_size = 0;
  size_t buffer_size = dimension * sizeof(float) + sizeof(uint32_t);
  buffer.resize(buffer_size);

  try {
    is_eof = false;
    while (true) {
      reader.read(reinterpret_cast<char *>(buffer.data()) + already_size, buffer_size - already_size);

      int64_t num = reader.gcount();

      already_size += num;

      if (already_size == buffer_size) {
        break;
      }

      if (reader.eof()) {
        is_eof = true;
        reader.close();
        return true;
      }
    }
    uint32_t dim = *(reinterpret_cast<uint32_t *>(buffer.data()));

    if (dim != static_cast<uint32_t>(dimension)) {
      std::string s = fmt::format("dimension is 0. not support ");
      DINGO_LOG(ERROR) << s;
      return false;
    }

    data.reserve(static_cast<size_t>(dimension));
    for (size_t i = 0; i < dimension; i++) {
      float f = *(reinterpret_cast<float *>(buffer.data() + sizeof(uint32_t) + i * sizeof(float)));
      data.push_back(f);
    }

  } catch (const std::exception &e) {
    std::string s = fmt::format("read error : {} {}", FLAGS_diskann_item_ann_test_path, e.what());
    DINGO_LOG(ERROR) << s;
    return false;
  }
  return true;
}

template <typename T>
static std::string OutputCostString(const std::string &index_type, const std::string &diskann_func, T cost) {
  std::string str = fmt::format("\tdisk_ann_item_{}\t{}\ttime_cost : {}\tmicroseconds", index_type, diskann_func, cost);
  return str;
}

TEST_F(DiskANNItemAnnTest, Import) {
  butil::Status status;
  std::ifstream reader(FLAGS_diskann_item_ann_test_path.c_str(), std::ios::binary);
  std::vector<float> data;
  bool is_eof;
  std::vector<pb::common::Vector> vectors;
  std::vector<int64_t> vector_ids;
  int ts = 1;
  int tso = 10;
  int64_t already_send_vector_count = 0;
  int64_t already_recv_vector_count = 0;
  int64_t internal_start_vector_id = start_vector_id;
  bool has_more = true;
  bool force_to_load_data_if_exist = false;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto lambda_import_function = [&vectors, &vector_ids, &has_more, &ts, &tso, force_to_load_data_if_exist,
                                 &already_send_vector_count,
                                 &already_recv_vector_count](std::shared_ptr<DiskANNItem> disk_ann_item_l2,
                                                             std::shared_ptr<DiskANNItem> disk_ann_item_ip,
                                                             std::shared_ptr<DiskANNItem> disk_ann_item_cosine) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    brpc::Controller cntl;
    ctx->SetCntl(&cntl);
    butil::Status status;
    status = disk_ann_item_l2->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                          already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    data_base_size = already_recv_vector_count;
    vectors.clear();
    vector_ids.clear();
    already_send_vector_count = already_recv_vector_count;
    already_recv_vector_count = 0;
  };

  auto start = lambda_time_now_function();

  while (true) {
    bool is_ok = ReadLine(reader, dimension, data, is_eof);
    if (!is_ok) {
      DINGO_LOG(ERROR) << "ReadLine failed";
      ASSERT_TRUE(false);
      return;
    }

    if (is_eof) {
      break;
    }

    pb::common::Vector vector;
    vector.set_dimension(dimension);
    vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector.add_float_values(data[j]);
    }
    vectors.push_back(std::move(vector));
    vector_ids.push_back(internal_start_vector_id);
    internal_start_vector_id++;

    if (data_base.size() / dimension < data_base_real_size) {
      data_base.insert(data_base.end(), data.begin(), data.end());
    }

    data.resize(0);

    if (vectors.size() >= import_batch_num) {
      lambda_import_function(disk_ann_item_l2, disk_ann_item_ip, disk_ann_item_cosine);
      DINGO_LOG(INFO) << "already_send_vector_count : " << already_send_vector_count << " ...";
    }
  }

  has_more = false;
  lambda_import_function(disk_ann_item_l2, disk_ann_item_ip, disk_ann_item_cosine);
  data_base_real_size = data_base.size() / dimension;
  // DINGO_LOG(INFO) << " already_send_vector_count : " << already_send_vector_count;
  DINGO_LOG(INFO) << "\timport_vector_total_count : " << already_send_vector_count;
  auto end = lambda_time_now_function();
  DINGO_LOG(INFO) << "\timport_vector total_count : " << already_send_vector_count
                  << "\ttime_cost : " << lambda_time_diff_microseconds_function(start, end) << " microseconds";
}

TEST_F(DiskANNItemAnnTest, Build) {
  butil::Status status;
  DiskANNCoreState state;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_l2->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "Build", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_ip->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "Build", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_cosine->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "Build", lambda_time_diff_microseconds_function(start, end));
  }
}

TEST_F(DiskANNItemAnnTest, Load) {
  butil::Status status;
  DiskANNCoreState state;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  pb::common::LoadDiskAnnParam load_param;
  load_param.set_num_nodes_to_cache(2);
  load_param.set_warmup(true);

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_l2->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "Load", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_ip->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "Load", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_cosine->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "Load", lambda_time_diff_microseconds_function(start, end));
  }

  // status
  {
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }
}

TEST_F(DiskANNItemAnnTest, Search) {
  butil::Status ok, status;
  DiskANNCoreState state;
  int64_t ts;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  // ok top_n = 3
  std::vector<pb::common::Vector> vectors;
  for (int i = 0; i < data_base_real_size; i++) {
    pb::common::Vector vector;
    vector.set_dimension(dimension);
    vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector.add_float_values(data_base[i * dimension + j]);
    }

    vectors.push_back(vector);
  }
  uint32_t top_n = 3;
  pb::common::SearchDiskAnnParam search_param;
  std::vector<pb::index::VectorWithDistanceResult> results;
  {
    auto start = lambda_time_now_function();
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "Search", lambda_time_diff_microseconds_function(start, end));
  }
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
  for (size_t i = 0; i < results.size(); i++) {
    DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
  }
  results.clear();
  {
    auto start = lambda_time_now_function();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "Search", lambda_time_diff_microseconds_function(start, end));
  }
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
  for (size_t i = 0; i < results.size(); i++) {
    DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
  }
  results.clear();
  {
    auto start = lambda_time_now_function();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "Search", lambda_time_diff_microseconds_function(start, end));
  }
  EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  results.clear();
}

TEST_F(DiskANNItemAnnTest, Close) {
  butil::Status status;
  DiskANNCoreState state;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "Close", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "Close", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "Close", lambda_time_diff_microseconds_function(start, end));
  }
}

TEST_F(DiskANNItemAnnTest, TryLoad) {
  butil::Status status;
  DiskANNCoreState state;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  pb::common::LoadDiskAnnParam load_param;
  load_param.set_num_nodes_to_cache(2);
  load_param.set_warmup(true);

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_l2->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "TryLoad", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_ip->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "TryLoad", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_cosine->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "TryLoad", lambda_time_diff_microseconds_function(start, end));
  }

  // status
  {
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }
}

TEST_F(DiskANNItemAnnTest, Destroy) {
  butil::Status status;
  DiskANNCoreState state;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_l2->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("l2", "Destroy", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_ip->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("ip", "Destroy", lambda_time_diff_microseconds_function(start, end));
  }

  {
    auto start = lambda_time_now_function();
    status = disk_ann_item_cosine->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    auto end = lambda_time_now_function();

    DINGO_LOG(INFO) << OutputCostString("cosine", "Destroy", lambda_time_diff_microseconds_function(start, end));
  }
}

}  // namespace dingodb
