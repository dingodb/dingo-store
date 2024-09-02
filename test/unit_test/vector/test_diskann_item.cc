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
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "diskann/diskann_item.h"
#include "diskann/diskann_utils.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

class DiskANNItemTest;
static void DoCreate(DiskANNItemTest &diskann_item_test);

class DiskANNItemTest : public testing::Test {
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

  friend void DoCreate(DiskANNItemTest &diskann_item_test);

  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;

  inline static std::shared_ptr<DiskANNItem> disk_ann_item_l2;
  inline static std::shared_ptr<DiskANNItem> disk_ann_item_ip;
  inline static std::shared_ptr<DiskANNItem> disk_ann_item_cosine;
  inline static int64_t id = 1;
  inline static uint32_t num_threads = 64;
  inline static float search_dram_budget_gb = 2.0f;
  inline static float build_dram_budget_gb = 4.0f;
  inline static std::shared_ptr<Context> ctx;
  inline static std::string base_dir = ".";
};

TEST_F(DiskANNItemTest, ClearAll) {
  DiskANNUtils::CreateDir(DiskANNItemTest::base_dir);
  DiskANNUtils::RemoveFile(DiskANNItemTest::base_dir + "/" + "data.bin");
  DiskANNUtils::RemoveDir(DiskANNItemTest::base_dir + "/" + "tmp");
  DiskANNUtils::RemoveDir(DiskANNItemTest::base_dir + "/" + "destroyed");
  DiskANNUtils::RemoveDir(DiskANNItemTest::base_dir + "/" + "normal");
  DiskANNUtils::RemoveDir(DiskANNItemTest::base_dir + "/" + "blackhole");
}

static void DoCreate(DiskANNItemTest &diskann_item_test) {
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
        diskann_item_test.ctx, diskann_item_test.id, index_parameter, diskann_item_test.num_threads,
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
        diskann_item_test.ctx, diskann_item_test.id + 1, index_parameter, diskann_item_test.num_threads,
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
        diskann_item_test.ctx, diskann_item_test.id + 2, index_parameter, diskann_item_test.num_threads,
        diskann_item_test.search_dram_budget_gb, diskann_item_test.build_dram_budget_gb);

    EXPECT_NE(diskann_item_test.disk_ann_item_l2.get(), nullptr);
  }
}

TEST_F(DiskANNItemTest, Create) { DoCreate(*this); }

TEST_F(DiskANNItemTest, Prepare) {
  std::string data_path = "./data.bin";
  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
    }
  }

  {
    int nb = data_base_size;
    int d = dimension;
    float *xb = data_base.data();
    std::cout << "generate data complete!!!" << std::endl;

    std::ofstream writer(data_path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!writer.is_open()) {
      DINGO_LOG(FATAL) << "file open fail : " << data_path;
    }

    writer.write(reinterpret_cast<char *>(&nb), sizeof(nb));
    writer.write(reinterpret_cast<char *>(&d), sizeof(d));

    int i = 0;
    int total = d * nb * sizeof(float);
    int left = total;
    int size = 1024;
    while (true) {
      size = std::min(size, left);
      writer.write(reinterpret_cast<char *>(xb) + i, size);
      left -= size;
      i += size;
      if (left <= 0) break;
    }
    writer.close();
  }
}

TEST_F(DiskANNItemTest, SetNoData) {
  butil::Status status;
  DiskANNCoreState state;

  {
    status = disk_ann_item_l2->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // import
  {
    int64_t already_recv_vector_count = 0;
    status = disk_ann_item_l2->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);

    status = disk_ann_item_ip->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);

    status = disk_ann_item_cosine->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
  }

  // build
  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // load
  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  // try load
  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_TRYLOAD_STATE_WRONG);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_TRYLOAD_STATE_WRONG);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_TRYLOAD_STATE_WRONG);
  }

  // search
  {
    uint32_t top_n = 0;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::common::Vector> vectors;
    std::vector<pb::index::VectorWithDistanceResult> results;
    int64_t ts = 0;

    status = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_NO_DATA);
    status = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_NO_DATA);
    status = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_NO_DATA);
  }

  // status
  {
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);
  }

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // status
  {
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);
  }

  // call do create
  {
    DoCreate(*this);
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kNoData);
  }

  {
    status = disk_ann_item_l2->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNItemTest, Import) {
  butil::Status status;
  DiskANNCoreState state;
  std::vector<pb::common::Vector> vectors;
  int ts = 1;
  int tso = 10;

  {
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
    }
  }

  // ts nomatch
  {
    disk_ann_item_l2->Close(ctx);
    disk_ann_item_ip->Close(ctx);
    disk_ann_item_cosine->Close(ctx);
    bool has_more = true;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    {
      int i = 0;
      already_send_vector_count = i;
      status = disk_ann_item_l2->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_ip->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_cosine->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                            already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;
    }

    {
      int i = 1;
      already_send_vector_count = i;
      status = disk_ann_item_l2->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TS_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_ip->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TS_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_cosine->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                            already_send_vector_count, ts + i, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TS_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;
    }
  }

  // tso nomatch
  {
    disk_ann_item_l2->Close(ctx);
    disk_ann_item_ip->Close(ctx);
    disk_ann_item_cosine->Close(ctx);
    bool has_more = true;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;
    {
      int i = 0;
      already_send_vector_count = i;
      status = disk_ann_item_l2->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_ip->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_cosine->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                            already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;
    }

    {
      int i = 1;
      already_send_vector_count = i;
      status = disk_ann_item_l2->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TSO_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_ip->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TSO_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_cosine->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                            already_send_vector_count, ts, tso + i, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TSO_NOT_MATCH);
      // EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;
    }
  }

  // ok
  {
    disk_ann_item_l2->Close(ctx);
    disk_ann_item_ip->Close(ctx);
    disk_ann_item_cosine->Close(ctx);
    bool has_more = true;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;
    for (int i = 0; i < data_base_size; i++) {
      already_send_vector_count = i;
      status = disk_ann_item_l2->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_ip->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                        already_send_vector_count, ts, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      status = disk_ann_item_cosine->Import(ctx, {vectors[i]}, {i}, has_more, force_to_load_data_if_exist,
                                            already_send_vector_count, ts, tso, already_recv_vector_count);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(already_recv_vector_count, i + 1);
      already_recv_vector_count = 0;

      if (1 == i) {
        // set no data
        {
          status = disk_ann_item_l2->SetNoData(ctx);
          EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

          status = disk_ann_item_ip->SetNoData(ctx);
          EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

          status = disk_ann_item_cosine->SetNoData(ctx);
          EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
        }
      }
    }

    already_send_vector_count = data_base_size;
    has_more = false;
    status = disk_ann_item_l2->Import(ctx, {}, {}, has_more, force_to_load_data_if_exist, already_send_vector_count, ts,
                                      tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, data_base_size);

    status = disk_ann_item_ip->Import(ctx, {}, {}, has_more, force_to_load_data_if_exist, already_send_vector_count, ts,
                                      tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, data_base_size);

    status = disk_ann_item_cosine->Import(ctx, {}, {}, has_more, force_to_load_data_if_exist, already_send_vector_count,
                                          ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, data_base_size);
  }

  // once
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    status = disk_ann_item_l2->Import(ctx, vectors, {}, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_ip->Import(ctx, vectors, {}, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_cosine->Import(ctx, vectors, {}, has_more, force_to_load_data_if_exist,
                                          already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;
  }
}

TEST_F(DiskANNItemTest, Build) {
  butil::Status status;
  DiskANNCoreState state;

  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
  }

  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_BUILDING);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_l2->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_ip->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_cosine->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }

  status = disk_ann_item_l2->Build(ctx, false, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = disk_ann_item_ip->Build(ctx, false, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = disk_ann_item_cosine->Build(ctx, false, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  ///////////////////////////////////////////////////////////////
  // rebuild
  status = disk_ann_item_l2->Build(ctx, true, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = disk_ann_item_ip->Build(ctx, true, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = disk_ann_item_cosine->Build(ctx, true, false);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  while (true) {
    sleep(1);
    state = disk_ann_item_l2->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_ip->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_cosine->Status(ctx);
    if (state == DiskANNCoreState::kUpdatedPath) {
      break;
    }
  }
}

TEST_F(DiskANNItemTest, Load) {
  butil::Status status;
  DiskANNCoreState state;

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_l2->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_ip->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_cosine->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kLoaded);
  }
}

TEST_F(DiskANNItemTest, Search) {
  butil::Status ok, status;
  DiskANNCoreState state;
  int64_t ts;

  // vectors empty topk == 0, return OK
  {
    uint32_t top_n = 0;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::common::Vector> vectors;
    std::vector<pb::index::VectorWithDistanceResult> results;

    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
  }

  // top_n = 0, return OK
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(data_base[i * dimension + j]);
      }

      vectors.push_back(vector);
    }
    uint32_t top_n = 0;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }

  // ok top_n = 20
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(data_base[i * dimension + j]);
      }

      vectors.push_back(vector);
    }
    uint32_t top_n = 20;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNItemTest, Close) {
  butil::Status status;
  DiskANNCoreState state;

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(state, DiskANNCoreState::kUnknown);
  }
}

TEST_F(DiskANNItemTest, TryLoad) {
  butil::Status status;
  DiskANNCoreState state;

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_l2->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_ip->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_cosine->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }
}

TEST_F(DiskANNItemTest, SearchAgain) {
  butil::Status ok;
  DiskANNCoreState state;
  int64_t ts;

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNItemTest, FakeBuilded) {
  butil::Status ok, status;
  DiskANNCoreState state;

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // call do create
  {
    DoCreate(*this);
    DiskANNCoreState state = disk_ann_item_l2->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kFakeBuilded);

    state = disk_ann_item_ip->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kFakeBuilded);

    state = disk_ann_item_cosine->Status(ctx);
    EXPECT_EQ(state, DiskANNCoreState::kFakeBuilded);
  }

  // set no data
  {
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
    status = disk_ann_item_l2->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
    status = disk_ann_item_ip->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);

    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
    status = disk_ann_item_cosine->SetNoData(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NO_DATA_STATE_WRONG);
  }

  // import
  {
    int64_t already_recv_vector_count = 0;
    status = disk_ann_item_l2->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);

    status = disk_ann_item_ip->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);

    status = disk_ann_item_cosine->Import(ctx, {}, {}, true, false, 0, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
  }

  // build
  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // load
  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  // search
  {
    uint32_t top_n = 0;
    pb::common::SearchDiskAnnParam search_param;
    std::vector<pb::common::Vector> vectors;
    std::vector<pb::index::VectorWithDistanceResult> results;
    int64_t ts = 0;

    status = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    status = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    status = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // try load
  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
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

  // try load
  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(DiskANNItemTest, Misc) {
  butil::Status ok, status;
  DiskANNCoreState state;
  int64_t ts = 10;
  int64_t tso = 100;

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // import once
  {
    std::vector<pb::common::Vector> vectors;
    std::vector<int64_t> vector_ids;
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
      vector_ids.push_back(i + data_base_size);
    }

    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    status = disk_ann_item_l2->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_ip->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_cosine->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                          already_send_vector_count, ts, tso, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  {
    status = disk_ann_item_l2->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    load_param.set_direct_load_without_build(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IS_LOADING);
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_l2->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_ip->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  while (true) {
    sleep(1);
    state = disk_ann_item_cosine->Status(ctx);
    if (state == DiskANNCoreState::kLoaded) {
      break;
    }
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNItemTest, MiscSync) {
  butil::Status ok, status;
  DiskANNCoreState state;
  int64_t ts;

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, false);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  {
    status = disk_ann_item_l2->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // import once
  {
    std::vector<pb::common::Vector> vectors;
    std::vector<int64_t> vector_ids;
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
      vector_ids.push_back(i + data_base_size);
    }

    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    status = disk_ann_item_l2->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_ip->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                      already_send_vector_count, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;

    status = disk_ann_item_cosine->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist,
                                          already_send_vector_count, 0, 0, already_recv_vector_count);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_STATE_WRONG);
    EXPECT_EQ(already_recv_vector_count, 0);
    already_recv_vector_count = 0;
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    status = disk_ann_item_l2->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_ip->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);

    status = disk_ann_item_cosine->Load(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  }

  {
    status = disk_ann_item_l2->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_ip->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);

    status = disk_ann_item_cosine->Build(ctx, false, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_BUILD_STATE_WRONG);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    load_param.set_direct_load_without_build(true);

    status = disk_ann_item_l2->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNItemTest, MiscTryLoadSync) {
  butil::Status ok, status;
  DiskANNCoreState state;
  int64_t ts;

  {
    status = disk_ann_item_l2->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Close(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);

    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  {
    pb::common::LoadDiskAnnParam load_param;
    load_param.set_num_nodes_to_cache(2);
    load_param.set_warmup(true);
    load_param.set_direct_load_without_build(true);
    status = disk_ann_item_l2->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->TryLoad(ctx, load_param, true);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    std::vector<pb::common::Vector> vectors;
    for (int i = 0; i < data_base_size; i++) {
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
    ok = disk_ann_item_l2->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_ip->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << results.size();
    for (size_t i = 0; i < results.size(); i++) {
      DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << results[i].DebugString();
    }
    results.clear();
    ok = disk_ann_item_cosine->Search(ctx, top_n, search_param, vectors, results, ts);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    results.clear();
  }
}

TEST_F(DiskANNItemTest, Destroy) {
  butil::Status status;
  DiskANNCoreState state;

  {
    status = disk_ann_item_l2->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_ip->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    status = disk_ann_item_cosine->Destroy(ctx);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }
}

}  // namespace dingodb
