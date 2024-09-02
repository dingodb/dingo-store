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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/role.h"
#include "config/config_manager.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "faiss/MetricType.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "server/server.h"
#include "vector/vector_index_diskann.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

// TEST_VECTOR_INDEX_DISKANN_MOCK define in  vector/vector_index_diskann.h
// start diskann_server first

DECLARE_bool(diskann_build_sync_internal);

DECLARE_bool(diskann_load_sync_internal);

DECLARE_bool(diskann_reset_force_delete_file_internal);

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

static const std::vector<std::string> kAllCFs = {Constant::kVectorDataCF};

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: $INSTANCE_ID$\n"
    "  keyring: TO_BE_CONTINUED\n"
    "log:\n"
    "  level: INFO\n"
    "  path: " +
    kLogPath +
    "\n"
    "vector:\n"
    "  index_path: $BASE_PATH$/data/vector_index_snapshot\n"
    "  enable_follower_hold_index: false\n"
    "  operation_parallel_thread_num: 16 # vector index operation parallel thread num.\n"
    "  fast_background_worker_num: 8 # vector index fast load/build.\n"
    "  background_worker_num: 16 # vector index slow load/build/rebuild.\n"
    "  max_background_task_count: 16 # vector index slow load/build/rebuild max pending task count.\n"
    "store:\n"
    "  path: " +
    kStorePath +
    "\n"
    "  background_thread_num: 16 # background_thread_num priority background_thread_ratio\n"
    "  fast_background_thread_num: 8 # background_thread_num priority background_thread_ratio\n"
    "  # background_thread_ratio: 0.5 # cpu core * ratio\n"
    "  stats_dump_period_s: 120\n"
    "diskann:\n"
    "  listen_host: 0.0.0.0\n"
    "  host: 172.30.14.11\n"
    "  port: 34001\n"
    "  diskann_server_build_worker_num: 64 # the number of build worker used by diskann_service\n"
    "  diskann_server_build_worker_max_pending_num: 256 # 0 is unlimited\n"
    "  diskann_server_load_worker_num: 128 # the number of load worker used by diskann_service\n"
    "  diskann_server_load_worker_max_pending_num: 1024 # 0 is unlimited\n";

class VectorIndexDiskannTest;
static void DoReset(VectorIndexDiskannTest &vector_index_diskann_test);

class VectorIndexDiskannTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kStorePath);
    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));
    ConfigManager::GetInstance().Register("index", config);
    SetRole("index");
    VectorIndexDiskANN::Init();

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  static void TearDownTestSuite() {
    vector_index_diskann_l2.reset();
    vector_index_diskann_ip.reset();
    vector_index_diskann_cosine.reset();
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  friend void DoReset(VectorIndexDiskannTest &vector_index_diskann_test);

  inline static std::shared_ptr<VectorIndex> vector_index_diskann_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_diskann_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_diskann_cosine;
  inline static int64_t l2_id = 1;
  inline static int64_t ip_id = 2;
  inline static int64_t cosine_id = 3;
  inline static faiss::idx_t dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;
  inline static std::shared_ptr<Config> config;
  inline static std::shared_ptr<RocksRawEngine> engine;
  inline static int64_t ts = 0;
};

TEST_F(VectorIndexDiskannTest, Create) {
  static const pb::common::Range kRange;
  static pb::common::RegionEpoch kEpoch;  // NOLINT
  kEpoch.set_conf_version(1);
  kEpoch.set_version(10);

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(50);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(60);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(1000);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // invalid param
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(64);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(100);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(75);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_EQ(vector_index_diskann_l2.get(), nullptr);
  }

  // valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
    vector_index_diskann_l2 = VectorIndexFactory::NewDiskANN(l2_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_diskann_l2.get(), nullptr);
  }

  // valid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
    vector_index_diskann_ip = VectorIndexFactory::NewDiskANN(ip_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_diskann_ip.get(), nullptr);
  }

  // valid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_diskann_parameter()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);
    vector_index_diskann_cosine = VectorIndexFactory::NewDiskANN(cosine_id, index_parameter, kEpoch, kRange, nullptr);
    EXPECT_NE(vector_index_diskann_cosine.get(), nullptr);
  }
}

static void DoReset(VectorIndexDiskannTest &vector_index_diskann_test) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = true;

  status = vector_index_diskann_test.vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_test.vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_test.vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, Reset) {
  DoReset(*this);

  auto writer = engine->Writer();

  auto lambda_delete_range_function = [&writer](int64_t region_part_id) {
    pb::common::Range region_range;
    std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
    std::string end_key =
        VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

    region_range.set_start_key(start_key);
    region_range.set_end_key(end_key);

    writer->KvDeleteRange(Constant::kVectorDataCF, region_range);
  };

  lambda_delete_range_function(l2_id);
  lambda_delete_range_function(ip_id);
  lambda_delete_range_function(cosine_id);
}

TEST_F(VectorIndexDiskannTest, SearchNoData) {
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
  }

  // ok
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
  }
}

TEST_F(VectorIndexDiskannTest, BuildNoData) {
  pb::error::Error internal_error;
  butil::Status status;
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  pb::common::VectorStateParameter vector_state_parameter;

  // FLAGS_diskann_build_sync_internal = true;

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        EXPECT_EQ(pb::common::DiskANNCoreState::NODATA, vector_state_parameter.diskann().state());
        break;
      }

      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        EXPECT_EQ(pb::common::DiskANNCoreState::NODATA, vector_state_parameter.diskann().state());
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        EXPECT_EQ(pb::common::DiskANNCoreState::NODATA, vector_state_parameter.diskann().state());
        break;
      }
      sleep(1);
    }
  }

  sleep(1);

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);

  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);

  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::NODATA);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_NODATA);

  DoReset(*this);
}

TEST_F(VectorIndexDiskannTest, Import) {
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
  }

  // import l2 ip cosine data
  {
    auto writer = engine->Writer();

    auto lambda_writer_vector_to_rocksdb_function = [&writer](int id) {
      pb::common::Range region_range;

      region_range.set_start_key(VectorCodec::PackageVectorKey(Constant::kExecutorRaw, id, 0 + data_base_size));
      region_range.set_end_key(
          VectorCodec::PackageVectorKey(Constant::kExecutorRaw, id, data_base_size + data_base_size));

      // pb::common::Range encode_range = mvcc::Codec::EncodeRange(region_range);

      pb::common::RegionDefinition region_definition;

      region_definition.set_id(id);
      region_definition.mutable_range()->Swap(&region_range);
      region_definition.set_schema_id(id);
      region_definition.set_table_id(id);
      region_definition.set_index_id(id);
      region_definition.set_part_id(id);
      region_definition.set_tenant_id(id);
      region_definition.mutable_index_parameter()->set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);

      pb::common::Region pb_region;
      pb_region.set_id(id);
      pb_region.set_region_type(::dingodb::pb::common::RegionType::INDEX_REGION);
      pb_region.mutable_definition()->Swap(&region_definition);

      std::vector<pb::common::KeyValue> kvs;

      auto region = store::Region::New(pb_region.definition());

      auto prefix = region->GetKeyPrefix();
      auto region_part_id = region->PartitionId();
      for (int i = 0; i < data_base_size; i++) {
        int64_t vector_id = i + data_base_size;
        pb::common::Vector vector;
        vector.set_dimension(dimension);
        vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (size_t j = 0; j < dimension; j++) {
          float value = data_base[i * dimension + j];
          vector.add_float_values(value);
        }

        std::string encode_key_with_ts = mvcc::Codec::EncodeKey(
            std::string(VectorCodec::PackageVectorKey(Constant::kExecutorRaw, id, vector_id)), ts);
        // std::cout << "encode_key_with_ts : " << Helper::StringToHex(encode_key_with_ts) << ", vector_id : " <<
        // vector_id
        //           << std::endl;
        pb::common::KeyValue kv;
        kv.set_key(encode_key_with_ts);
        std::string value = vector.SerializeAsString();
        mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, value);
        kv.mutable_value()->swap(value);
        kvs.push_back(std::move(kv));
      }

      writer->KvBatchPutAndDelete(Constant::kVectorDataCF, kvs, {});
    };

    lambda_writer_vector_to_rocksdb_function(l2_id);
    lambda_writer_vector_to_rocksdb_function(ip_id);
    lambda_writer_vector_to_rocksdb_function(cosine_id);

    auto reader = engine->Reader();
    int64_t count = 0;

    auto lambda_reader_vector_count_function = [&reader](int id) {
      int64_t count = 0;
      std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, id, 0 + data_base_size);
      std::string end_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, id, data_base_size + data_base_size);
      pb::common::Range region_range;

      region_range.set_start_key(start_key);
      region_range.set_end_key(end_key);
      pb::common::Range encode_range = mvcc::Codec::EncodeRange(region_range);
      reader->KvCount(Constant::kVectorDataCF, encode_range.start_key(), encode_range.end_key(), count);

      return count;
    };

    count = lambda_reader_vector_count_function(l2_id);
    EXPECT_EQ(count, data_base_size);
    count = 0;
    count = lambda_reader_vector_count_function(ip_id);
    EXPECT_EQ(count, data_base_size);
    count = 0;
    count = lambda_reader_vector_count_function(cosine_id);
    EXPECT_EQ(count, data_base_size);
    count = 0;
  }
}

TEST_F(VectorIndexDiskannTest, Build) {
  pb::error::Error internal_error;
  butil::Status status;
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  pb::common::VectorStateParameter vector_state_parameter;

  // FLAGS_diskann_build_sync_internal = true;

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }
}

TEST_F(VectorIndexDiskannTest, Load) {
  butil::Status status;
  pb::error::Error internal_error;

  pb::common::VectorLoadParameter parameter;
  pb::common::VectorStateParameter vector_state_parameter;

  // FLAGS_diskann_load_sync_internal = true;

  status = vector_index_diskann_l2->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_ip->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_cosine->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }
}

TEST_F(VectorIndexDiskannTest, Search) {
  butil::Status ok;

  // invalid param failed, topk == 0, return OK
  {
    pb::common::VectorWithId vector_with_id;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    uint32_t topk = 1;
    std::vector<pb::index::VectorWithDistanceResult> results;
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
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

    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // filter not support
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

    vector_ids.reserve(data_base_size / 2);
    for (int cnt = 0; cnt < data_base_size / 2; cnt++) {
      vector_ids.push_back(cnt + data_base_size);
    }

    auto flat_list_filter_functor = std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids);

    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {flat_list_filter_functor}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_NOT_SUPPORT);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {flat_list_filter_functor}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_NOT_SUPPORT);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {flat_list_filter_functor}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EVECTOR_NOT_SUPPORT);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    DINGO_LOG(INFO) << "vector_index_diskann_l2 results size: " << results.size();
    for (const auto &result : results) {
      DINGO_LOG(INFO) << "vector_index_diskann_l2 results: " << result.DebugString();
    }
    results.clear();

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    DINGO_LOG(INFO) << "vector_index_diskann_ip results size: " << results.size();
    for (const auto &result : results) {
      DINGO_LOG(INFO) << "vector_index_diskann_ip results: " << result.DebugString();
    }
    results.clear();
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    DINGO_LOG(INFO) << "vector_index_diskann_cosine results size: " << results.size();
    for (const auto &result : results) {
      DINGO_LOG(INFO) << "vector_index_diskann_cosine results: " << result.DebugString();
    }
    results.clear();
  }
}

TEST_F(VectorIndexDiskannTest, Status) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::OK);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::OK);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::OK);
}

TEST_F(VectorIndexDiskannTest, Dump) {
  butil::Status status;
  bool dump_all = false;
  std::vector<std::string> dump_datas;

  status = vector_index_diskann_l2->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 1);
  DINGO_LOG(INFO) << "dump_datas[0]: " << dump_datas[0];
  dump_datas.clear();

  status = vector_index_diskann_ip->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 1);
  DINGO_LOG(INFO) << "dump_datas[0]: " << dump_datas[0];
  dump_datas.clear();

  status = vector_index_diskann_cosine->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 1);
  DINGO_LOG(INFO) << "dump_datas[0]: " << dump_datas[0];
  dump_datas.clear();

  ///////////////////////////////////////////////////////////////////////
  dump_all = true;
  status = vector_index_diskann_l2->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 3);
  for (const auto &dump_data : dump_datas) {
    DINGO_LOG(INFO) << "dump_data: " << dump_data;
  }
  dump_datas.clear();

  status = vector_index_diskann_ip->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 3);
  for (const auto &dump_data : dump_datas) {
    DINGO_LOG(INFO) << "dump_data: " << dump_data;
  }
  dump_datas.clear();

  status = vector_index_diskann_cosine->Dump(dump_all, dump_datas);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(dump_datas.size(), 3);
  for (const auto &dump_data : dump_datas) {
    DINGO_LOG(INFO) << "dump_data: " << dump_data;
  }
  dump_datas.clear();
}

TEST_F(VectorIndexDiskannTest, Count) {
  butil::Status status;
  int64_t count = 0;

  status = vector_index_diskann_l2->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(count, data_base_size);

  status = vector_index_diskann_ip->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(count, data_base_size);

  status = vector_index_diskann_cosine->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(count, data_base_size);

  EXPECT_EQ(vector_index_diskann_l2->GetDimension(), dimension);
  EXPECT_EQ(vector_index_diskann_l2->GetMetricType(), pb::common::MetricType::METRIC_TYPE_L2);
  EXPECT_EQ(vector_index_diskann_ip->GetMetricType(), pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
  EXPECT_EQ(vector_index_diskann_cosine->GetMetricType(), pb::common::MetricType::METRIC_TYPE_COSINE);
}

TEST_F(VectorIndexDiskannTest, Build1Data) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  FLAGS_diskann_reset_force_delete_file_internal = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  FLAGS_diskann_reset_force_delete_file_internal = true;
}

TEST_F(VectorIndexDiskannTest, LoadDirect) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  FLAGS_diskann_reset_force_delete_file_internal = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  pb::common::VectorLoadParameter parameter;
  parameter.mutable_diskann()->set_num_nodes_to_cache(2);
  parameter.mutable_diskann()->set_warmup(true);
  parameter.mutable_diskann()->set_direct_load_without_build(true);

  status = vector_index_diskann_l2->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
  }
  status = vector_index_diskann_ip->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
  }
  status = vector_index_diskann_cosine->Load(parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        break;
      }
      sleep(1);
    }
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  FLAGS_diskann_reset_force_delete_file_internal = true;
}

TEST_F(VectorIndexDiskannTest, BuildDuplicate) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  // FLAGS_diskann_build_sync_internal = true;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, l2_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, cosine_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      if (pb::common::DiskANNCoreState::UNKNOWN == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::UNKNOWN, vector_state_parameter.diskann().state());
      }
      if (pb::common::DiskANNCoreState::IMPORTING == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::IMPORTING, vector_state_parameter.diskann().state());
      }
      if (internal_error.errcode() == pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      if (pb::common::DiskANNCoreState::UNKNOWN == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::UNKNOWN, vector_state_parameter.diskann().state());
      }
      if (pb::common::DiskANNCoreState::IMPORTING == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::IMPORTING, vector_state_parameter.diskann().state());
      }
      if (internal_error.errcode() == pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED) {
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      if (pb::common::DiskANNCoreState::UNKNOWN == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::UNKNOWN, vector_state_parameter.diskann().state());
      }
      if (pb::common::DiskANNCoreState::IMPORTING == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(pb::common::DiskANNCoreState::IMPORTING, vector_state_parameter.diskann().state());
      }
      if (internal_error.errcode() == pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED) {
        break;
      }
      sleep(1);
    }
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::IMPORTING);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILD_FAILED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::IMPORTING);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILD_FAILED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(internal_error.errcode(), pb::error::Errno::EDISKANN_IMPORT_VECTOR_ID_DUPLICATED);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::IMPORTING);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILD_FAILED);

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
}

TEST_F(VectorIndexDiskannTest, BuildMultiThread) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());

  auto lambda_build_function = [&reader](std::shared_ptr<VectorIndex> vector_index_diskann, int64_t part_id) {
    pb::error::Error internal_error;
    butil::Status status;
    pb::common::VectorStateParameter vector_state_parameter;
    pb::common::Range region_range;
    std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, part_id, 0 + data_base_size);
    std::string end_key =
        VectorCodec::PackageVectorKey(Constant::kExecutorRaw, part_id, data_base_size + data_base_size);

    region_range.set_start_key(start_key);
    region_range.set_end_key(end_key);

    pb::common::VectorBuildParameter parameter;
    parameter.mutable_diskann()->set_force_to_build_if_exist(false);

    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    if (status.error_code() == pb::error::Errno::OK) {
      if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::BUILDED) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::IMPORTING) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::IMPORTING);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::IMPORTED) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::IMPORTED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UPDATINGPATH) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATINGPATH);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UPDATEDPATH) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
      } else {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
      }
    } else {
      EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_IMPORT_TSO_NOT_MATCH);
    }

    while (true) {
      status = vector_index_diskann->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UNKNOWN) {
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::IMPORTING) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::IMPORTED) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::BUILDING) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::BUILDED) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UPDATINGPATH) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
          continue;
        }
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  };

  std::vector<std::thread> threads;
  int thread_count = 100;
  threads.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    if (0 == (i % 3)) {
      threads.emplace_back(lambda_build_function, vector_index_diskann_l2, l2_id);
    } else if (1 == (i % 3)) {
      threads.emplace_back(lambda_build_function, vector_index_diskann_ip, ip_id);
    } else {
      threads.emplace_back(lambda_build_function, vector_index_diskann_cosine, cosine_id);
    }
  }

  for (int i = 0; i < thread_count; i++) {
    threads[i].join();
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, LoadMultiThread) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  auto lambda_load_function = [](std::shared_ptr<VectorIndex> vector_index_diskann) {
    butil::Status status;
    pb::common::VectorStateParameter vector_state_parameter;
    pb::error::Error internal_error;

    pb::common::VectorLoadParameter parameter;
    parameter.mutable_diskann()->set_direct_load_without_build(false);
    parameter.mutable_diskann()->set_warmup(true);
    parameter.mutable_diskann()->set_num_nodes_to_cache(2);

    status = vector_index_diskann->Load(parameter, vector_state_parameter);
    if (status.error_code() == pb::error::Errno::OK) {
      if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UPDATEDPATH) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::LOADING) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
      } else {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
      }
    }

    while (true) {
      status = vector_index_diskann->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::UPDATEDPATH) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::LOADING) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
          continue;
        }
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  };

  std::vector<std::thread> threads;
  int thread_count = 100;
  threads.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    if (0 == (i % 3)) {
      threads.emplace_back(lambda_load_function, vector_index_diskann_l2);
    } else if (1 == (i % 3)) {
      threads.emplace_back(lambda_load_function, vector_index_diskann_ip);
    } else {
      threads.emplace_back(lambda_load_function, vector_index_diskann_cosine);
    }
  }

  for (int i = 0; i < thread_count; i++) {
    threads[i].join();
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  FLAGS_diskann_reset_force_delete_file_internal = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, TryLoadMultiThread) {
  butil::Status status;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  FLAGS_diskann_reset_force_delete_file_internal = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  auto lambda_load_function = [](std::shared_ptr<VectorIndex> vector_index_diskann) {
    butil::Status status;
    pb::common::VectorStateParameter vector_state_parameter;
    pb::error::Error internal_error;

    pb::common::VectorLoadParameter parameter;
    parameter.mutable_diskann()->set_direct_load_without_build(true);
    parameter.mutable_diskann()->set_warmup(true);
    parameter.mutable_diskann()->set_num_nodes_to_cache(2);

    status = vector_index_diskann->Load(parameter, vector_state_parameter);
    if (status.error_code() == pb::error::Errno::OK) {
      if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::FAKEBUILDED) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::FAKEBUILDED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
      } else if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::LOADING) {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
      } else {
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
      }
    }

    while (true) {
      status = vector_index_diskann->Status(vector_state_parameter, internal_error);
      if (status.error_code() == pb::error::Errno::OK) {
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::FAKEBUILDED) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
          continue;
        }
        if (vector_state_parameter.diskann().state() == pb::common::DiskANNCoreState::LOADING) {
          EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
          continue;
        }
        EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  };

  std::vector<std::thread> threads;
  int thread_count = 100;
  threads.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    if (0 == (i % 3)) {
      threads.emplace_back(lambda_load_function, vector_index_diskann_l2);
    } else if (1 == (i % 3)) {
      threads.emplace_back(lambda_load_function, vector_index_diskann_ip);
    } else {
      threads.emplace_back(lambda_load_function, vector_index_diskann_cosine);
    }
  }

  for (int i = 0; i < thread_count; i++) {
    threads[i].join();
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  FLAGS_diskann_reset_force_delete_file_internal = true;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscFull) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = true;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // search EDISKANN_NOT_BUILD
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_BUILD);
  }

  // build
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  parameter.mutable_diskann()->set_force_to_build_if_exist(false);

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  // load
  pb::common::VectorLoadParameter load_parameter;
  load_parameter.mutable_diskann()->set_direct_load_without_build(false);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
  }
  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  // search again
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  FLAGS_diskann_reset_force_delete_file_internal = false;
  delete_data_file = false;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscDirect) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  FLAGS_diskann_reset_force_delete_file_internal = false;
  delete_data_file = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // search EDISKANN_NOT_LOAD
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  // tryload
  pb::common::VectorLoadParameter load_parameter;
  load_parameter.mutable_diskann()->set_direct_load_without_build(true);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  // search again
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  FLAGS_diskann_reset_force_delete_file_internal = true;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscDisorder) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = true;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // load first
  pb::common::VectorLoadParameter load_parameter;
  load_parameter.mutable_diskann()->set_direct_load_without_build(false);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);

  EXPECT_EQ(status.error_code(), pb::error::Errno::EDISKANN_LOAD_STATE_WRONG);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UNKNOWN);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // build
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  parameter.mutable_diskann()->set_force_to_build_if_exist(false);

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

        break;
      }
      sleep(1);
    }
  }

  // build again
  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  // search
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EDISKANN_NOT_LOAD);
  }

  // load
  load_parameter.mutable_diskann()->set_direct_load_without_build(false);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }
  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);
  if (FLAGS_diskann_load_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADING);
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);
        break;
      }
      sleep(1);
    }
  }

  // load again
  load_parameter.mutable_diskann()->set_direct_load_without_build(true);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);
  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  // try load again
  status = vector_index_diskann_l2->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Load(load_parameter, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  // search again
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);

  delete_data_file = false;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscDisorder2) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = true;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // build
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  parameter.mutable_diskann()->set_force_to_build_if_exist(false);

  auto lambda_build_function = [&region_range, &reader, &parameter](auto vector_index_diskann) {
    pb::common::VectorStateParameter vector_state_parameter;
    butil::Status status;
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Build(region_range, reader, parameter, ts, vector_state_parameter);
  };

  lambda_build_function(vector_index_diskann_l2);
  lambda_build_function(vector_index_diskann_ip);
  lambda_build_function(vector_index_diskann_cosine);
  lambda_build_function(vector_index_diskann_l2);
  lambda_build_function(vector_index_diskann_ip);
  lambda_build_function(vector_index_diskann_cosine);

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  // build again
  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);

  delete_data_file = false;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscDisorder3) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = true;
  pb::error::Error internal_error;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // build
  mvcc::ReaderPtr reader = std::make_shared<mvcc::KvReader>(engine->Reader());
  pb::common::Range region_range;
  int64_t region_part_id = l2_id;
  std::string start_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, 0 + data_base_size);
  std::string end_key =
      VectorCodec::PackageVectorKey(Constant::kExecutorRaw, region_part_id, data_base_size + data_base_size);

  region_range.set_start_key(start_key);
  region_range.set_end_key(end_key);

  pb::common::VectorBuildParameter parameter;
  parameter.mutable_diskann()->set_force_to_build_if_exist(false);

  status = vector_index_diskann_l2->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_ip->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }
  status = vector_index_diskann_cosine->Build(region_range, reader, parameter, ts, vector_state_parameter);
  if (FLAGS_diskann_build_sync_internal) {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::UPDATEDPATH);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
  } else {
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::BUILDING);
    EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDING);
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_build_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::UPDATEDPATH == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_BUILDED);
        break;
      }
      sleep(1);
    }
  }

  // load
  pb::common::VectorLoadParameter load_parameter;
  load_parameter.mutable_diskann()->set_direct_load_without_build(false);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  auto lambda_load_function = [&region_range, &reader, &load_parameter](auto vector_index_diskann) {
    pb::common::VectorStateParameter vector_state_parameter;
    butil::Status status;
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
  };

  lambda_load_function(vector_index_diskann_l2);
  lambda_load_function(vector_index_diskann_ip);
  lambda_load_function(vector_index_diskann_cosine);
  lambda_load_function(vector_index_diskann_l2);
  lambda_load_function(vector_index_diskann_ip);
  lambda_load_function(vector_index_diskann_cosine);

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  // search
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  delete_data_file = false;
  FLAGS_diskann_reset_force_delete_file_internal = false;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);
}

TEST_F(VectorIndexDiskannTest, MiscDisorder4) {
  butil::Status status, ok;
  pb::common::VectorStateParameter vector_state_parameter;
  bool delete_data_file = false;
  pb::error::Error internal_error;

  FLAGS_diskann_reset_force_delete_file_internal = false;

  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  // try load
  pb::common::VectorLoadParameter load_parameter;
  load_parameter.mutable_diskann()->set_direct_load_without_build(true);
  load_parameter.mutable_diskann()->set_warmup(true);
  load_parameter.mutable_diskann()->set_num_nodes_to_cache(2);

  auto lambda_try_load_function = [&load_parameter](auto vector_index_diskann) {
    pb::common::VectorStateParameter vector_state_parameter;
    butil::Status status;
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    status = vector_index_diskann->Load(load_parameter, vector_state_parameter);
  };

  lambda_try_load_function(vector_index_diskann_l2);
  lambda_try_load_function(vector_index_diskann_ip);
  lambda_try_load_function(vector_index_diskann_cosine);
  lambda_try_load_function(vector_index_diskann_l2);
  lambda_try_load_function(vector_index_diskann_ip);
  lambda_try_load_function(vector_index_diskann_cosine);

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  if (!FLAGS_diskann_load_sync_internal) {
    while (true) {
      status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      if (pb::common::DiskANNCoreState::LOADED == vector_state_parameter.diskann().state()) {
        EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

        break;
      }
      sleep(1);
    }
  }

  status = vector_index_diskann_l2->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_ip->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  status = vector_index_diskann_cosine->Status(vector_state_parameter, internal_error);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::LOADED);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_LOADED);

  // search
  {
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(10);
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
    ok = vector_index_diskann_l2->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_ip->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = vector_index_diskann_cosine->Search(vector_with_ids, topk, {}, false, {}, results);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  delete_data_file = false;
  status = vector_index_diskann_l2->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_ip->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  status = vector_index_diskann_cosine->Reset(delete_data_file, vector_state_parameter);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  EXPECT_EQ(vector_state_parameter.diskann().state(), pb::common::DiskANNCoreState::RESET);
  EXPECT_EQ(vector_state_parameter.diskann().diskann_state(), pb::common::DiskANNState::DISKANN_INITIALIZED);

  FLAGS_diskann_reset_force_delete_file_internal = true;
}

TEST_F(VectorIndexDiskannTest, Drop) {
  butil::Status status;

  status = vector_index_diskann_l2->Drop();
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = vector_index_diskann_ip->Drop();
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  status = vector_index_diskann_cosine->Drop();
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

  int64_t count = 0;

  status = vector_index_diskann_l2->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EINDEX_NOT_FOUND);
  EXPECT_EQ(count, 0);

  status = vector_index_diskann_ip->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EINDEX_NOT_FOUND);
  EXPECT_EQ(count, 0);

  status = vector_index_diskann_cosine->GetCount(count);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EINDEX_NOT_FOUND);
  EXPECT_EQ(count, 0);
}

}  // namespace dingodb
