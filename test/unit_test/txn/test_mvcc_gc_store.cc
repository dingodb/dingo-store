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

#include <dirent.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "config/yaml_config.h"
#include "coordinator/tso_control.h"
#include "engine/raw_engine.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "server/server.h"

// open ENABLE_GC_MOCK macro !!!!!!!!!

DECLARE_string(role);

namespace dingodb {
DECLARE_bool(dingo_log_switch_txn_detail);
DECLARE_int64(gc_delete_batch_count);

static const std::string kRootPath = "./unit_test";       // NOLINT
static const std::string kLogPath = kRootPath + "/log";   // NOLINT
static const std::string kStorePath = kRootPath + "/db";  // NOLINT

static const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "log:\n"
    "  path: " +
    kLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kStorePath + "\n";

static void DoGcCore(bool gc_stop, int64_t safe_point_ts);
static void DeleteRange();
static void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                        int set_tso_position, std::vector<mvcc::ValueFlag> ops,
                        std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                           RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops)>
                            func);
static void PrepareDataFunction(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops);

class MvccGcStoreTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kStorePath);

    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    FLAGS_role = "store";
    ConfigManager::GetInstance().Register(FLAGS_role, config);
    // ConfigManager::GetInstance().Register("index", config);
    Server::GetInstance().InitServerID();
    Server::GetInstance().InitCoordinatorInteraction();
    Server::GetInstance().InitTsProvider();
    Server::GetInstance().InitRocksRawEngine();
    Server::GetInstance().InitEngine();
    engine = std::dynamic_pointer_cast<RocksRawEngine>(
        Server::GetInstance().GetRawEngine(pb::common::RawEngine::RAW_ENG_ROCKSDB));
    Server::GetInstance().InitStoreMetaManager();

    FLAGS_dingo_log_switch_txn_detail = true;
    FLAGS_gc_delete_batch_count = 3;
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  static int64_t Tso2Timestamp(pb::meta::TsoTimestamp tso) {
    return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
  }

  void SetUp() override {}

  void TearDown() override {}

  static inline std::shared_ptr<RocksRawEngine> engine = nullptr;
  static inline std::shared_ptr<Config> config = nullptr;
  static inline int64_t safe_point_ts = 0;
  static inline std::string prefix_start_key;
  static inline std::string prefix_end_key;
  static inline int64_t partition_id = 0x11223344;

  // friend function
  friend void DoGcCore(bool gc_stop, int64_t safe_point_ts);
  friend void DeleteRange();

  friend void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                          int set_tso_position, std::vector<mvcc::ValueFlag> ops,
                          std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                             RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops)>
                              func);
  friend void PrepareDataFunction(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                  RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops);
};

static void DoGcCore(bool gc_stop, int64_t safe_point_ts) {
  // do gc
  butil::Status status;
  std::shared_ptr<Engine> raft_engine;
  std::shared_ptr<Context> ctx;

  std::shared_ptr<GCSafePoint> gc_safe_point = std::make_shared<GCSafePoint>();
  gc_safe_point->SetGcFlagAndSafePointTs(Constant::kDefaultTenantId, gc_stop, safe_point_ts);

  int64_t region_id = 1;

  std::string region_start_key(MvccGcStoreTest::prefix_start_key);
  std::string region_end_key(MvccGcStoreTest::prefix_end_key);

  pb::common::RegionDefinition definition;
  definition.set_id(region_id);
  definition.mutable_range()->set_start_key(region_start_key);
  definition.mutable_range()->set_end_key(region_end_key);
  definition.set_part_id(MvccGcStoreTest::partition_id);
  definition.set_tenant_id(0);

  definition.mutable_index_parameter()->set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_NONE);

  store::RegionPtr region_ptr = store::Region::New(definition);

  ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id);
#if defined(ENABLE_GC_MOCK)
  ctx->SetWriter(MvccGcStoreTest::engine->Writer());
#endif

  Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta()->AddRegion(region_ptr);

  status = TxnEngineHelper::DoGc(MvccGcStoreTest::engine, raft_engine, ctx, MvccGcStoreTest::safe_point_ts,
                                 gc_safe_point, region_start_key, region_end_key);
  Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta()->DeleteRegion(region_id);
}

static void DeleteRange() {
  auto writer = MvccGcStoreTest::engine->Writer();
  // delete range
  {
    pb::common::Range range;
    range.set_start_key(mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer));
    range.set_end_key(mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer));

    writer->KvDeleteRange({Constant::kStoreDataCF}, range);
  }
}

static void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                        int set_tso_position, std::vector<mvcc::ValueFlag> ops,
                        std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                           RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops)>
                            func) {
  auto writer = MvccGcStoreTest::engine->Writer();
  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      MvccGcStoreTest::prefix_start_key = prefix_key_array[start_index];
      MvccGcStoreTest::prefix_end_key = prefix_key_array[end_index];

      for (size_t i = 0; i < end_index; i++) {
        const auto &prefix_key = prefix_key_array[i];
        if (i >= start_index) {
          for (int j = 0; j < count; j++) {
            func(j, physical, logical++, prefix_key, writer, ops);
          }
        }

        physical++;

        if (i == set_tso_position) {
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          MvccGcStoreTest::safe_point_ts = MvccGcStoreTest::Tso2Timestamp(tso);
        }
      }

      if (-1 == set_tso_position) {
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        MvccGcStoreTest::safe_point_ts = MvccGcStoreTest::Tso2Timestamp(tso);
      }
    }
  }
}

static void PrepareDataFunction(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                RawEngine::WriterPtr writer, std::vector<mvcc::ValueFlag> ops) {
  pb::common::KeyValue kv_default;
  pb::common::KeyValue kv_scalar;
  std::vector<pb::common::KeyValue> kv_scalar_speedups;
  pb::common::KeyValue kv_table;
  pb::meta::TsoTimestamp tso;
  tso.set_physical(physical);
  tso.set_logical(logical);
  int64_t start_ts = MvccGcStoreTest::Tso2Timestamp(tso);
  tso.set_logical(++logical);
  int64_t commit_ts = MvccGcStoreTest::Tso2Timestamp(tso);

  std::string default_key = mvcc::Codec::EncodeKey(std::string(prefix_key), commit_ts);

  pb::common::VectorWithId vector_with_id;
  pb::common::Vector vector;
  vector.set_dimension(4);
  vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
  vector.add_float_values(1.0);
  vector.add_float_values(2.0);
  vector.add_float_values(3.0);
  vector.add_float_values(4.0);
  std::string default_value = vector.SerializeAsString();
  std::string scalar_value = vector_with_id.scalar_data().SerializeAsString();
  std::string scalar_speedup_value;
  std::string table_value = vector_with_id.table_data().SerializeAsString();

  mvcc::ValueFlag value_flag = ops[j % ops.size()];

  switch (value_flag) {
    case mvcc::ValueFlag::kPut: {
      kv_default.set_key(default_key);
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, default_value);
      kv_default.set_value(default_value);

      break;
    }
    case mvcc::ValueFlag::kPutTTL: {
      kv_default.set_key(default_key);
      int64_t ttl = Helper::TimestampMs();
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPutTTL, ttl, default_value);
      kv_default.set_value(default_value);

      break;
    }
    case mvcc::ValueFlag::kDelete: {
      kv_default.set_key(default_key);
      default_value.clear();
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kDelete, default_value);
      kv_default.set_value(default_value);

      break;
    }
    default: {
      break;
    }
  }

  writer->KvPut(Constant::kStoreDataCF, kv_default);
}

// safe_point_ts > any data &&  safe_point_ts < any data  first put
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointAtMiddleFirstPutAndGcStop) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, 3,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kPutTTL, mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut},
              PrepareDataFunction);

  DoGcCore(true, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ((6) * FLAGS_gc_delete_batch_count, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// No data
TEST_F(MvccGcStoreTest, DoGcNoData) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, 0, FLAGS_gc_delete_batch_count, -1,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut, mvcc::ValueFlag::kPutTTL},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts > any data &&  safe_point_ts < any data  first put ttl
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointAtMiddleFirstPutTtl) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, 3,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut, mvcc::ValueFlag::kPutTTL},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ((2) * FLAGS_gc_delete_batch_count, count);
  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts > any data &&  safe_point_ts < any data  first put
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointAtMiddleFirstPut) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, 3,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kPutTTL, mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ((2) * FLAGS_gc_delete_batch_count + 4, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts < any data timestamp first put
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointLessThanAnyDataTimestampFirstPut) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 1, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, 0,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kPutTTL, mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ((prefix_key_array.size() - 2) * FLAGS_gc_delete_batch_count, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts > any data timestamp first put
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointGreaterAnyDataTimestampFirstPut) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, -1,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kPutTTL, mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(prefix_key_array.size() - 1, count);
  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts > any data timestamp first put ttl
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointGreaterAnyDataTimestampFirstPutTtl) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, -1,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kDelete, mvcc::ValueFlag::kPut, mvcc::ValueFlag::kPutTTL},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

// safe_point_ts > any data timestamp first delete
TEST_F(MvccGcStoreTest, DoGcNormalGcSafePointGreaterAnyDataTimestampFirstDelete) {
  int64_t vector_id = 1;
  std::vector<std::string> prefix_key_array = {
      std::string() + "r" + std::to_string(partition_id) + "1",
      std::string() + "r" + std::to_string(partition_id) + "2",
      std::string() + "r" + std::to_string(partition_id) + "3",
      std::string() + "r" + std::to_string(partition_id) + "4",
      std::string() + "r" + std::to_string(partition_id) + "5",
      std::string() + "r" + std::to_string(partition_id) + "6",
      std::string() + "r" + std::to_string(partition_id) + "7",
  };
  for (auto &prefix_key : prefix_key_array) {
    prefix_key = VectorCodec::PackageVectorKey(Constant::kExecutorRaw, MvccGcStoreTest::partition_id, vector_id++);
  }
  PrepareData(prefix_key_array, 0, prefix_key_array.size() - 1, FLAGS_gc_delete_batch_count, -1,
              std::vector<mvcc::ValueFlag>{mvcc::ValueFlag::kPut, mvcc::ValueFlag::kPutTTL, mvcc::ValueFlag::kDelete},
              PrepareDataFunction);

  DoGcCore(false, MvccGcStoreTest::safe_point_ts);
#if defined(ENABLE_GC_MOCK)
  auto reader = MvccGcStoreTest::engine->Reader();
  std::shared_ptr<Snapshot> snapshot = MvccGcStoreTest::engine->GetSnapshot();
  int64_t count = 0;
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);

  DeleteRange();
  snapshot = MvccGcStoreTest::engine->GetSnapshot();
  reader->KvCount(Constant::kStoreDataCF, snapshot,
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_start_key, Constant::kMaxVer),
                  mvcc::Codec::EncodeKey(MvccGcStoreTest::prefix_end_key, Constant::kMaxVer), count);
  EXPECT_EQ(0, count);
#else
  DeleteRange();
#endif
}

}  // namespace dingodb
