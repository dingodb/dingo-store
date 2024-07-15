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
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/role.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "config/yaml_config.h"
#include "coordinator/tso_control.h"
#include "engine/bdb_raw_engine.h"
#include "engine/mono_store_engine.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "event/store_state_machine_event.h"
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store_internal.pb.h"
#include "serial/record_encoder.h"
#include "serial/schema/boolean_schema.h"
#include "serial/schema/float_schema.h"
#include "serial/schema/integer_schema.h"
#include "serial/schema/long_schema.h"
#include "serial/schema/string_schema.h"

namespace dingodb {
// DECLARE_string(role);

static const std::string kDefaultCf = "default";

static const std::vector<std::string> kAllCFs = {Constant::kTxnWriteCF, Constant::kTxnDataCF, Constant::kTxnLockCF,
                                                 kDefaultCf, Constant::kStoreMetaCF};

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

const std::string kYamlConfigContent =
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

static std::string StrToHex(std::string str, std::string separator = "") {
  const std::string hex = "0123456789ABCDEF";
  std::stringstream ss;

  for (char i : str) ss << hex[(unsigned char)i >> 4] << hex[(unsigned char)i & 0xf] << separator;

  return ss.str();
}

// rand string
static std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

class TxnPessimisticLockTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    dingodb::Helper::CreateDirectories(kStorePath);
    std::srand(std::time(nullptr));

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));
    SetRole("store");
    ConfigManager::GetInstance().Register(GetRoleName(), config);

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));

    auto raw_bdb_engine = std::make_shared<BdbRawEngine>();
    ASSERT_TRUE(raw_bdb_engine != nullptr);
    auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
    ASSERT_TRUE(listener_factory != nullptr);

    auto ts_provider = mvcc::TsProvider::New(nullptr);

    ASSERT_TRUE(ts_provider->Init());

    auto meta_reader = std::make_shared<MetaReader>(engine);
    auto meta_writer = std::make_shared<MetaWriter>(engine);
    auto store_meta_manager = std::make_shared<StoreMetaManager>(meta_reader, meta_writer);
    ASSERT_TRUE(store_meta_manager->Init());

    auto store_metrics_manager = std::make_shared<StoreMetricsManager>(meta_reader, meta_writer);
    ASSERT_TRUE(store_metrics_manager->Init());

    mono_engine = std::make_shared<MonoStoreEngine>(engine, raw_bdb_engine, listener_factory->Build(), ts_provider,
                                                    store_meta_manager, store_metrics_manager);
    ASSERT_TRUE(mono_engine != nullptr);
    ASSERT_TRUE(mono_engine->Init(config));

    InitRecord();
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    dingodb::Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  static void DeleteRange();
  static void InitRecord();
  static void PrepareData();
  static void SetKvFunction(bool short_value, pb::store::Op write_op, bool include_lock, bool is_pissimistic_lock,
                            pb::common::KeyValue &key_value);
  static void PrepareDataAndLock(bool short_value, pb::store::Op write_op, bool include_lock, bool is_pissimistic_lock,
                                 std::unordered_map<std::string, std::string> &kvs,
                                 std::unordered_map<std::string, bool> &lock_keys);

  static inline std::shared_ptr<RocksRawEngine> engine;
  static inline std::shared_ptr<MonoStoreEngine> mono_engine;
  static inline int64_t start_ts = 0;
  static inline int64_t end_ts = 0;
  static inline int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
  static inline int64_t logical = 0;
  static inline int64_t lock_ttl = 2036211681000;  // 2034-07-11 14:21:21
  // static inline std::unordered_map<std::string, std::string> kvs;
  static inline std::shared_ptr<RecordEncoder> record_encoder;
};

static int64_t Tso2Timestamp(pb::meta::TsoTimestamp tso) {
  return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
}

void TxnPessimisticLockTest::SetKvFunction(bool short_value, pb::store::Op write_op, bool include_lock,
                                           bool is_pissimistic_lock, pb::common::KeyValue &key_value) {
  pb::common::KeyValue kv_write;
  pb::meta::TsoTimestamp tso;
  tso.set_physical(physical);
  tso.set_logical(logical);
  int64_t target_start_ts = Tso2Timestamp(tso);
  tso.set_logical(++logical);
  int64_t commit_ts = Tso2Timestamp(tso);

  if (0 == start_ts) {
    start_ts = target_start_ts;
  }

  end_ts = commit_ts;

  std::string write_key = mvcc::Codec::EncodeKey(std::string(key_value.key()), commit_ts);

  pb::store::WriteInfo write_info;
  write_info.set_start_ts(start_ts);
  write_info.set_op(write_op);
  if (short_value) {
    write_info.set_short_value(key_value.value());
  }

  kv_write.set_key(write_key);
  kv_write.set_value(write_info.SerializeAsString());

  engine->Writer()->KvPut(Constant::kTxnWriteCF, kv_write);

  if (!short_value) {
    pb::common::KeyValue kv_data;
    std::string data_key = mvcc::Codec::EncodeKey(std::string(key_value.key()), start_ts);
    kv_data.set_key(data_key);
    kv_data.set_value(key_value.value());

    butil::Status ok = engine->Writer()->KvPut(Constant::kTxnDataCF, kv_data);

    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  if (include_lock) {
    pb::common::KeyValue lock_data;
    lock_data.set_key(mvcc::Codec::EncodeKey(key_value.key(), Constant::kLockVer));
    pb::store::LockInfo lock_info;
    lock_info.set_primary_lock(key_value.key());
    lock_info.set_lock_ts(start_ts);
    if (is_pissimistic_lock) {
      lock_info.set_for_update_ts(start_ts);
    }

    lock_info.set_key(key_value.key());
    lock_info.set_lock_ttl(lock_ttl);
    lock_info.set_lock_type(pb::store::Op::Lock);
    lock_data.set_value(lock_info.SerializeAsString());

    butil::Status ok = engine->Writer()->KvPut(Constant::kTxnLockCF, lock_data);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

void TxnPessimisticLockTest::PrepareDataAndLock(bool short_value, pb::store::Op write_op, bool include_lock,
                                                bool is_pissimistic_lock,
                                                std::unordered_map<std::string, std::string> &kvs,
                                                std::unordered_map<std::string, bool> &lock_keys) {
  // 1
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(false);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(22);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(22.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(2200);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(22.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_222222"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder->Encode('r', record, *key_value.mutable_key(), *key_value.mutable_value());

    EXPECT_EQ(ret, 0);

    SetKvFunction(short_value, write_op, include_lock, is_pissimistic_lock, key_value);
    kvs[key_value.key()] = key_value.value();
    lock_keys[key_value.key()] = true;
  }

  // 2
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(33);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(33.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(3300);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(33.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_333333"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder->Encode('r', record, *key_value.mutable_key(), *key_value.mutable_value());

    EXPECT_EQ(ret, 0);

    SetKvFunction(short_value, write_op, include_lock, is_pissimistic_lock, key_value);
    kvs[key_value.key()] = key_value.value();
    lock_keys[key_value.key()] = true;
  }
}
void TxnPessimisticLockTest::InitRecord() {
  int schema_version = 1;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas;
  long common_id = 1;  // NOLINT

  schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

  schemas->reserve(6);

  std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema = std::make_shared<DingoSchema<std::optional<bool>>>();
  bool_schema->SetIsKey(true);
  bool_schema->SetAllowNull(true);
  bool_schema->SetIndex(0);

  schemas->emplace_back(std::move(bool_schema));

  std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
      std::make_shared<DingoSchema<std::optional<int32_t>>>();
  int_schema->SetIsKey(false);
  int_schema->SetAllowNull(true);
  int_schema->SetIndex(1);
  schemas->emplace_back(std::move(int_schema));

  std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
      std::make_shared<DingoSchema<std::optional<float>>>();
  float_schema->SetIsKey(false);
  float_schema->SetAllowNull(true);
  float_schema->SetIndex(2);
  schemas->emplace_back(std::move(float_schema));

  std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
      std::make_shared<DingoSchema<std::optional<int64_t>>>();
  long_schema->SetIsKey(false);
  long_schema->SetAllowNull(true);
  long_schema->SetIndex(3);
  schemas->emplace_back(std::move(long_schema));

  std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
      std::make_shared<DingoSchema<std::optional<double>>>();
  double_schema->SetIsKey(true);
  double_schema->SetAllowNull(true);
  double_schema->SetIndex(4);
  schemas->emplace_back(std::move(double_schema));

  std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
      std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  string_schema->SetIsKey(true);
  string_schema->SetAllowNull(true);
  string_schema->SetIndex(5);
  schemas->emplace_back(std::move(string_schema));
  record_encoder = std::make_shared<RecordEncoder>(schema_version, schemas, common_id);
}

void TxnPessimisticLockTest::DeleteRange() {
  const std::string &cf_name = kDefaultCf;
  auto writer = engine->Writer();

  pb::common::Range range;

  std::string my_min_key(Helper::GenMinStartKey());
  std::string my_max_key(Helper::GenMaxStartKey());

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  range.set_start_key(my_min_key);
  range.set_end_key(Helper::PrefixNext(my_max_key));

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(Constant::kTxnWriteCF, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> delete_kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, delete_kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : delete_kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(Constant::kTxnDataCF, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> delete_kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, delete_kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : delete_kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(Constant::kTxnLockCF, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> delete_kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, delete_kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : delete_kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

TEST_F(TxnPessimisticLockTest, KvDeleteRangeBefore) { DeleteRange(); }

// todo(yangjundong)
TEST_F(TxnPessimisticLockTest, PessimisticLock) {
  GTEST_SKIP() << "not impl PessimisticLock ";
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // Normal

  // Op not correct

  // Lock confilict

  // Write confilct

  // Rollback

  // Duplicated

  // Rollback

  // LockTypeNotMatch

  // Acquire lock on a prewritten key should fail

  // Acquire lock on a committed key should fail.

  // Pessimistic prewrite on a committed key should fail.

  // Acquire lock when there is lock with different for_update_ts.

  // lock key is Deleted.

  // Over write lock info(update lock info)

  // Duplicated command
}

TEST_F(TxnPessimisticLockTest, PessimisticLockReturnValue) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // normal
  {
    bool short_value = false;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(mutations.size(), expected_kvs.size());

    for (auto const &kv : expected_kvs) {
      auto found = kvs.find(kv.key());
      EXPECT_NE(found, kvs.end());
      if (found != kvs.end()) {
        EXPECT_EQ(kv.value(), found->second);
      }
    }
    DeleteRange();
  }

  // short value
  {
    bool short_value = true;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);
    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(mutations.size(), expected_kvs.size());
    for (auto const &kv : expected_kvs) {
      auto found = kvs.find(kv.key());
      EXPECT_NE(found, kvs.end());
      if (found != kvs.end()) {
        EXPECT_EQ(kv.value(), found->second);
      }
    }
    DeleteRange();
  }

  // Lock not exist key, return value is empty.
  {
    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;

    std::string primary_lock = "not_exist";
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    bool return_values = true;

    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Lock);
    mutation.set_key("not_exist");
    mutations.emplace_back(mutation);

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);

    EXPECT_EQ(mutations.size(), expected_kvs.size());
    EXPECT_EQ(expected_kvs[0].key(), mutations[0].key());
    EXPECT_EQ(expected_kvs[0].value(), "");
    DeleteRange();
  }

  // WriteConflict
  {
    bool short_value = false;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    EXPECT_NE(kvs.begin(), kvs.end());
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = start_ts;
    int64_t target_for_update_ts = start_ts;
    int64_t target_lock_ttl = lock_ttl;
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(expected_kvs.size(), 0);
    EXPECT_GT(response.txn_result_size(), 0);

    for (auto const &txn_result : response.txn_result()) {
      EXPECT_EQ(txn_result.write_conflict().reason(),
                ::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_PessimisticRetry);
    }
    DeleteRange();
  }

  // KeyIsLocked
  {
    bool short_value = false;
    bool include_lock = true;
    bool is_pissimistic_lock = true;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    EXPECT_NE(kvs.begin(), kvs.end());
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(expected_kvs.size(), 0);
    EXPECT_GT(response.txn_result_size(), 0);

    for (auto const &txn_result : response.txn_result()) {
      auto found = lock_keys.find(txn_result.locked().key());
      EXPECT_NE(found, lock_keys.end());
    }
    DeleteRange();
  }

  // Lock type not match
  {
    bool short_value = false;
    bool include_lock = true;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    EXPECT_NE(kvs.begin(), kvs.end());
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(expected_kvs.size(), 0);
    EXPECT_GT(response.txn_result_size(), 0);

    for (auto const &txn_result : response.txn_result()) {
      auto found = lock_keys.find(txn_result.locked().key());
      EXPECT_NE(found, lock_keys.end());
    }
    DeleteRange();
  }

  // lock key is Deleted, return value is empty.
  {
    bool short_value = false;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Delete, include_lock, is_pissimistic_lock, kvs,
                       lock_keys);

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 1;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(mutations.size(), expected_kvs.size());
    for (auto const &kv : expected_kvs) {
      auto found = kvs.find(kv.key());
      EXPECT_NE(found, kvs.end());
      if (found != kvs.end()) {
        EXPECT_EQ(kv.value(), "");
      }
    }
    DeleteRange();
  }

  // Over write lock info(update lock info)
  {
    // prepare data
    bool short_value = false;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);
    {
      butil::Status ok;
      pb::store::TxnPessimisticLockResponse response;
      auto ctx = std::make_shared<Context>();
      ctx->SetRegionId(region_id);
      ctx->SetCfName(Constant::kStoreDataCF);
      ctx->SetResponse(&response);
      std::vector<pb::store::Mutation> mutations;
      std::vector<pb::common::KeyValue> expected_kvs;
      std::string primary_lock = kvs.begin()->first;
      int64_t target_start_ts = end_ts + 1;
      int64_t target_for_update_ts = end_ts + 10;
      int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
      bool return_values = true;

      for (auto const &kv : kvs) {
        pb::store::Mutation mutation;
        mutation.set_op(::dingodb::pb::store::Op::Lock);
        mutation.set_key(kv.first);
        mutations.emplace_back(mutation);
      }

      auto status =
          TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                           target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

      EXPECT_EQ(status.ok(), true);
      EXPECT_EQ(mutations.size(), expected_kvs.size());
      for (auto const &kv : expected_kvs) {
        auto found = kvs.find(kv.key());
        EXPECT_NE(found, kvs.end());
        if (found != kvs.end()) {
          EXPECT_EQ(kv.value(), found->second);
        }
      }
    }

    butil::Status ok;
    pb::store::TxnPessimisticLockResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::vector<pb::common::KeyValue> expected_kvs;
    std::string primary_lock = kvs.begin()->first;
    int64_t target_start_ts = end_ts + 1;
    int64_t target_for_update_ts = end_ts + 20;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    bool return_values = true;

    for (auto const &kv : kvs) {
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Lock);
      mutation.set_key(kv.first);
      mutations.emplace_back(mutation);
    }

    auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                   target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(mutations.size(), expected_kvs.size());
    for (auto const &kv : expected_kvs) {
      auto found = kvs.find(kv.key());
      EXPECT_NE(found, kvs.end());
      if (found != kvs.end()) {
        EXPECT_EQ(kv.value(), found->second);
      }
    }

    DeleteRange();
  }

  // Duplicated command
  {
    // prepare data
    bool short_value = false;
    bool include_lock = false;
    bool is_pissimistic_lock = false;
    std::unordered_map<std::string, std::string> kvs;
    std::unordered_map<std::string, bool> lock_keys;
    PrepareDataAndLock(short_value, ::dingodb::pb::store::Op::Put, include_lock, is_pissimistic_lock, kvs, lock_keys);

    {
      butil::Status ok;
      pb::store::TxnPessimisticLockResponse response;
      auto ctx = std::make_shared<Context>();
      ctx->SetRegionId(region_id);
      ctx->SetCfName(Constant::kStoreDataCF);
      ctx->SetResponse(&response);
      std::vector<pb::store::Mutation> mutations;
      std::vector<pb::common::KeyValue> expected_kvs;
      std::string primary_lock = kvs.begin()->first;
      int64_t target_start_ts = end_ts + 1;
      int64_t target_for_update_ts = end_ts + 10;
      int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
      bool return_values = true;

      for (auto const &kv : kvs) {
        pb::store::Mutation mutation;
        mutation.set_op(::dingodb::pb::store::Op::Lock);
        mutation.set_key(kv.first);
        mutations.emplace_back(mutation);
      }

      auto status =
          TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                           target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

      EXPECT_EQ(status.ok(), true);
      EXPECT_EQ(mutations.size(), expected_kvs.size());
      for (auto const &kv : expected_kvs) {
        auto found = kvs.find(kv.key());
        EXPECT_NE(found, kvs.end());
        if (found != kvs.end()) {
          EXPECT_EQ(kv.value(), found->second);
        }
      }
    }

    {
      butil::Status ok;
      pb::store::TxnPessimisticLockResponse response;
      auto ctx = std::make_shared<Context>();
      ctx->SetRegionId(region_id);
      ctx->SetCfName(Constant::kStoreDataCF);
      ctx->SetResponse(&response);
      std::vector<pb::store::Mutation> mutations;
      std::vector<pb::common::KeyValue> expected_kvs;
      std::string primary_lock = kvs.begin()->first;
      int64_t target_start_ts = end_ts + 1;
      int64_t target_for_update_ts = end_ts + 10;
      int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
      bool return_values = true;

      for (auto const &kv : kvs) {
        pb::store::Mutation mutation;
        mutation.set_op(::dingodb::pb::store::Op::Lock);
        mutation.set_key(kv.first);
        mutations.emplace_back(mutation);
      }

      auto status =
          TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                           target_lock_ttl, target_for_update_ts, return_values, expected_kvs);

      EXPECT_EQ(status.ok(), true);
      EXPECT_EQ(mutations.size(), expected_kvs.size());
      for (auto const &kv : expected_kvs) {
        auto found = kvs.find(kv.key());
        EXPECT_NE(found, kvs.end());
        if (found != kvs.end()) {
          EXPECT_EQ(kv.value(), found->second);
        }
      }
    }

    DeleteRange();
  }
}

TEST_F(TxnPessimisticLockTest, KvDeleteRange) { DeleteRange(); }

}  // namespace dingodb
