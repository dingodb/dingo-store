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
#include "gtest/gtest.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

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

class TxnPreWriteTest : public testing::Test {
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
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    dingodb::Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  static void DeleteRange();
  static void MustUnlock(std::string key);
  static void MustLocked(std::string key, int64_t start_ts);
  static void MustPessimisticLocked(std::string key, int64_t start_ts, int64_t for_update_ts);
  static void MustGet(std::string key, int64_t start_ts, std::string expect_value);
  static void MustGetCommitTs(std::string key, int64_t start_ts, int64_t expect_ts);
  static void MustGetCommitTsNone(std::string key, int64_t start_ts);
  static void MustAcquirePessimisticlock(std::shared_ptr<RocksRawEngine> engine,
                                         std::shared_ptr<MonoStoreEngine> mono_engine, store::RegionPtr region,
                                         std::string primary_lock, std::string key, int64_t start_ts,
                                         int64_t for_update_ts);
  static void MustPessimisticRollback(store::RegionPtr region, int64_t start_ts, int64_t lock_for_update_ts,
                                      const std::vector<std::string> &keys);
  static void MustCommit(store::RegionPtr region, int64_t start_ts, int64_t commit_ts,
                         const std::vector<std::string> &keys);

  static inline std::shared_ptr<RocksRawEngine> engine;
  static inline std::shared_ptr<MonoStoreEngine> mono_engine;
  static inline int64_t lock_ttl = 2036211681000;  // 2034-07-11 14:21:21
};
void TxnPreWriteTest::MustCommit(store::RegionPtr region, int64_t start_ts, int64_t commit_ts,
                                 const std::vector<std::string> &keys) {
  pb::store::TxnCommitResponse response;
  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(region->Id());
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetResponse(&response);

  auto status = TxnEngineHelper::Commit(engine, mono_engine, ctx, region, start_ts, commit_ts, keys);
  EXPECT_EQ(status.ok(), true);
  EXPECT_EQ(response.txn_result().has_locked(), false);
  EXPECT_EQ(response.txn_result().has_write_conflict(), false);
}

void TxnPreWriteTest::MustAcquirePessimisticlock(std::shared_ptr<RocksRawEngine> engine,
                                                 std::shared_ptr<MonoStoreEngine> mono_engine, store::RegionPtr region,
                                                 std::string primary_lock, std::string key, int64_t start_ts,
                                                 int64_t for_update_ts) {
  pb::store::TxnPessimisticLockResponse response;
  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(region->Id());
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetResponse(&response);

  std::vector<pb::store::Mutation> mutations;
  std::vector<pb::common::KeyValue> expected_kvs;

  int64_t target_start_ts = start_ts;
  int64_t target_for_update_ts = for_update_ts;
  int64_t target_lock_ttl = 0;
  bool return_values = false;
  pb::store::Mutation mutation;
  mutation.set_op(::dingodb::pb::store::Op::Lock);
  mutation.set_key(key);
  mutations.emplace_back(mutation);
  auto status = TxnEngineHelper::PessimisticLock(engine, mono_engine, ctx, mutations, primary_lock, target_start_ts,
                                                 target_lock_ttl, target_for_update_ts, return_values, expected_kvs);
  EXPECT_EQ(status.ok(), true);
  EXPECT_EQ(response.txn_result_size(), 0);
}

void TxnPreWriteTest::MustPessimisticRollback(store::RegionPtr region, int64_t start_ts, int64_t for_update_ts,
                                              const std::vector<std::string> &keys) {
  pb::store::TxnPessimisticRollbackResponse response;
  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(region->Id());
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetResponse(&response);

  auto status = TxnEngineHelper::PessimisticRollback(engine, mono_engine, ctx, region, start_ts, for_update_ts, keys);
  EXPECT_EQ(status.ok(), true);
  EXPECT_EQ(response.txn_result_size(), 0);
}
void TxnPreWriteTest::MustGetCommitTsNone(std::string key, int64_t start_ts) {
  pb::store::TxnResultInfo txn_result_info;
  std::set<int64_t> resolved_locks;
  auto isolation_level = pb::store::SnapshotIsolation;
  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  EXPECT_TRUE(ret_init.ok());

  auto write_iter = txn_reader.GetWriteIter();
  EXPECT_NE(write_iter, nullptr);

  pb::store::WriteInfo write_info;

  int64_t commit_ts = 0;
  auto ret2 = txn_reader.GetWriteInfo(0, start_ts, 0, key, false, true, true, write_info, commit_ts);
  EXPECT_EQ(commit_ts, 0);
}

void TxnPreWriteTest::MustGetCommitTs(std::string key, int64_t start_ts, int64_t expect_ts) {
  pb::store::TxnResultInfo txn_result_info;
  std::set<int64_t> resolved_locks;
  auto isolation_level = pb::store::SnapshotIsolation;
  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  EXPECT_TRUE(ret_init.ok());

  auto write_iter = txn_reader.GetWriteIter();
  EXPECT_NE(write_iter, nullptr);

  pb::store::WriteInfo write_info;

  int64_t commit_ts = 0;
  auto ret2 = txn_reader.GetWriteInfo(0, start_ts, 0, key, false, true, true, write_info, commit_ts);
  EXPECT_EQ(write_info.op(), pb::store::Op::Put);
  EXPECT_EQ(commit_ts, expect_ts);
}

void TxnPreWriteTest::MustGet(std::string key, int64_t start_ts, std::string expect_value) {
  pb::store::TxnResultInfo txn_result_info;
  std::set<int64_t> resolved_locks;
  auto isolation_level = pb::store::SnapshotIsolation;
  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  EXPECT_TRUE(ret_init.ok());

  auto write_iter = txn_reader.GetWriteIter();
  EXPECT_NE(write_iter, nullptr);

  pb::common::KeyValue kv;
  kv.set_key(key);

  pb::store::LockInfo lock_info;
  auto ret = txn_reader.GetLockInfo(key, lock_info);
  EXPECT_TRUE(ret.ok());

  auto is_lock_conflict =
      TxnEngineHelper::CheckLockConflict(lock_info, isolation_level, start_ts, resolved_locks, txn_result_info);
  EXPECT_FALSE(is_lock_conflict);

  EXPECT_TRUE((isolation_level == pb::store::IsolationLevel::SnapshotIsolation) ||
              (isolation_level == pb::store::IsolationLevel::ReadCommitted));
  int64_t iter_start_ts;
  bool is_valid = false;
  if (isolation_level == pb::store::IsolationLevel::SnapshotIsolation) {
    iter_start_ts = start_ts;
  } else if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
    iter_start_ts = Constant::kMaxVer;
  }

  IteratorOptions iter_options;
  iter_options.lower_bound = mvcc::Codec::EncodeKey(key, iter_start_ts);
  iter_options.upper_bound = mvcc::Codec::EncodeKey(key, 0);

  // check isolation level and return value
  write_iter->Seek(iter_options.lower_bound);
  while (write_iter->Valid() && write_iter->Key() < iter_options.upper_bound) {
    EXPECT_TRUE(write_iter->Key().length() > 8);

    std::string write_key;
    int64_t write_ts;
    mvcc::Codec::DecodeKey(write_iter->Key(), write_key, write_ts);

    bool is_valid = false;
    if (isolation_level == pb::store::IsolationLevel::SnapshotIsolation) {
      if (write_ts <= start_ts) {
        is_valid = true;
      }
    } else if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
      is_valid = true;
    }

    if (is_valid) {
      // write_ts <= start_ts, return write_info
      pb::store::WriteInfo write_info;
      auto ret = write_info.ParseFromArray(write_iter->Value().data(), write_iter->Value().size());
      EXPECT_TRUE(ret);
      // check the op type of write_info
      if (write_info.op() == pb::store::Op::Rollback) {
        // goto next write line
        write_iter->Next();
        continue;
      } else if (write_info.op() == pb::store::Op::Delete) {
        // if op is delete, value is null
        kv.set_value(std::string());
        break;
      } else if (write_info.op() != pb::store::Op::Put) {
        EXPECT_TRUE(false);
        std::cout << "[txn]BatchGet meet invalid write_info.op: " << write_info.op()
                  << ", key: " << Helper::StringToHex(key) << ", write_info: " << write_info.ShortDebugString();
      }

      if (!write_info.short_value().empty()) {
        kv.set_value(write_info.short_value());
        break;
      }

      auto ret1 = txn_reader.GetDataValue(mvcc::Codec::EncodeKey(key, write_info.start_ts()), *kv.mutable_value());
      if (!ret1.ok() && ret1.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        std::cout << "[txn]BatchGet read data failed, key: " << Helper::StringToHex(key)
                  << ", status: " << ret1.error_str();
      } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        std::cout << "[txn]BatchGet read data failed, data is illegally not found, key: " << Helper::StringToHex(key)
                  << ", status: " << ret1.error_str() << ", ts: " << write_info.start_ts();
      }
      break;
    } else {
    }

    write_iter->Next();
  }
  EXPECT_EQ(kv.value(), expect_value);
}
void TxnPreWriteTest::MustPessimisticLocked(std::string key, int64_t start_ts, int64_t for_update_ts) {
  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  EXPECT_TRUE(ret_init.ok());

  pb::store::LockInfo lock_info;
  auto ret = txn_reader.GetLockInfo(key, lock_info);
  EXPECT_TRUE(ret.ok());
  EXPECT_EQ(lock_info.lock_ts(), start_ts);
  EXPECT_EQ(lock_info.for_update_ts(), for_update_ts);
}

void TxnPreWriteTest::MustLocked(std::string key, int64_t start_ts) {
  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  EXPECT_TRUE(ret_init.ok());

  pb::store::LockInfo lock_info;
  auto ret = txn_reader.GetLockInfo(key, lock_info);
  EXPECT_TRUE(ret.ok());
  EXPECT_EQ(lock_info.lock_ts(), start_ts);
}

void TxnPreWriteTest::MustUnlock(std::string key) {
  auto reader = engine->Reader();
  std::string lock_value;
  auto status = reader->KvGet(Constant::kTxnLockCF, mvcc::Codec::EncodeKey(key, Constant::kLockVer), lock_value);
  EXPECT_EQ(status.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
}

void TxnPreWriteTest::DeleteRange() {
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

TEST_F(TxnPreWriteTest, KvDeleteRangeBefore) { DeleteRange(); }

TEST_F(TxnPreWriteTest, PreWrite) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // optismistic prewrite
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }
}

TEST_F(TxnPreWriteTest, PessimisticPreWrite) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // pessimistic prewrite
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, target_start_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_start_ts);

    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }

  // test for update ts
  {
    butil::Status ok;

    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    auto lambda_test_normal = [&](int64_t start_ts, int64_t lock_for_update_ts, int64_t expected_for_update_ts,
                                  bool success, int64_t commit_ts) {
      pb::store::TxnPrewriteResponse response;
      ctx->SetResponse(&response);
      MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, start_ts, lock_for_update_ts);
      MustPessimisticLocked(key, start_ts, lock_for_update_ts);
      for_update_ts_checks.insert_or_assign(0, expected_for_update_ts);
      if (success) {
        auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, start_ts,
                                                target_lock_ttl, txn_size, try_one_pc, max_commit_ts,
                                                pessimistic_checks, for_update_ts_checks, lock_extra_datas);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(response.one_pc_commit_ts(), 0);
        EXPECT_EQ(response.txn_result_size(), 0);
        MustLocked(key, start_ts);
        // test idempotency
        auto status1 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, start_ts,
                                                 target_lock_ttl, txn_size, try_one_pc, max_commit_ts,
                                                 pessimistic_checks, for_update_ts_checks, lock_extra_datas);
        EXPECT_EQ(status1.ok(), true);
        EXPECT_EQ(response.one_pc_commit_ts(), 0);
        EXPECT_EQ(response.txn_result_size(), 0);
        MustLocked(key, start_ts);
        std::vector<std::string> commit_keys;
        commit_keys.push_back(key);
        MustCommit(region, start_ts, commit_ts, commit_keys);
        MustUnlock(key);
      } else {
        auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, start_ts,
                                                target_lock_ttl, txn_size, try_one_pc, max_commit_ts,
                                                pessimistic_checks, for_update_ts_checks, lock_extra_datas);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(response.one_pc_commit_ts(), 0);
        EXPECT_GT(response.txn_result_size(), 0);
        for (auto const &txn_result : response.txn_result()) {
          EXPECT_EQ(txn_result.locked().key(), key);
          EXPECT_EQ(txn_result.locked().lock_ts(), start_ts);
          EXPECT_EQ(txn_result.locked().for_update_ts(), lock_for_update_ts);
        }
        std::vector<std::string> rollback_keys;
        rollback_keys.push_back(key);
        MustPessimisticRollback(region, start_ts, lock_for_update_ts, rollback_keys);
        MustUnlock(key);
      }
    };
    lambda_test_normal(10, 10, 10, true, 19);
    //   Note that the `for_update_ts` field in prewrite request is not guaranteed to
    //   be greater or equal to the max for_update_ts that has been written to
    //   a pessimistic lock during the transaction.
    lambda_test_normal(20, 20, 24, false, 0);
    lambda_test_normal(30, 35, 35, true, 39);
    lambda_test_normal(40, 45, 40, false, 0);
    lambda_test_normal(50, 55, 51, false, 0);
    DeleteRange();
  }
}

TEST_F(TxnPreWriteTest, RepeatedPreWrite) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // pessimistic reapeated prewrite
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, target_start_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_start_ts);
    // normal prewrite
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);

    // repeat prewrite
    auto status1 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                             target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                             for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status1.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }

  // optismistic reapeated prewrite
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);

    // normal prewrite
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);

    // repeat prewrite
    try_one_pc = true;
    auto status1 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                             target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                             for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status1.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }
}
TEST_F(TxnPreWriteTest, PreWriteWithOnePC) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // optismistic prewrite1pc
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = true;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), target_start_ts + 1);
    MustUnlock(key);
    MustGet(key, target_start_ts + 2, value);
    MustGetCommitTs(key, target_start_ts + 1, target_start_ts + 1);
    DeleteRange();
  }

  // Test the idempotency of prewrite when falling back to 2PC.
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 50;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = true;
    int64_t max_commit_ts = 20;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);
    for (int i = 0; i < 2; i++) {
      auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock,
                                              target_start_ts, target_lock_ttl, txn_size, try_one_pc, max_commit_ts,
                                              pessimistic_checks, for_update_ts_checks, lock_extra_datas);
      EXPECT_EQ(status.ok(), true);
      EXPECT_EQ(response.one_pc_commit_ts(), 0);
      MustLocked(key, target_start_ts);
    }
    DeleteRange();
  }
  // Test a 1PC request should not be partially written when encounters error on
  // the halfway. If some of the keys are successfully written as committed state,
  // the atomicity will be broken.
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string key2 = "maru_test_key2";
    std::string value2 = "value2";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);
    // Lock key.
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.txn_result_size(), 0);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);

    try_one_pc = true;
    target_start_ts = 20;
    pb::store::Mutation mutation2;
    mutation2.set_op(::dingodb::pb::store::Op::Put);
    mutation2.set_key(key2);
    mutation2.set_value(value2);
    mutations.emplace_back(mutation2);
    txn_size = 2;
    for_update_ts_checks.insert_or_assign(1, 0);
    lock_extra_datas.insert_or_assign(1, "");
    pessimistic_checks.push_back(0);
    // Try 1PC on the two keys and it will fail on the second one.
    auto status2 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                             target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                             for_update_ts_checks, lock_extra_datas);
    EXPECT_TRUE(status2.ok());
    for (auto const &txn_result : response.txn_result()) {
      EXPECT_EQ(txn_result.locked().key(), key);
      EXPECT_EQ(txn_result.locked().lock_ts(), 10);
    }
    MustUnlock(key2);
    MustLocked(key, 10);
    MustGetCommitTsNone(key, target_start_ts + 1);
    MustGetCommitTsNone(key2, target_start_ts + 1);
    DeleteRange();
  }
}

TEST_F(TxnPreWriteTest, PessimisticPreWriteWithOnePC) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // pessimistic prewrite1pc
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = true;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, target_start_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_start_ts);

    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), target_start_ts + 1);
    MustUnlock(key);
    MustGet(key, target_start_ts + 2, value);
    MustGetCommitTs(key, target_start_ts + 1, target_start_ts + 1);
    DeleteRange();
  }

  // one_pc_commit_ts = max(start_ts, for_update_ts)+1
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_for_update_ts = 12;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = true;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);

    for_update_ts_checks.insert_or_assign(0, target_for_update_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_for_update_ts);

    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), target_for_update_ts + 1);
    MustUnlock(key);
    MustGet(key, target_for_update_ts + 1, value);
    MustGetCommitTs(key, target_for_update_ts + 1, target_for_update_ts + 1);
    DeleteRange();
  }

  // max_commit_ts less than final_commit_ts fallback to 2pc
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_for_update_ts = 12;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = true;
    int64_t max_commit_ts = 5;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);

    for_update_ts_checks.insert_or_assign(0, target_for_update_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_for_update_ts);

    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.txn_result_size(), 0);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_for_update_ts + 1);
    DeleteRange();
  }

  // Test a 1PC request should not be partially written when encounters error on
  // the halfway. If some of the keys are successfully written as committed state,
  // the atomicity will be broken.
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string key2 = "maru_test_key2";
    std::string value2 = "value2";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t expect_first_start_ts = target_start_ts;
    int64_t target_for_update_ts = 12;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;

    {
      // optismistic prewrite
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Put);
      mutation.set_key(key);
      mutation.set_value(value);
      mutations.emplace_back(mutation);
      for_update_ts_checks.insert_or_assign(0, 0);
      lock_extra_datas.insert_or_assign(0, "");
      pessimistic_checks.push_back(0);
      auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock,
                                              target_start_ts, target_lock_ttl, txn_size, try_one_pc, max_commit_ts,
                                              pessimistic_checks, for_update_ts_checks, lock_extra_datas);
      EXPECT_EQ(status.ok(), true);
      EXPECT_EQ(response.txn_result_size(), 0);
      EXPECT_EQ(response.one_pc_commit_ts(), 0);
      MustLocked(key, target_start_ts);
      MustGetCommitTsNone(key, target_start_ts + 1);
    }
    {
      // pessimistic prewrite
      txn_size = 2;
      try_one_pc = true;
      target_start_ts = 50;
      target_for_update_ts = 60;
      pb::store::Mutation mutation;
      mutation.set_op(::dingodb::pb::store::Op::Put);
      mutation.set_key(key2);
      mutation.set_value(value2);
      mutations.emplace_back(mutation);
      for_update_ts_checks.insert_or_assign(1, target_for_update_ts);
      lock_extra_datas.insert_or_assign(1, "");
      pessimistic_checks.push_back(1);
    }
    // lock key2
    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key2, target_start_ts, target_for_update_ts);

    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    EXPECT_GT(response.txn_result_size(), 0);
    for (auto const &txn_result : response.txn_result()) {
      EXPECT_EQ(txn_result.locked().key(), key);
      EXPECT_EQ(txn_result.locked().lock_ts(), expect_first_start_ts);
    }
    MustLocked(key, expect_first_start_ts);
    MustPessimisticLocked(key2, target_start_ts, target_for_update_ts);
    MustGetCommitTsNone(key, target_for_update_ts + 1);
    MustGetCommitTsNone(key2, target_for_update_ts + 1);
    DeleteRange();
  }
}

TEST_F(TxnPreWriteTest, RepeatedPreWriteWithOnePC) {
  // create region for test
  auto region_id = 373;
  auto region = store::Region::New(region_id);
  region->SetState(pb::common::StoreRegionState::NORMAL);
  auto store_region_meta = mono_engine->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->AddRegion(region);
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  mono_engine->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);

  // pessimistic reapeated prewrite1pc
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, target_start_ts);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(1);

    MustAcquirePessimisticlock(engine, mono_engine, region, primary_lock, key, target_start_ts, target_start_ts);
    // normal prewrite
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);

    // repeat prewrite 1pc, fallback to 2pc
    try_one_pc = true;
    auto status1 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                             target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                             for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status1.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }

  // optismistic reapeated prewrite1pc
  {
    butil::Status ok;
    pb::store::TxnPrewriteResponse response;
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetResponse(&response);
    std::vector<pb::store::Mutation> mutations;
    std::string key = "maru_test_key1";
    std::string value = "value1";
    std::string primary_lock = key;
    int64_t target_start_ts = 10;
    int64_t target_lock_ttl = lock_ttl;  // 2034-07-11 14:21:21
    int64_t txn_size = 1;
    bool try_one_pc = false;
    int64_t max_commit_ts = 0;
    std::vector<int64_t> pessimistic_checks;
    std::map<int64_t, int64_t> for_update_ts_checks;
    std::map<int64_t, std::string> lock_extra_datas;
    pb::store::Mutation mutation;
    mutation.set_op(::dingodb::pb::store::Op::Put);
    mutation.set_key(key);
    mutation.set_value(value);
    mutations.emplace_back(mutation);
    for_update_ts_checks.insert_or_assign(0, 0);
    lock_extra_datas.insert_or_assign(0, "");
    pessimistic_checks.push_back(0);

    // normal prewrite
    auto status = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                            target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                            for_update_ts_checks, lock_extra_datas);

    EXPECT_EQ(status.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);

    // repeat prewrite 1pc, fallback to 2pc
    try_one_pc = true;
    auto status1 = TxnEngineHelper::Prewrite(engine, mono_engine, ctx, region, mutations, primary_lock, target_start_ts,
                                             target_lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                             for_update_ts_checks, lock_extra_datas);
    EXPECT_EQ(status1.ok(), true);
    EXPECT_EQ(response.one_pc_commit_ts(), 0);
    MustLocked(key, target_start_ts);
    MustGetCommitTsNone(key, target_start_ts + 1);
    DeleteRange();
  }
}

TEST_F(TxnPreWriteTest, KvDeleteRange) { DeleteRange(); }

}  // namespace dingodb
