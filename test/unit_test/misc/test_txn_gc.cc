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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
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
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

static const std::vector<std::string> kAllCFs = {Constant::kTxnWriteCF, Constant::kTxnDataCF, Constant::kTxnLockCF,
                                                 Constant::kStoreDataCF};

static const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'o', 'v', 'w', 'x',
                                 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

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

static void DoGcCore(bool gc_stop, int64_t safe_point_ts, bool force_gc_stop);
static void DeleteRange();
static void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                        int set_tso_position,
                        std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                           RawEngine::WriterPtr writer)>
                            func);

[[deprecated]] static void PrepareDataForDoGcDataNormalNoLock();
[[deprecated]] static void PrepareDataForDoGcDataNormalGcSafePointWarning();
[[deprecated]] static void PrepareDataForDoGcNoDataAtBefore();
[[deprecated]] static void PrepareDataForDoGcDataNormalGcStopAtMiddle();
[[deprecated]] static void PrepareDataForDoGcDataNormalGcStopAtEnd();
[[deprecated]] static void PrepareDataForDoGcNoData();
[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfterManyKeys();
[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfter();
[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBack();
[[deprecated]] static void PrepareDataForDoGcDataEmpty();
[[deprecated]] static void PrepareDataForDoGcDataNormal();
[[deprecated]] static void PrepareDataForDoGcSpanLock();
class TxnGcTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    Helper::CreateDirectories(kStorePath);

    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));

    ConfigManager::GetInstance().Register("store", config);
    ConfigManager::GetInstance().Register("index", config);
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

  // friend function
  friend void DoGcCore(bool gc_stop, int64_t safe_point_ts, bool force_gc_stop);
  friend void DeleteRange();
  friend void PrepareDataForDoGcDataNormalNoLock();
  friend void PrepareDataForDoGcDataNormalGcSafePointWarning();
  friend void PrepareDataForDoGcNoDataAtBefore();
  friend void PrepareDataForDoGcDataNormalGcStopAtMiddle();
  friend void PrepareDataForDoGcDataNormalGcStopAtEnd();
  friend void PrepareDataForDoGcNoData();
  friend void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfterManyKeys();
  friend void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfter();
  friend void PrepareDataForDoGcDataDeleteAndPutAndRollBack();
  friend void PrepareDataForDoGcDataEmpty();
  friend void PrepareDataForDoGcDataNormal();
  friend void PrepareDataForDoGcSpanLock();
  friend void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                          int set_tso_position,
                          std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                             RawEngine::WriterPtr writer)>
                              func);
};

static void DoGcCore(bool gc_stop, int64_t safe_point_ts, bool force_gc_stop) {
  // do gc
  butil::Status status;
  std::shared_ptr<Engine> raft_engine;
  std::shared_ptr<Context> ctx;

  std::shared_ptr<GCSafePoint> gc_safe_point = std::make_shared<GCSafePoint>();
  gc_safe_point->SetGcFlagAndSafePointTs(gc_stop, safe_point_ts);
  gc_safe_point->SetForceGcStop(force_gc_stop);

  int64_t region_id = 1;

  store::RegionPtr region_ptr = std::make_shared<store::Region>(region_id);
  std::string region_start_key(TxnGcTest::prefix_start_key);
  std::string region_end_key(TxnGcTest::prefix_end_key);

  ctx = std::make_shared<Context>();
  ctx->SetRegionId(region_id);

  status = TxnEngineHelper::DoGc(TxnGcTest::engine, raft_engine, ctx, TxnGcTest::safe_point_ts, gc_safe_point,
                                 region_start_key, region_end_key);
}

static void DeleteRange() {
  auto writer = TxnGcTest::engine->Writer();
  // delete range
  {
    pb::common::Range range;
    range.set_start_key(TxnGcTest::prefix_start_key);
    range.set_end_key(TxnGcTest::prefix_end_key);

    writer->KvDeleteRange(
        {
            Constant::kTxnWriteCF,
            Constant::kTxnDataCF,
            Constant::kTxnLockCF,
        },
        range);
  }
}

static void PrepareData(const std::vector<std::string> &prefix_key_array, int start_index, int end_index, int count,
                        int set_tso_position,
                        std::function<void(int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                                           RawEngine::WriterPtr writer)>
                            func) {
  auto writer = TxnGcTest::engine->Writer();
  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      TxnGcTest::prefix_start_key = prefix_key_array[start_index];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - end_index];

      for (size_t i = 0; i < std::size(prefix_key_array) - end_index; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const auto &prefix_key = prefix_key_array[i];
        for (int j = 0; j < count; j++) {
          func(j, physical, logical, prefix_key, writer);
        }
        LOG(INFO);
        physical++;

        if (i == set_tso_position) {
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
        }
      }

      if (-1 == set_tso_position) {
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
      }
    }
  }
}

[[deprecated]] static void PrepareDataForDoGcDataNormalNoLock() {
  auto writer = TxnGcTest::engine->Writer();
  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 32768; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);
          if (0 == j % 2) {
            write_info.set_short_value("short_value");
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (0 != j % 2) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock empty
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataNormalGcSafePointWarning() {
  auto writer = TxnGcTest::engine->Writer();
  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 3; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);
          if (0 == j % 2) {
            write_info.set_short_value("short_value");
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (0 != j % 2) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock empty
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcNoDataAtBefore() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{
          "aba", "abb", "abc", "abd", "abe", "abf", "abg",
      };
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 1; i < std::size(prefix_key_array) - 2; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 12; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          if (0 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Delete);
          } else if (4 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Rollback);
          } else if (3 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
          } else if (2 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Lock);
          } else if (1 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::None);
          } else {
            write_info.set_op(::dingodb::pb::store::Op::Put);
          }

          LOG(INFO) << fmt::format("key : {} op : {} start_ts ; {} commit_ts : {}", prefix_key,
                                   ::dingodb::pb::store::Op_Name(write_info.op()), start_ts, commit_ts);

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (write_info.op() == ::dingodb::pb::store::Op::Put) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock prefix_key once
          if (0 == j % 6) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }
        }
        LOG(INFO);
        physical++;

        if (i == (std::size(prefix_key_array) - 1) / 2) {
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
        }
      }
    }
  }
}

[[deprecated]] static void PrepareDataForDoGcDataNormalGcStopAtMiddle() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 32768; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);

          // lock prefix_key once
          if (0 == j) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataNormalGcStopAtEnd() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 3; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);
          if (0 == j % 2) {
            write_info.set_short_value("short_value");
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (0 != j % 2) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock empty
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcNoData() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{
          "aba",
          "abb",
          "abc",
      };
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfterManyKeys() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{
          "aba", "abb", "abc", "abd", "abe", "abf", "abg",
      };
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 12; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          if (0 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Delete);
          } else if (4 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Rollback);
          } else if (3 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
          } else if (2 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Lock);
          } else if (1 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::None);
          } else {
            write_info.set_op(::dingodb::pb::store::Op::Put);
          }

          LOG(INFO) << fmt::format("key : {} op : {} start_ts ; {} commit_ts : {}", prefix_key,
                                   ::dingodb::pb::store::Op_Name(write_info.op()), start_ts, commit_ts);

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (write_info.op() == ::dingodb::pb::store::Op::Put) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock prefix_key once
          if (0 == j % 6) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }
        }
        LOG(INFO);
        physical++;

        if (i == (std::size(prefix_key_array) - 1) / 2) {
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
        }
      }
    }
  }
}

[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfter() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{
          "aba",
          "abb",
      };
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 12; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          if (5 == j % 6) {
            write_info.set_op(::dingodb::pb::store::Op::Delete);
          } else if (4 == j % 12) {
            write_info.set_op(::dingodb::pb::store::Op::Rollback);
          } else if (3 == j % 12) {
            write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
          } else if (2 == j % 12) {
            write_info.set_op(::dingodb::pb::store::Op::Lock);
          } else if (1 == j % 12) {
            write_info.set_op(::dingodb::pb::store::Op::None);
          } else {
            write_info.set_op(::dingodb::pb::store::Op::Put);
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (write_info.op() == ::dingodb::pb::store::Op::Put) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock prefix_key once
          if (0 == j % 6) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }

          if (6 == j) {
            pb::meta::TsoTimestamp tso;
            tso.set_physical(physical);
            tso.set_logical(logical);
            TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
          }
        }
        LOG(INFO);
        physical++;
      }
    }

    //    pb::meta::TsoTimestamp tso;
    //    tso.set_physical(physical);
    //    tso.set_logical(logical);
    //    safe_point_ts = Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataDeleteAndPutAndRollBack() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{
          "aba",
          "abb",
          "abc",
      };
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 6; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          if (5 == j) {
            write_info.set_op(::dingodb::pb::store::Op::Delete);
          } else if (4 == j) {
            write_info.set_op(::dingodb::pb::store::Op::Rollback);
          } else if (3 == j) {
            write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
          } else if (2 == j) {
            write_info.set_op(::dingodb::pb::store::Op::Lock);
          } else if (1 == j) {
            write_info.set_op(::dingodb::pb::store::Op::None);
          } else {
            write_info.set_op(::dingodb::pb::store::Op::Put);
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (write_info.op() == ::dingodb::pb::store::Op::Put) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock prefix_key once
          if (0 == j) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataEmpty() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 3; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          // lock empty
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcDataNormal() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 3; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);
          if (0 == j % 2) {
            write_info.set_short_value("short_value");
          }

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          if (0 != j % 2) {
            pb::common::KeyValue kv_data;
            std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
            std::string data_value = prefix_key + "_value";
            kv_data.set_key(data_key);
            kv_data.set_value(data_value);

            writer->KvPut(Constant::kTxnDataCF, kv_data);
          }

          // lock empty
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

[[deprecated]] static void PrepareDataForDoGcSpanLock() {
  auto writer = TxnGcTest::engine->Writer();

  // prepare data
  {
    int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
    int64_t logical = 0;

    // write column
    {
      std::string prefix_key_array[]{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
      TxnGcTest::prefix_start_key = prefix_key_array[0];
      TxnGcTest::prefix_end_key = prefix_key_array[std::size(prefix_key_array) - 1];

      for (size_t i = 0; i < std::size(prefix_key_array) - 1; i++) {
        // for (const auto &prefix_key : prefix_key_array) {
        const std::string &prefix_key = prefix_key_array[i];
        for (int j = 0; j < 32768; j++) {
          pb::common::KeyValue kv_write;
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
          tso.set_logical(++logical);
          int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

          // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

          std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

          pb::store::WriteInfo write_info;
          write_info.set_start_ts(start_ts);
          write_info.set_op(::dingodb::pb::store::Op::Put);

          kv_write.set_key(write_key);
          kv_write.set_value(write_info.SerializeAsString());

          writer->KvPut(Constant::kTxnWriteCF, kv_write);

          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);

          // lock prefix_key once
          if (0 == j) {
            pb::common::KeyValue kv_lock;
            std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
            std::string lock_value = prefix_key + "_value";

            pb::store::LockInfo lock_info;
            lock_info.set_primary_lock("primary_lock:dummy");
            lock_info.set_key("prefix_key");
            lock_info.set_lock_ts(start_ts);
            lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
            lock_value = lock_info.SerializeAsString();

            kv_lock.set_key(lock_key);
            kv_lock.set_value(lock_value);

            writer->KvPut(Constant::kTxnLockCF, kv_lock);
          }
        }
        LOG(INFO);
        physical++;
      }
    }

    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
  }
}

TEST_F(TxnGcTest, DoGcDataNormalNoLock) {
  // PrepareDataForDoGcDataNormalNoLock();

  PrepareData(
      {"aba", "abb", "abc", "abd", "abe", "abf", "abg"}, 0, 1, 32768, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);
        if (0 == j % 2) {
          write_info.set_short_value("short_value");
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (0 != j % 2) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock empty
      });

  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// gc_safe_point. safe point < safe_point
// print warning
TEST_F(TxnGcTest, DoGcDataNormalGcSafePointWarning) {
  // PrepareDataForDoGcDataNormalGcSafePointWarning();

  PrepareData(
      {"aba", "abb"}, 0, 1, 3, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);
        if (0 == j % 2) {
          write_info.set_short_value("short_value");
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (0 != j % 2) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock empty
      });
  DoGcCore(false, TxnGcTest::safe_point_ts + 10, false);
  DeleteRange();
}

// no data at before
TEST_F(TxnGcTest, DoGcNoDataAtBefore) {
  // PrepareDataForDoGcNoDataAtBefore();

  std::vector<std::string> prefix_key_array{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
  PrepareData(
      prefix_key_array, 0, 2, 12, (std::size(prefix_key_array) - 1) / 2,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        if (0 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Delete);
        } else if (4 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Rollback);
        } else if (3 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
        } else if (2 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Lock);
        } else if (1 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::None);
        } else {
          write_info.set_op(::dingodb::pb::store::Op::Put);
        }

        // LOG(INFO) << fmt::format("key : {} op : {} start_ts ; {} commit_ts : {}", prefix_key,
        //                ::dingodb::pb::store::Op_Name(write_info.op()), start_ts, commit_ts)
        //;

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (write_info.op() == ::dingodb::pb::store::Op::Put) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock prefix_key once
        if (0 == j % 6) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// gc stop at middle
TEST_F(TxnGcTest, DoGcDataNormalGcStopAtMiddle) {
  // PrepareDataForDoGcDataNormalGcStopAtMiddle();
  std::vector<std::string> prefix_key_array{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
  PrepareData(
      prefix_key_array, 0, 1, 32768, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        pb::common::KeyValue kv_data;
        std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
        std::string data_value = prefix_key + "_value";
        kv_data.set_key(data_key);
        kv_data.set_value(data_value);

        writer->KvPut(Constant::kTxnDataCF, kv_data);

        // lock prefix_key once
        if (0 == j) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }

        // lock empty
      });
  DoGcCore(true, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// gc stop at end
TEST_F(TxnGcTest, DoGcDataNormalGcStopAtEnd) {
  // PrepareDataForDoGcDataNormalGcStopAtEnd();
  std::vector<std::string> prefix_key_array{"aba", "abb"};
  PrepareData(
      prefix_key_array, 0, 1, 3, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);
        if (0 == j % 2) {
          write_info.set_short_value("short_value");
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (0 != j % 2) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock empty
      });
  DoGcCore(true, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// No data
TEST_F(TxnGcTest, DoGcNoData) {
  // PrepareDataForDoGcNoData();
  std::vector<std::string> prefix_key_array{
      "aba",
  };
  PrepareData(
      prefix_key_array, 0, 1, 0, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        // lock empty
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// safe point before and after has data
// op = delete, put, rollback, PutIfAbsent, none ...
// many keys
TEST_F(TxnGcTest, DoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfterManyKeys) {
  // PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfterManyKeys();
  std::vector<std::string> prefix_key_array{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
  PrepareData(
      prefix_key_array, 0, 1, 12, (std::size(prefix_key_array) - 1) / 2,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        if (0 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Delete);
        } else if (4 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Rollback);
        } else if (3 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
        } else if (2 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Lock);
        } else if (1 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::None);
        } else {
          write_info.set_op(::dingodb::pb::store::Op::Put);
        }

        // LOG(INFO) << fmt::format("key : {} op : {} start_ts ; {} commit_ts : {}", prefix_key,
        //                          ::dingodb::pb::store::Op_Name(write_info.op()), start_ts, commit_ts)
        //          ;

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (write_info.op() == ::dingodb::pb::store::Op::Put) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock prefix_key once
        if (0 == j % 6) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// safe point before and after has data
// op = delete, put, rollback, PutIfAbsent, none ...
TEST_F(TxnGcTest, DoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfter) {
  // PrepareDataForDoGcDataDeleteAndPutAndRollBackAndSafePointHasDataBeforeAfter();
  std::vector<std::string> prefix_key_array{"aba", "abb"};
  PrepareData(
      prefix_key_array, 0, 1, 12, -2,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        if (5 == j % 6) {
          write_info.set_op(::dingodb::pb::store::Op::Delete);
        } else if (4 == j % 12) {
          write_info.set_op(::dingodb::pb::store::Op::Rollback);
        } else if (3 == j % 12) {
          write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
        } else if (2 == j % 12) {
          write_info.set_op(::dingodb::pb::store::Op::Lock);
        } else if (1 == j % 12) {
          write_info.set_op(::dingodb::pb::store::Op::None);
        } else {
          write_info.set_op(::dingodb::pb::store::Op::Put);
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (write_info.op() == ::dingodb::pb::store::Op::Put) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock prefix_key once
        if (0 == j % 6) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }

        if (6 == j) {
          pb::meta::TsoTimestamp tso;
          tso.set_physical(physical);
          tso.set_logical(logical);
          TxnGcTest::safe_point_ts = TxnGcTest::Tso2Timestamp(tso);
        }
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// op = delete, put, rollback, PutIfAbsent, none ...
TEST_F(TxnGcTest, DoGcDataDeleteAndPutAndRollBack) {
  // PrepareDataForDoGcDataDeleteAndPutAndRollBack();
  std::vector<std::string> prefix_key_array{
      "aba",
      "abb",
      "abc",
  };
  PrepareData(
      prefix_key_array, 0, 1, 6, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        if (5 == j) {
          write_info.set_op(::dingodb::pb::store::Op::Delete);
        } else if (4 == j) {
          write_info.set_op(::dingodb::pb::store::Op::Rollback);
        } else if (3 == j) {
          write_info.set_op(::dingodb::pb::store::Op::PutIfAbsent);
        } else if (2 == j) {
          write_info.set_op(::dingodb::pb::store::Op::Lock);
        } else if (1 == j) {
          write_info.set_op(::dingodb::pb::store::Op::None);
        } else {
          write_info.set_op(::dingodb::pb::store::Op::Put);
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (write_info.op() == ::dingodb::pb::store::Op::Put) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock prefix_key once
        if (0 == j) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// write  -> data empty
TEST_F(TxnGcTest, DoGcDataEmpty) {
  // PrepareDataForDoGcDataEmpty();
  std::vector<std::string> prefix_key_array{
      "aba",
      "abb",
  };
  PrepareData(prefix_key_array, 0, 1, 3, -1,
              [&]([[maybe_unused]] int j, int64_t physical, int64_t logical, const std::string &prefix_key,
                  RawEngine::WriterPtr writer) {
                pb::common::KeyValue kv_write;
                pb::meta::TsoTimestamp tso;
                tso.set_physical(physical);
                tso.set_logical(logical);
                int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
                tso.set_logical(++logical);
                int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

                // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts)
                //          ;

                std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

                pb::store::WriteInfo write_info;
                write_info.set_start_ts(start_ts);
                write_info.set_op(::dingodb::pb::store::Op::Put);

                kv_write.set_key(write_key);
                kv_write.set_value(write_info.SerializeAsString());

                writer->KvPut(Constant::kTxnWriteCF, kv_write);

                // lock empty
              });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

TEST_F(TxnGcTest, DoGcDataNormal) {
  // PrepareDataForDoGcDataNormal();
  std::vector<std::string> prefix_key_array{
      "aba",
      "abb",
  };
  PrepareData(
      prefix_key_array, 0, 1, 3, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);
        if (0 == j % 2) {
          write_info.set_short_value("short_value");
        }

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        if (0 != j % 2) {
          pb::common::KeyValue kv_data;
          std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
          std::string data_value = prefix_key + "_value";
          kv_data.set_key(data_key);
          kv_data.set_value(data_value);

          writer->KvPut(Constant::kTxnDataCF, kv_data);
        }

        // lock empty
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);
  DeleteRange();
}

// lock column family span too many data. not scan repeated.
TEST_F(TxnGcTest, DoGcSpanLock) {
  // PrepareDataForDoGcSpanLock();
  std::vector<std::string> prefix_key_array{"aba", "abb", "abc", "abd", "abe", "abf", "abg"};
  PrepareData(
      prefix_key_array, 0, 1, 32768, -1,
      [&](int j, int64_t physical, int64_t logical, const std::string &prefix_key, RawEngine::WriterPtr writer) {
        pb::common::KeyValue kv_write;
        pb::meta::TsoTimestamp tso;
        tso.set_physical(physical);
        tso.set_logical(logical);
        int64_t start_ts = TxnGcTest::Tso2Timestamp(tso);
        tso.set_logical(++logical);
        int64_t commit_ts = TxnGcTest::Tso2Timestamp(tso);

        // LOG(INFO) << fmt::format("key : {} start_ts ; {} commit_ts : {}", prefix_key, start_ts, commit_ts);

        std::string write_key = Helper::EncodeTxnKey(std::string(prefix_key), commit_ts);

        pb::store::WriteInfo write_info;
        write_info.set_start_ts(start_ts);
        write_info.set_op(::dingodb::pb::store::Op::Put);

        kv_write.set_key(write_key);
        kv_write.set_value(write_info.SerializeAsString());

        writer->KvPut(Constant::kTxnWriteCF, kv_write);

        pb::common::KeyValue kv_data;
        std::string data_key = Helper::EncodeTxnKey(std::string(prefix_key), start_ts);
        std::string data_value = prefix_key + "_value";
        kv_data.set_key(data_key);
        kv_data.set_value(data_value);

        writer->KvPut(Constant::kTxnDataCF, kv_data);

        // lock prefix_key once
        if (0 == j) {
          pb::common::KeyValue kv_lock;
          std::string lock_key = Helper::EncodeTxnKey(std::string(prefix_key), Constant::kLockVer);
          std::string lock_value = prefix_key + "_value";

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock("primary_lock:dummy");
          lock_info.set_key("prefix_key");
          lock_info.set_lock_ts(start_ts);
          lock_info.set_lock_type(::dingodb::pb::store::Op::Lock);
          lock_value = lock_info.SerializeAsString();

          kv_lock.set_key(lock_key);
          kv_lock.set_value(lock_value);

          writer->KvPut(Constant::kTxnLockCF, kv_lock);
        }
      });
  DoGcCore(false, TxnGcTest::safe_point_ts, false);

  DeleteRange();
}

}  // namespace dingodb
