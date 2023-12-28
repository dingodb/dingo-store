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
#include "engine/bdb_raw_engine.h"

#include <gflags/gflags.h>

#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "rocksdb/sst_file_reader.h"
#include "serial/buf.h"
#include "serial/schema/string_schema.h"

#define BDB_BUILD_USE_SNAPSHOT
#define BDB_FAST_GET_APROX_SIZE

namespace dingodb {

DEFINE_int32(bdb_env_cache_size_gb, 4, "bdb env cache size(GB)");
DEFINE_int32(bdb_page_size, 32 * 1024, "bdb page size");
DEFINE_int32(bdb_max_retries, 20, "bdb max retry on a deadlock");
DEFINE_int32(bdb_ingest_external_file_batch_put_count, 128, "bdb ingest external file batch put cout");

namespace bdb {

// BdbHelper
std::string BdbHelper::EncodeKey(const std::string& cf_name, const std::string& key) {
  Buf buf(32);

  DingoSchema<std::optional<std::shared_ptr<std::string>>> schema;
  schema.SetIsKey(true);
  schema.SetAllowNull(false);
  schema.EncodeKeyPrefix(&buf, std::make_shared<std::string>(cf_name));
  return buf.GetString().append(key);
}

// #define BDB_BUILD_USE_SNAPSHOT
std::string BdbHelper::EncodeCfName(const std::string& cf_name) { return BdbHelper::EncodeKey(cf_name, std::string()); }

int BdbHelper::DecodeKey(const std::string& /*cf_name*/, const Dbt& /*bdb_key*/, std::string& /*key*/) { return -1; }

void BdbHelper::DbtToBinary(const Dbt& dbt, std::string& binary) {
  binary.assign((const char*)dbt.get_data(), dbt.get_size());
}

void BdbHelper::BinaryToDbt(const std::string& binary, Dbt& dbt) {
  dbt.set_data((void*)binary.c_str());
  dbt.set_size(binary.size());
}

int BdbHelper::DbtPairToKv(const std::string& cf_name, const Dbt& bdb_key, const Dbt& value, pb::common::KeyValue& kv) {
  std::string encoded_cf_name = EncodeCfName(cf_name);
  if (!IsBdbKeyPrefixWith(bdb_key, encoded_cf_name)) {
    return -1;
  }

  kv.set_key((const char*)bdb_key.get_data() + encoded_cf_name.size(), bdb_key.get_size() - encoded_cf_name.size());
  kv.set_value((const char*)value.get_data(), value.get_size());
  return 0;
}

int BdbHelper::DbtCompare(const Dbt& dbt1, const Dbt& dbt2) {
  assert(dbt1.get_data() != nullptr && dbt2.get_data() != nullptr);
  const int32_t min_len = (dbt1.get_size() < dbt2.get_size()) ? dbt1.get_size() : dbt2.get_size();
  int ret = memcmp(dbt1.get_data(), dbt2.get_data(), min_len);
  if (ret == 0) {
    if (dbt1.get_size() < dbt2.get_size()) {
      ret = -1;
    } else if (dbt1.get_size() > dbt2.get_size()) {
      ret = 1;
    }
  }
  return ret;
}

bool BdbHelper::IsBdbKeyPrefixWith(const Dbt& bdb_key, const std::string& binary) {
  if (binary.size() > bdb_key.get_size()) {
    return false;
  }

  return memcmp(bdb_key.get_data(), binary.c_str(), binary.size()) == 0;
}

std::string BdbHelper::GetEncodedCfNameUpperBound(const std::string& cf_name) { return EncodeCfName(cf_name + "\1"); };

// Iterator
Iterator::Iterator(const std::string& cf_name, /*bool snapshot_mode,*/ IteratorOptions options, Dbc* cursorp)
    : /*snapshot_mode_(snapshot_mode),*/ options_(options), /*cf_name_(cf_name), */ cursorp_(cursorp), valid_(false) {
  encoded_cf_name_ = BdbHelper::EncodeCfName(cf_name);
  encoded_cf_name_upper_bound_ = BdbHelper::GetEncodedCfNameUpperBound(cf_name);
  bdb_value_.set_flags(DB_DBT_MALLOC);
}

Iterator::~Iterator() {
  if (cursorp_ != nullptr) {
    try {
      cursorp_->close();
    } catch (DbException& db_exception) {
      DINGO_LOG(WARNING) << fmt::format("cursor close failed, exception: {}.", db_exception.what());
    }
  }
};

bool Iterator::Valid() const {
  if (!valid_) {
    return false;
  }

  // cf_name check
  if (!BdbHelper::IsBdbKeyPrefixWith(bdb_key_, encoded_cf_name_)) {
    return false;
  }

  if (!options_.upper_bound.empty()) {
    Dbt upper_key;
    std::string upper = encoded_cf_name_ + options_.upper_bound;
    BdbHelper::BinaryToDbt(upper, upper_key);
    if (BdbHelper::DbtCompare(upper_key, bdb_key_) <= 0) {
      return false;
    }
  }

  if (!options_.lower_bound.empty()) {
    Dbt lower_key;
    std::string lower = encoded_cf_name_ + options_.lower_bound;
    BdbHelper::BinaryToDbt(lower, lower_key);
    if (BdbHelper::DbtCompare(lower_key, bdb_key_) > 0) {
      return false;
    }
  }

  return true;
}

void Iterator::SeekToFirst() { Seek(std::string()); }

void Iterator::SeekToLast() {
  valid_ = false;
  status_ = butil::Status();

  BdbHelper::BinaryToDbt(encoded_cf_name_upper_bound_, bdb_key_);
  try {
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_SET_RANGE);
    if (ret == 0) {
      Prev();
      return;
    } else if (ret == DB_NOTFOUND) {
      ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_LAST);
      if (ret == 0) {
        valid_ = true;
        return;
      }
    } else {
      DINGO_LOG(ERROR) << fmt::format("[bdb] get failed ret: {}.", ret);
      status_ = butil::Status(pb::error::EINTERNAL, "internal cursor seek to last error.");
    }
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    status_ = butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek failed, exception: {}.", db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db seek failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::Seek(const std::string& target) {
  valid_ = false;
  status_ = butil::Status();

  std::string store_key = encoded_cf_name_ + target;
  BdbHelper::BinaryToDbt(store_key, bdb_key_);

  try {
    // find smallest key greater than or equal to the target.
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_SET_RANGE);
    if (ret == 0) {
      valid_ = true;
      // if (snapshot_mode_) {
      //   Next();
      // }
      return;
    }

    if (ret != DB_NOTFOUND) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] get failed ret: {}.", ret);
      status_ = butil::Status(pb::error::EINTERNAL, "internal cursor seek error.");
    }
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    status_ = butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek failed, exception: {}.", db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db seek failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::SeekForPrev(const std::string& target) {
  valid_ = false;
  status_ = butil::Status();

  std::string store_key = encoded_cf_name_ + target;
  Dbt bdb_target_key;
  BdbHelper::BinaryToDbt(store_key, bdb_target_key);
  BdbHelper::BinaryToDbt(store_key, bdb_key_);

  try {
    // find smallest key greater than or equal to the target.
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_SET_RANGE);
    if (ret == 0) {
      if (BdbHelper::DbtCompare(bdb_target_key, bdb_key_) == 0) {
        valid_ = true;
      } else {
        Prev();
      }
      return;
    }

    // target is greater than max key in bdb, find the last one.
    if (ret == DB_NOTFOUND) {
      ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_LAST);
      if (ret == 0) {
        valid_ = true;
        return;
      }
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] get failed ret: {}.", ret);

  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    status_ = butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek for prev failed, exception: {}.", db_exception.what());
    status_ =
        butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db seek for prev failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::Next() {
  valid_ = false;
  status_ = butil::Status();

  try {
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_NEXT);
    if (ret == 0) {
      valid_ = true;
      return;
    }

    if (ret != DB_NOTFOUND) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] get failed ret: {}.", ret);
      status_ = butil::Status(pb::error::EINTERNAL, "internal cursor next error.");
    }
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    status_ = butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db cursor next failed, exception: {}.", db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db cursor next failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::Prev() {
  valid_ = false;
  status_ = butil::Status();

  try {
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_PREV);
    if (ret == 0) {
      valid_ = true;
      return;
    }

    if (ret != DB_NOTFOUND) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] get failed ret: {}.", ret);
      status_ = butil::Status(pb::error::EINTERNAL, "internal cursor prev error.");
    }
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    status_ = butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db cursor prev failed, exception: {}.", db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db cursor prev failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

// Snapshot
Snapshot::~Snapshot() {
  if (txn_ != nullptr) {
    int ret = 0;
    // commit
    try {
      ret = txn_->commit(0);
      if (ret == 0) {
        txn_ = nullptr;
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
      ret = BdbHelper::kCommitException;
    }

    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
      txn_->abort();
      txn_ = nullptr;
    }
  }
}

// Reader
butil::Status Reader::KvGet(const std::string& cf_name, const std::string& key, std::string& value) {
#ifdef BDB_BUILD_USE_SNAPSHOT
  return KvGet(cf_name, GetSnapshot(), key, value);
#else
  return KvGet(cf_name, nullptr, key, value);
#endif
}

butil::Status Reader::KvGet(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& key,
                            std::string& value) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  try {
    std::string store_key = BdbHelper::EncodeKey(cf_name, key);
    Dbt bdb_key;
    BdbHelper::BinaryToDbt(store_key, bdb_key);

    Dbt bdb_value;
    bdb_value.set_flags(DB_DBT_MALLOC);

    int ret = 0;
    if (snapshot != nullptr) {
      std::shared_ptr<bdb::Snapshot> ss = std::dynamic_pointer_cast<bdb::Snapshot>(snapshot);
      if (ss == nullptr) {
        DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
        return butil::Status(pb::error::EINTERNAL, "snapshot pointer cast error.");
      }
      ret = GetDb()->get(ss->GetDbTxn(), &bdb_key, &bdb_value, 0);
    } else {
      ret = GetDb()->get(nullptr, &bdb_key, &bdb_value, 0);
    }

    if (ret == 0) {
      BdbHelper::DbtToBinary(bdb_value, value);
      return butil::Status();
    } else if (ret == DB_NOTFOUND) {
      DINGO_LOG(WARNING) << "[bdb] key not found.";
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key.");
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] db get failed, ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal get error.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << "[bdb] db exception: " << db_exception.what();
    return butil::Status(pb::error::EBDB_EXCEPTION, "%s", db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << "[bdb] std exception: " << std_exception.what();
    return butil::Status(pb::error::ESTD_EXCEPTION, "%s", std_exception.what());
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknow error.");
}

butil::Status Reader::KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
#ifdef BDB_BUILD_USE_SNAPSHOT
  return KvScan(cf_name, GetSnapshot(), start_key, end_key, kvs);
#else
  return KvScan(cf_name, nullptr, start_key, end_key, kvs);
#endif
}

butil::Status Reader::KvScan(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                             const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  if (BAIDU_UNLIKELY(start_key >= end_key)) {
    return butil::Status();
  }

  // Acquire a cursor
  Dbc* cursorp = nullptr;
  // Release the cursor later
  DEFER(  // FOR_CLANG_FORMAT
      if (cursorp != nullptr) {
        try {
          cursorp->close();
        } catch (DbException& db_exception) {
          LOG(WARNING) << fmt::format("cursor close failed, exception: {}.", db_exception.what());
        }
      });

  try {
    int ret = 0;
    if (snapshot != nullptr) {
      std::shared_ptr<bdb::Snapshot> ss = std::dynamic_pointer_cast<bdb::Snapshot>(snapshot);
      if (ss == nullptr) {
        DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
        return butil::Status(pb::error::EINTERNAL, "snapshot pointer cast error.");
      }
      ret = GetDb()->cursor(ss->GetDbTxn(), &cursorp, DB_TXN_SNAPSHOT);
    } else {
      ret = GetDb()->cursor(nullptr, &cursorp, DB_READ_COMMITTED);
    }

    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] create cursor failed ret: {}.", ret);
      return butil::Status(pb::error::EINTERNAL, "Internal create cursor error.");
    }

    std::string store_key = BdbHelper::EncodeKey(cf_name, start_key);
    Dbt bdb_key;
    BdbHelper::BinaryToDbt(store_key, bdb_key);

    std::string store_end_key = BdbHelper::EncodeKey(cf_name, end_key);
    Dbt bdb_end_key;
    BdbHelper::BinaryToDbt(store_end_key, bdb_end_key);

    Dbt bdb_value;
    bdb_value.set_flags(DB_DBT_MALLOC);

    // find fist postion
    ret = cursorp->get(&bdb_key, &bdb_value, DB_SET_RANGE);
    if (ret != 0) {
      if (ret == DB_NOTFOUND) {
        // can not find data in the range, return OK;
        return butil::Status();
      }
      DINGO_LOG(ERROR) << fmt::format("[bdb] txn get failed ret: {}.", ret);
      return butil::Status(pb::error::EINTERNAL, "Internal txn get error.");
    }

    pb::common::KeyValue first_kv;
    if (BdbHelper::DbtPairToKv(cf_name, bdb_key, bdb_value, first_kv) != 0) {
      DINGO_LOG(WARNING) << fmt::format("[bdb] invalid bdb key, size: {}, data: {}.", bdb_key.get_size(),
                                        bdb_key.get_data());
      return butil::Status();
    }

    DINGO_LOG(INFO) << fmt::format("[bdb] get real start key: {}, value: {}", bdb_key.get_data(), bdb_value.get_data());
    kvs.emplace_back(std::move(first_kv));

    int index = 0;
    while (cursorp->get(&bdb_key, &bdb_value, DB_NEXT) == 0) {
      if (BdbHelper::DbtCompare(bdb_key, bdb_end_key) < 0) {
        pb::common::KeyValue kv;
        if (BAIDU_UNLIKELY(BdbHelper::DbtPairToKv(cf_name, bdb_key, bdb_value, kv) != 0)) {
          DINGO_LOG(WARNING) << fmt::format("[bdb] invalid bdb key, size: {}, data: {}.", bdb_key.get_size(),
                                            bdb_key.get_data());
          return butil::Status();
        }
        kvs.emplace_back(std::move(kv));
      }
    }
    return butil::Status();

  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException, giving up.");
    return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db scan failed, exception: {}.", db_exception.what());
    return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db scan failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Reader::KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                              int64_t& count) {
#ifdef BDB_BUILD_USE_SNAPSHOT
  return KvCount(cf_name, GetSnapshot(), start_key, end_key, count);
#else
  return KvCount(cf_name, nullptr, start_key, end_key, count);
#endif
}

butil::Status Reader::KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                              const std::string& end_key, int64_t& count) {
  count = 0;

  DbTxn* txn = nullptr;
  int32_t isolation_flag = DB_READ_COMMITTED;
  if (snapshot != nullptr) {
    std::shared_ptr<bdb::Snapshot> ss = std::dynamic_pointer_cast<bdb::Snapshot>(snapshot);
    if (ss == nullptr) {
      DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
      return butil::Status(pb::error::EINTERNAL, "snapshot pointer cast error.");
    }
    txn = ss->GetDbTxn();
    isolation_flag = DB_TXN_SNAPSHOT;
  }

  return GetRangeCountByCursor(cf_name, txn, start_key, end_key, isolation_flag, count);
}

std::shared_ptr<dingodb::Iterator> Reader::NewIterator(const std::string& cf_name, IteratorOptions options) {
#ifdef BDB_BUILD_USE_SNAPSHOT
  return NewIterator(cf_name, GetSnapshot(), options);
#else
  return NewIterator(cf_name, nullptr, options);
#endif
}

std::shared_ptr<dingodb::Iterator> Reader::NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                                       IteratorOptions options) {
  // Acquire a cursor
  Dbc* cursorp = nullptr;
  int ret = 0;
  try {
    if (snapshot != nullptr) {
      std::shared_ptr<bdb::Snapshot> ss = std::dynamic_pointer_cast<bdb::Snapshot>(snapshot);
      if (ss != nullptr) {
        // ret = GetDb()->cursor(ss->GetDbTxn(), &cursorp, DB_TXN_SNAPSHOT);
        ret = GetDb()->cursor(nullptr, &cursorp, DB_TXN_SNAPSHOT);
        if (ret == 0) {
          return std::make_shared<Iterator>(cf_name, options, cursorp);
        }
      } else {
        DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
      }
    } else {
      ret = GetDb()->cursor(nullptr, &cursorp, DB_READ_COMMITTED);
      if (ret == 0) {
        return std::make_shared<Iterator>(cf_name, options, cursorp);
      }
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] cursor create failed, ret: {}.", ret);
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] cursor create failed, exception: {}.", db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
  }

  return nullptr;
}

butil::Status Reader::GetRangeCountByCursor(const std::string& cf_name, DbTxn* txn, const std::string& start_key,
                                            const std::string& end_key, const int32_t isolation_flag, int64_t& count) {
  count = 0;

  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  if (BAIDU_UNLIKELY(start_key >= end_key)) {
    return butil::Status();
  }

  // Acquire a cursor
  Dbc* cursorp = nullptr;
  // Release the cursor later
  DEFER(  // FOR_CLANG_FORMAT
      if (cursorp != nullptr) {
        try {
          cursorp->close();
        } catch (DbException& db_exception) {
          LOG(WARNING) << fmt::format("cursor close failed, exception: {}.", db_exception.what());
        }
      });

  int ret = GetDb()->cursor(txn, &cursorp, isolation_flag);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] create cursor failed ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal create cursor error.");
  }

  std::string store_key = BdbHelper::EncodeKey(cf_name, start_key);
  Dbt bdb_key;
  BdbHelper::BinaryToDbt(store_key, bdb_key);

  std::string store_end_key = BdbHelper::EncodeKey(cf_name, end_key);
  Dbt bdb_end_key;
  BdbHelper::BinaryToDbt(store_end_key, bdb_end_key);

  Dbt bdb_value;
  bdb_value.set_flags(DB_DBT_MALLOC);

  // find fist postion
  ret = cursorp->get(&bdb_key, &bdb_value, DB_SET_RANGE);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] txn get failed ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal txn get error.");
  }

  DINGO_LOG(INFO) << fmt::format("[bdb] get key: {}, value: {}", bdb_key.get_data(), bdb_value.get_data());
  ++count;

  int index = 0;
  while (cursorp->get(&bdb_key, &bdb_value, DB_NEXT) == 0) {
    if (BdbHelper::DbtCompare(bdb_key, bdb_end_key) < 0) {
      ++count;
    }
  }
  return butil::Status();
}

std::shared_ptr<BdbRawEngine> Reader::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[bdb] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<Db> Reader::GetDb() { return GetRawEngine()->GetDb(); }

dingodb::SnapshotPtr Reader::GetSnapshot() { return GetRawEngine()->GetSnapshot(); }

butil::Status Reader::RetrieveByCursor(const std::string& cf_name, DbTxn* txn, const std::string& key,
                                       std::string& value) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty.");
  }

  Dbc* cursorp = nullptr;
  // close cursorp
  DEFER(  // FOR_CLANG_FORMAT
      if (cursorp != nullptr) {
        try {
          cursorp->close();
          cursorp = nullptr;
        } catch (DbException& db_exception) {
          LOG(WARNING) << fmt::format("[bdb] cursor close failed, exception: {}.", db_exception.what());
        }
      });

  try {
    // Get the cursor
    GetDb()->cursor(txn, &cursorp, DB_READ_COMMITTED);

    std::string store_key = BdbHelper::EncodeKey(cf_name, key);
    Dbt bdb_key;
    BdbHelper::BinaryToDbt(store_key, bdb_key);

    Dbt bdb_value;
    int ret = cursorp->get(&bdb_key, &bdb_value, DB_FIRST);
    if (ret == 0) {
      BdbHelper::DbtToBinary(bdb_value, value);
      return butil::Status();
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor failed, ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal get error.");
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor, got deadlock.");
    return butil::Status(pb::error::EBDB_DEADLOCK, "retrive by cursor, got deadlock.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor, got db exception: {}.", db_exception.what());
    return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("retrive by cursor, failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

// Writer
butil::Status Writer::KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  DbEnv* envp = GetDb()->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      if (txn != nullptr) {
        txn->abort();
        txn = nullptr;
      });

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      std::string store_key = BdbHelper::EncodeKey(cf_name, kv.key());
      Dbt bdb_key;
      BdbHelper::BinaryToDbt(store_key, bdb_key);

      Dbt bdb_value;
      BdbHelper::BinaryToDbt(kv.value(), bdb_value);

      ret = GetDb()->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] put failed, ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal put error.");
      }

      // commit
      try {
        ret = txn->commit(0);
        if (ret == 0) {
          txn = nullptr;
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
        ret = BdbHelper::kCommitException;
      }

      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
        return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
      }

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        // First thing that we MUST do is abort the transaction.
        if (txn != nullptr) {
          txn->abort();
          txn = nullptr;
        };
        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {}.", db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db put failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvBatchPutAndDelete(const std::string& cf_name,
                                          const std::vector<pb::common::KeyValue>& kvs_to_put,
                                          const std::vector<std::string>& keys_to_delete) {
  if (BAIDU_UNLIKELY(kvs_to_put.empty() && keys_to_delete.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty keys.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  DbEnv* envp = GetDb()->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      if (txn != nullptr) {
        txn->abort();
        txn = nullptr;
      });

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      for (const auto& kv : kvs_to_put) {
        if (BAIDU_UNLIKELY(kv.key().empty())) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
          return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
        }

        std::string store_key = BdbHelper::EncodeKey(cf_name, kv.key());
        Dbt bdb_key;
        BdbHelper::BinaryToDbt(store_key, bdb_key);
        Dbt bdb_value;
        BdbHelper::BinaryToDbt(kv.value(), bdb_value);
        ret = GetDb()->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
        if (ret != 0) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] put failed, ret: {}.", ret);
          return butil::Status(pb::error::EINTERNAL, "Internal put error.");
        }
      }

      for (const auto& key : keys_to_delete) {
        if (BAIDU_UNLIKELY(key.empty())) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
          return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
        }

        std::string store_key = BdbHelper::EncodeKey(cf_name, key);
        Dbt bdb_key;
        BdbHelper::BinaryToDbt(store_key, bdb_key);
        GetDb()->del(txn, &bdb_key, 0);
        if (ret != 0 && ret != DB_NOTFOUND) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
          return butil::Status(pb::error::EINTERNAL, "Internal put error.");
        }
      }

      // commit
      try {
        ret = txn->commit(0);
        if (ret == 0) {
          txn = nullptr;
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
        ret = BdbHelper::kCommitException;
      }

      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
        return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
      }

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        // First thing that we MUST do is abort the transaction.
        if (txn != nullptr) {
          txn->abort();
        };

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {}.", db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db put failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvBatchPutAndDelete(
    const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) {
  DbEnv* envp = GetDb()->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      if (txn != nullptr) {
        txn->abort();
        txn = nullptr;
      });

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      // put
      for (const auto& [cf_name, kv_puts] : kv_puts_with_cf) {
        if (BAIDU_UNLIKELY(kv_puts.empty())) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] keys empty not support");
          return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
        }

        for (const auto& kv : kv_puts) {
          if (BAIDU_UNLIKELY(kv.key().empty())) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] key empty not support");
            return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
          }

          std::string store_key = BdbHelper::EncodeKey(cf_name, kv.key());
          Dbt bdb_key;
          BdbHelper::BinaryToDbt(store_key, bdb_key);
          Dbt bdb_value;
          BdbHelper::BinaryToDbt(kv.value(), bdb_value);
          ret = GetDb()->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
          if (ret != 0) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] put failed, ret: {}.", ret);
            return butil::Status(pb::error::EINTERNAL, "Internal put error.");
          }
        }
      }

      // delete
      for (const auto& [cf_name, kv_deletes] : kv_deletes_with_cf) {
        if (BAIDU_UNLIKELY(kv_deletes.empty())) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] keys empty not support");
          return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
        }

        for (const auto& key : kv_deletes) {
          if (BAIDU_UNLIKELY(key.empty())) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] key empty not support");
            return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
          }

          std::string store_key = BdbHelper::EncodeKey(cf_name, key);
          Dbt bdb_key;
          BdbHelper::BinaryToDbt(store_key, bdb_key);
          GetDb()->del(txn, &bdb_key, 0);
          if (ret != 0 && ret != DB_NOTFOUND) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
            return butil::Status(pb::error::EINTERNAL, "Internal put error.");
          }
        }
      }

      // commit
      try {
        ret = txn->commit(0);
        if (ret == 0) {
          txn = nullptr;
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
        ret = BdbHelper::kCommitException;
      }

      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
        return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
      }

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        // First thing that we MUST do is abort the transaction.
        if (txn != nullptr) {
          txn->abort();
        };

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {}.", db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db put failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvDelete(const std::string& cf_name, const std::string& key) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  DbEnv* envp = GetDb()->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      if (txn != nullptr) {
        txn->abort();
        txn = nullptr;
      });

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      std::string store_key = BdbHelper::EncodeKey(cf_name, key);
      Dbt bdb_key;
      BdbHelper::BinaryToDbt(store_key, bdb_key);

      ret = GetDb()->del(txn, &bdb_key, 0);
      if (ret != 0 && ret != DB_NOTFOUND) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal delete error.");
      }

      // commit
      try {
        ret = txn->commit(0);
        if (ret == 0) {
          txn = nullptr;
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
        ret = BdbHelper::kCommitException;
      }

      if (BAIDU_UNLIKELY(ret != 0)) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
        return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
      }

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        // First thing that we MUST do is abort the transaction.
        if (txn != nullptr) {
          txn->abort();
          txn = nullptr;
        };
        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.", retry_count);
        retry_count++;
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {}.", db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db delete failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) {
  if (range.start_key().empty() || range.end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
  }
  if (range.start_key() >= range.end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
  }

  std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
  range_with_cfs[cf_name] = {range};

  return KvBatchDeleteRange(range_with_cfs);
}

butil::Status Writer::KvBatchDeleteRange(const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) {
  DbEnv* envp = GetDb()->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      if (txn != nullptr) {
        txn->abort();
        txn = nullptr;
      });

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      for (const auto& [cf_name, ranges] : range_with_cfs) {
        for (const auto& range : ranges) {
          if (range.start_key().empty() || range.end_key().empty()) {
            return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
          }
          if (range.start_key() >= range.end_key()) {
            return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
          }

          butil::Status status = DeleteRangeByCursor(cf_name, range, txn);
          if (!status.ok()) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] delete range by cursor: {}.", status.error_cstr());
            return status;
          }
        }
      }

      // commit
      try {
        ret = txn->commit(0);
        if (ret == 0) {
          txn = nullptr;
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {}.", db_exception.what());
        ret = BdbHelper::kCommitException;
      }

      if (BAIDU_UNLIKELY(ret != 0)) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
        return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
      }

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        // First thing that we MUST do is abort the transaction.
        if (txn != nullptr) {
          txn->abort();
          txn = nullptr;
        };
        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.", retry_count);
        retry_count++;
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {}.", db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db delete failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::DeleteRangeByCursor(const std::string& cf_name, const pb::common::Range& range, DbTxn* txn) {
  butil::Status status = Helper::CheckRange(range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] check range: {}.", status.error_cstr());
    return status;
  }

  // Acquire a cursor
  Dbc* cursorp = nullptr;
  // Release the cursor later
  DEFER(  // FOR_CLANG_FORMAT
      if (cursorp != nullptr) {
        try {
          cursorp->close();
        } catch (DbException& db_exception) {
          LOG(WARNING) << fmt::format("cursor close failed, exception: {}.", db_exception.what());
        }
      });

  GetDb()->cursor(txn, &cursorp, DB_READ_COMMITTED);

  std::string store_key = BdbHelper::EncodeKey(cf_name, range.start_key());
  Dbt bdb_key;
  BdbHelper::BinaryToDbt(store_key, bdb_key);

  std::string store_end_key = BdbHelper::EncodeKey(cf_name, range.end_key());
  Dbt bdb_end_key;
  BdbHelper::BinaryToDbt(store_end_key, bdb_end_key);

  Dbt bdb_value;
  bdb_value.set_flags(DB_DBT_MALLOC);

  // find fist postion
  int ret = cursorp->get(&bdb_key, &bdb_value, DB_SET_RANGE);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] txn get failed ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal txn get error.");
  }

  DINGO_LOG(INFO) << fmt::format("[bdb] get key: {}, value: {}", bdb_key.get_data(), bdb_value.get_data());
  cursorp->del(0);

  int index = 0;
  while (cursorp->get(&bdb_key, &bdb_value, DB_NEXT) == 0) {
    if (BdbHelper::DbtCompare(bdb_key, bdb_end_key) < 0) {
      cursorp->del(0);
    }
  }
  return butil::Status();
}

std::shared_ptr<BdbRawEngine> Writer::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[bdb] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<Db> Writer::GetDb() { return GetRawEngine()->GetDb(); }

}  // namespace bdb

// Open a BDB database
int32_t BdbRawEngine::OpenDb(Db** dbpp, const char* file_name, DbEnv* envp, uint32_t extra_flags) {
  int ret;
  uint32_t open_flags;

  try {
    Db* db = new Db(envp, 0);

    // Point to the new'd Db
    *dbpp = db;
    db->set_pagesize(FLAGS_bdb_page_size);

    if (extra_flags != 0) {
      ret = db->set_flags(extra_flags);
    }

    // Now open the database */
    open_flags = DB_CREATE |            // Allow database creation
                 DB_READ_UNCOMMITTED |  // Allow uncommitted reads
                 DB_AUTO_COMMIT |       // Allow autocommit
                 DB_MULTIVERSION |      // Multiversion concurrency control
                 DB_THREAD;             // Cause the database to be free-threade1

    db->open(nullptr,     // Txn pointer
             file_name,   // File name
             nullptr,     // Logical db name
             DB_BTREE,    // Database type (using btree)
             open_flags,  // Open flags
             0);          // File mode. Using defaults
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("OpenDb: db open failed: {}.", db_exception.what());
    return -1;
  }

  return 0;
}

// override functions
bool BdbRawEngine::Init(std::shared_ptr<Config> config, const std::vector<std::string>& /*cf_names*/) {
  DINGO_LOG(INFO) << "Init bdb raw engine...";
  if (BAIDU_UNLIKELY(!config)) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] config empty not support!");
    return false;
  }

  std::string bdb_path = config->GetString(Constant::kStorePathConfigName) + "/bdb";
  if (BAIDU_UNLIKELY(bdb_path.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] can not find: {}/bdb", Constant::kStorePathConfigName);
    return false;
  }

  if (!Helper::IsExistPath(bdb_path)) {
    auto ret = Helper::CreateDirectories(bdb_path);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] create dir failed: {}.", ret.error_cstr());
      return false;
    }
  }

  // Initialize our handles
  Db* db = nullptr;
  DbEnv* envp = nullptr;
  const char* file_name = "dingo.db";

  // Env open flags
  uint32_t env_flags = DB_CREATE |        // Create the environment if it does not exist
                       DB_RECOVER |       // Run normal recovery.
                       DB_INIT_LOCK |     // Initialize the locking subsystem
                       DB_INIT_LOG |      // Initialize the logging subsystem
                       DB_INIT_TXN |      // Initialize the transactional subsystem. This
                                          // also turns on logging.
                       DB_INIT_MPOOL |    // Initialize the memory pool (in-memory cache)
                       DB_MULTIVERSION |  // Multiversion concurrency control
                       DB_THREAD;         // Cause the environment to be free-threaded
  try {
    // Create and open the environment
    envp = new DbEnv(0);

    // Indicate that we want db to internally perform deadlock
    // detection.  Also indicate that the transaction with
    // the fewest number of write locks will receive the
    // deadlock notification in the event of a deadlock.
    envp->set_lk_detect(DB_LOCK_MINWRITE);
    envp->set_cachesize(FLAGS_bdb_env_cache_size_gb, 0, 0);

    envp->open((const char*)bdb_path.c_str(), env_flags, 0);

    // If we had utility threads (for running checkpoints or
    // deadlock detection, for example) we would spawn those
    // here.

    // Open the database
    int ret = OpenDb(&db, file_name, envp, 0);
    if (ret < 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error opening database: {}/{}, ret: {}.", bdb_path, file_name, ret);
      return false;
    }
  } catch (DbException& db_exctption) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] error opening database environment: {}, exception: {}.", db_path_,
                                    db_exctption.what());
    return false;
  }

  db_path_ = bdb_path + "/" + file_name;
  db_.reset(db);

  reader_ = std::make_shared<bdb::Reader>(GetSelfPtr());
  writer_ = std::make_shared<bdb::Writer>(GetSelfPtr());
  DINGO_LOG(INFO) << fmt::format("[bdb] db path: {}", db_path_);

  return true;
}

void BdbRawEngine::Close() {
  try {
    DbEnv* envp = db_->get_env();

    // Close our database handle if it was opened.
    if (db_ != nullptr) {
      db_->close(0);
    }

    // Close our environment if it was opened.
    if (envp != nullptr) {
      envp->close(0);
      delete envp;
      envp = nullptr;
    }
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] error closing database and environment, exception: {}.",
                                    db_exception.what());
  }

  DINGO_LOG(INFO) << "[bdb] I'm all done.";
}

std::string BdbRawEngine::GetName() { return pb::common::RawEngine_Name(pb::common::RAW_ENG_BDB); }

pb::common::RawEngine BdbRawEngine::GetRawEngineType() { return pb::common::RAW_ENG_BDB; }

dingodb::SnapshotPtr BdbRawEngine::GetSnapshot() {
  try {
    DbEnv* envp = db_->get_env();
    DbTxn* txn = nullptr;
    int ret = envp->txn_begin(nullptr, &txn, DB_TXN_SNAPSHOT);
    if (ret == 0) {
      return std::make_shared<bdb::Snapshot>(db_, txn);
    } else {
      DINGO_LOG(ERROR) << fmt::format("[bdb] got DeadLockException, giving up.");
    }

  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek for prev failed, exception: {}.", db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
  }

  DINGO_LOG(ERROR) << "unknown error.";
  return nullptr;
}

butil::Status BdbRawEngine::MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,  // NOLINT
                                                 const std::vector<std::string>& cf_names,                 // NOLINT
                                                 std::vector<std::string>& merge_sst_paths) {              // NOLINT
  return butil::Status();
}
butil::Status BdbRawEngine::IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) {
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  if (options.env == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Internal ingest external file error.");
  }

  rocksdb::SstFileReader reader(options);

  rocksdb::Status status;
  for (const auto& file_name : files) {
    status = reader.Open(file_name);
    if (BAIDU_UNLIKELY(!status.ok())) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] reader open failed, error: {}.", status.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal ingest external file error.");
    }

    std::vector<pb::common::KeyValue> kvs;
    std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      // DINGO_LOG(DEBUG) << fmt::format("[bdb] ingesting cf_name: {}, sst file name: {}, KV: {} | {}.", cf_name,
      //                                 file_name, iter->key().ToStringView(), iter->value().ToStringView());
      pb::common::KeyValue kv;
      kv.set_key(iter->key().data(), iter->key().size());
      kv.set_value(iter->value().data(), iter->value().size());

      kvs.emplace_back(kv);

      if (kvs.size() >= FLAGS_bdb_ingest_external_file_batch_put_count) {
        butil::Status s = writer_->KvBatchPutAndDelete(cf_name, kvs, {});
        if (BAIDU_UNLIKELY(!s.ok())) {
          DINGO_LOG(ERROR) << fmt::format(
              "[bdb] batch put failed, cf_name: {}, sst file name: {}, status code: {}, message: {}", cf_name,
              file_name, s.error_code(), s.error_str());
          return butil::Status(pb::error::EINTERNAL, "Internal ingest external file error.");
        }

        kvs.clear();
      }
    }

    if (!kvs.empty()) {
      butil::Status s = writer_->KvBatchPutAndDelete(cf_name, kvs, {});
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format(
            "[bdb] batch put failed, cf_name: {}, sst file name: {}, status code: {}, message: {}", cf_name, file_name,
            s.error_code(), s.error_str());
        return butil::Status(pb::error::EINTERNAL, "Internal ingest external file error.");
      }

      kvs.clear();
    }
  }

  DINGO_LOG(INFO) << "ingest external file done!";
  return butil::Status();
}

void BdbRawEngine::Flush(const std::string& /*cf_name*/) {
  try {
    int ret = db_->sync(0);
    if (ret == 0) {
      DINGO_LOG(INFO) << fmt::format("[bdb] flush done!");
    } else {
      DINGO_LOG(ERROR) << fmt::format("[bdb] flush failed, ret: {}.", ret);
    }
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] error flushing, exception: {}.", db_exception.what());
  }
}

butil::Status BdbRawEngine::Compact(const std::string& cf_name) {
  std::string encoded_cf_name = bdb::BdbHelper::EncodeCfName(cf_name);
  std::string encoded_cf_name_upper_bound = bdb::BdbHelper::GetEncodedCfNameUpperBound(cf_name);

  Dbt start, stop;
  bdb::BdbHelper::BinaryToDbt(encoded_cf_name, start);
  bdb::BdbHelper::BinaryToDbt(encoded_cf_name_upper_bound, stop);

  try {
    DB_COMPACT compact_data;
    memset(&compact_data, 0, sizeof(DB_COMPACT));
    compact_data.compact_fillpercent = 80;

    int ret = db_->compact(nullptr, &start, &stop, &compact_data, 0, nullptr);
    if (ret == 0) {
      DINGO_LOG(INFO) << fmt::format(
          "[bdb] compact done! number of pages freed: {}, number of pages examine: {}, number of levels removed: {}, "
          "number of deadlocks: {}, pages truncated to OS: {}, page number for truncation: {}",
          compact_data.compact_pages_free, compact_data.compact_pages_examine, compact_data.compact_levels,
          compact_data.compact_deadlock, compact_data.compact_pages_truncated, compact_data.compact_truncate);
      return butil::Status();
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] compact failed, ret: {}.", ret);
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] error compacting, exception: {}.", db_exception.what());
  }

  return butil::Status(pb::error::EINTERNAL, "Internal compact error.");
}

std::vector<int64_t> BdbRawEngine::GetApproximateSizes(const std::string& cf_name,
                                                       std::vector<pb::common::Range>& ranges) {
  std::vector<int64_t> result_counts;

#ifndef BDB_FAST_GET_APROX_SIZE
  std::shared_ptr<bdb::Reader> bdb_reader = std::dynamic_pointer_cast<bdb::Reader>(reader_);
  if (bdb_reader == nullptr) {
    DINGO_LOG(ERROR) << "[bdb] reader pointer cast error.";
    return result_counts;
  }

  for (const auto& range : ranges) {
    int64_t count = 0;
    butil::Status status = bdb_reader->GetRangeCountByCursor(cf_name, nullptr, range.start_key(), range.end_key(),
                                                             DB_READ_UNCOMMITTED, count);
    if (BAIDU_UNLIKELY(!status.ok())) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] get range by cursor failed, status code: {}, message: {}",
                                      status.error_code(), status.error_str());
      return std::vector<int64_t>();
    }
    result_counts.push_back(count);
  }

#else

  // estimat count
  try {
    // DB_BTREE_STAT bdb_stat;
    DB_BTREE_STAT* bdb_stat = nullptr;
    DEFER(  // FOR_CLANG_FORMAT
        if (bdb_stat != nullptr) {
          // Note: must use free, not delete.
          // This struct is allocated by C.
          free(bdb_stat);
          bdb_stat = nullptr;
        });

    // use DB_FAST_STAT, Don't traverse the database
    int ret = db_->stat(nullptr, &bdb_stat, DB_FAST_STAT);
    // int ret = db_->stat(nullptr, &bdb_stat, DB_READ_UNCOMMITTED);
    if (BAIDU_UNLIKELY(ret != 0)) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] stat failed, cf_name: {}.", cf_name);
      return std::vector<int64_t>();
    }

    int64_t bdb_total_key_count = bdb_stat->bt_nkeys;

    for (const auto& range : ranges) {
      butil::Status status = Helper::CheckRange(range);
      if (BAIDU_UNLIKELY(!status.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] check range failed, cf_name: {}, status code: {}, message: {}", cf_name,
                                        status.error_code(), status.error_str());
        return std::vector<int64_t>();
      }

      int64_t count = 0;
      Dbt bdb_start_key, bdb_end_key;

      std::string store_start_key = bdb::BdbHelper::EncodeKey(cf_name, range.start_key());
      bdb::BdbHelper::BinaryToDbt(store_start_key, bdb_start_key);

      std::string store_end_key = bdb::BdbHelper::EncodeKey(cf_name, range.end_key());
      bdb::BdbHelper::BinaryToDbt(store_end_key, bdb_end_key);

      DB_KEY_RANGE range_start;
      memset(&range_start, 0, sizeof(range_start));
      ret = db_->key_range(nullptr, &bdb_start_key, &range_start, 0);
      if (BAIDU_UNLIKELY(ret != 0)) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] start key_range failed, cf_name: {}.", cf_name);
        return std::vector<int64_t>();
      }

      DB_KEY_RANGE range_end;
      memset(&range_end, 0, sizeof(range_end));
      ret = db_->key_range(nullptr, &bdb_end_key, &range_end, 0);
      if (BAIDU_UNLIKELY(ret != 0)) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] end key_range failed, cf_name: {}.", cf_name);
        return std::vector<int64_t>();
      }

      double range_result = 1.0 - range_start.less - range_end.equal - range_end.greater;
      if (range_result >= 1e-15) {
        count = int64_t(bdb_total_key_count * range_result);
      } else {
        count = 0;
      }

      DINGO_LOG(INFO) << fmt::format(
          "[bdb] cf_name: {}, count: {}, bdb total key count: {}, range_result: {}, range_start.less: {}, "
          "range_end.equal: "
          "{}, range_end.greater: {}.",
          cf_name, count, bdb_total_key_count, range_result, range_start.less, range_end.equal, range_end.greater);
      result_counts.push_back(count);
    }
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] error get approximate sizes, exception: {}.", db_exception.what());
    return std::vector<int64_t>();
  }

#endif

  return result_counts;
}

}  // namespace dingodb
