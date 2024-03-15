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
#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bdb_raw_engine.h"
#include "butil/compiler_specific.h"
#include "bvar/reducer.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "db.h"
#include "engine/iterator.h"
#include "fmt/core.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "rocksdb/sst_file_reader.h"
#include "third-party/build/bdb/db.h"
#include "third-party/build/bdb/db_cxx.h"

// #define BDB_FAST_GET_APROX_SIZE
#define BDB_BUILD_USE_BULK_DELETE

bvar::Adder<uint64_t> bdb_snapshot_alive_count("bdb_snapshot_alive_count");
bvar::Adder<uint64_t> bdb_transaction_alive_count("bdb_transaction_alive_count");

namespace bthread {
DECLARE_int32(bthread_concurrency);
}  // namespace bthread

namespace dingodb {

DEFINE_int32(bdb_base_backoff_ms, 10, "bdb dead lock backoff time(ms)");

DEFINE_int32(bdb_page_size, 32 * 1024, "bdb page size");

DEFINE_int32(bdb_env_cache_size_gb, 8, "bdb env cache size(GB)");
DEFINE_int32(bdb_env_cache_size_bytes, 0, "bdb env cache size(bytes)");
DEFINE_int32(bdb_txn_max, 65536, "bdb txn max");
DEFINE_int32(bdb_txn_memory_max, 262144, "bdb txn memory max");
DEFINE_int32(bdb_lk_max_lockers, 65536, "bdb max lockers");
DEFINE_int32(bdb_lk_max_locks, 262144, "bdb max locks");
DEFINE_int32(bdb_lk_max_objects, 262144, "bdb max objects");
DEFINE_int32(bdb_mutex_increment, 1024, "bdb mutex increment");
DEFINE_int32(bdb_lk_partitions, 0, "bdb lk partition");

DEFINE_int32(bdb_mutex_max, 0, "bdb mutex max, this is deprecated from bdb");

DEFINE_int32(bdb_max_retries, 30, "bdb max retry on a deadlock");
DEFINE_int32(bdb_ingest_external_file_batch_put_count, 128, "bdb ingest external file batch put cout");
DEFINE_int32(bdb_checkpoint_time_s, 60, "bdb checkpoint time interval(s)");
DEFINE_int32(bdb_dead_lock_detect_time_s, 1, "bdb dead_lock_detect interval(s)");
DEFINE_int32(bdb_stat_time_s, 60, "bdb stat time interval(s)");

DEFINE_bool(bdb_use_db_pool, false, "bdb use db pool");
DEFINE_int32(bdb_db_pool_size, 4096, "bdb db pool size, must bigger than bthread_connecurrency");

namespace bdb {

#define DB_MAXIMUM_PAGESIZE (64 * 1024) /* Maximum database page size */

// we use visable char as prefix, so we can use it as a range upper bound and better for debug.
std::unordered_map<std::string, char> BdbHelper::cf_name_to_id = {
    {"default", '0'}, {"meta", '1'}, {"vector_scalar", '2'}, {"vector_table", '3'},
    {"data", '4'},    {"lock", '5'}, {"write", '6'}};

std::unordered_map<char, std::string> BdbHelper::cf_id_to_name = {
    {'0', "default"}, {'1', "meta"}, {'2', "vector_scalar"}, {'3', "vector_table"},
    {'4', "data"},    {'5', "lock"}, {'6', "write"}};

static void DelayBeforeNextRetry(int32_t retry_count) {
  if (retry_count <= (FLAGS_bdb_max_retries / 3)) {
    return;
  } else {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(retry_count, 2 * (FLAGS_bdb_max_retries + retry_count));
    bthread_usleep(FLAGS_bdb_base_backoff_ms * 1000L * distr(gen));
  }
}

// BdbHelper
std::string BdbHelper::EncodeKey(const std::string& cf_name, const std::string& key) {
  return EncodeKey(GetCfId(cf_name), key);
}

std::string BdbHelper::EncodeKey(char cf_id, const std::string& key) {
  return std::string(CF_ID_LEN, cf_id).append(key);
}

std::string BdbHelper::EncodeCf(char cf_id) { return std::string(CF_ID_LEN, cf_id); }

std::string BdbHelper::EncodeCf(const std::string& cf_name) { return EncodeCf(GetCfId(cf_name)); }

int BdbHelper::DecodeKey(const std::string& /*cf_name*/, const Dbt& bdb_key, std::string& key) {
  CHECK(bdb_key.get_data() != nullptr);
  CHECK(bdb_key.get_size() > CF_ID_LEN);

  key.assign((const char*)bdb_key.get_data() + CF_ID_LEN, bdb_key.get_size() - CF_ID_LEN);
  return -1;
}

void BdbHelper::DbtToString(const Dbt& dbt, std::string& str) {
  str.assign((const char*)dbt.get_data(), dbt.get_size());
}

// This function is only used to decode the key, for the value, the data in dbt is equal the user data.
void BdbHelper::DbtToUserKey(const Dbt& dbt, std::string& user_data) {
  user_data.assign((const char*)(dbt.get_data()) + CF_ID_LEN, dbt.get_size() - CF_ID_LEN);
}

std::string BdbHelper::DbtToString(const Dbt& dbt) { return std::string((const char*)dbt.get_data(), dbt.get_size()); }

uint32_t BdbHelper::GetKeysSize(const std::vector<std::string>& keys) {
  uint32_t size = 0;

  for (const auto& key : keys) {
    size += key.size();
  }

  return size;
}

void BdbHelper::StringToDbt(const std::string& str, Dbt& dbt) {
  dbt.set_data((void*)str.data());
  dbt.set_size(str.size());
}

int BdbHelper::CompareDbt(const Dbt& dbt1, const Dbt& dbt2) {
  assert(dbt1.get_data() != nullptr && dbt2.get_data() != nullptr);

  std::string_view sv1((const char*)dbt1.get_data(), dbt1.get_size());
  std::string_view sv2((const char*)dbt2.get_data(), dbt2.get_size());

  return sv1.compare(sv2);
}

std::string BdbHelper::EncodedCfUpperBound(char cf_id) { return std::string(CF_ID_LEN, cf_id + 1); };

std::string BdbHelper::EncodedCfUpperBound(const std::string& cf_name) {
  return EncodedCfUpperBound(GetCfId(cf_name));
};

char BdbHelper::GetCfId(const std::string& cf_name) {
  auto it = cf_name_to_id.find(cf_name);
  if (BAIDU_UNLIKELY(it == cf_name_to_id.end())) {
    DINGO_LOG(FATAL) << fmt::format("[bdb] invalid cf name: {}.", cf_name);
  }
  return it->second;
}

std::string BdbHelper::GetCfName(char cf_id) {
  auto it = cf_id_to_name.find(cf_id);
  if (BAIDU_UNLIKELY(it == cf_id_to_name.end())) {
    DINGO_LOG(FATAL) << fmt::format("[bdb] invalid cf id: {}.", cf_id);
  }
  return it->second;
}

int BdbHelper::TxnCommit(DbTxn** txn_ptr) {
  if (BAIDU_UNLIKELY(*txn_ptr == nullptr)) {
    return 0;
  }

  auto ret = (*txn_ptr)->commit(0);
  if (ret == 0) {
    *txn_ptr = nullptr;
  }

  bdb_transaction_alive_count << -1;

  return ret;
}

int BdbHelper::TxnAbort(DbTxn** txn_ptr) {
  if (BAIDU_UNLIKELY(*txn_ptr == nullptr)) {
    return 0;
  }

  auto ret = (*txn_ptr)->abort();
  if (ret == 0) {
    *txn_ptr = nullptr;
  }

  bdb_transaction_alive_count << -1;

  return ret;
}

void BdbHelper::GetLogFileNames(DbEnv* env, std::set<std::string>& file_names) {
  // use log_archive to get log files
  try {
    char** list;
    // int ret = env->log_archive(&list, DB_ARCH_ABS | DB_ARCH_LOG);  // Get the list of log files we can remove
    int ret = env->log_archive(&list, DB_ARCH_ABS);  // Get the list of log files we can remove
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, ret: {}.", ret);
    } else {
      // If the call to log_archive succeeded, list points to an array of log filenames
      if (list == nullptr) {
        DINGO_LOG(INFO) << fmt::format("[bdb] log archive succeed, list is nullptr.");
        return;
      }

      for (char** p = list; *p; ++p) {
        std::string filename(*p);
        file_names.insert(filename);
      }

      // Free the memory allocated by log_archive
      free(list);
    }
  } catch (DbException& db_exception) {
    PrintEnvStat(env);
    DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, std exception: {}.", std_exception.what());
  }
}

void BdbHelper::PrintEnvStat(DbEnv* env) {
  if (env == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] env is nullptr.");
    return;
  }

  // print db_stat result
  try {
    int ret = env->stat_print(DB_STAT_SUBSYSTEM);
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] db_stat failed, ret: {}.", ret);
    } else {
      DINGO_LOG(INFO) << fmt::format("[bdb] db_stat ok");
    }
  } catch (DbException& db_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db_stat failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] db_stat failed, std exception: {}.", std_exception.what());
  }
}

void BdbHelper::CheckpointThread(DbEnv* env, Db* /*db*/, std::atomic<bool>& is_close) {
  CHECK(env != nullptr);

  for (uint64_t i = 1;; i++) {
    if (is_close.load()) {
      break;
    }

    // do lock_detect every 1s
    if (i % FLAGS_bdb_dead_lock_detect_time_s == 0) {
      try {
        int ret = env->lock_detect(0, DB_LOCK_MINWRITE, nullptr);
        if (ret != 0) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] env->lock_detect failed, ret: {}.", ret);
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(env);
        DINGO_LOG(ERROR) << fmt::format("[bdb] env->lock_detect failed, exception: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
      } catch (std::exception& std_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] env->lock_detect failed, std exception: {}.", std_exception.what());
      }
    }

    if (i % FLAGS_bdb_stat_time_s == 0) {
      // print db_stat result
      try {
        int ret = env->stat_print(DB_STAT_SUBSYSTEM);
        if (ret != 0) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] env->stat_print failed, ret: {}.", ret);
        } else {
          DINGO_LOG(INFO) << fmt::format("[bdb] env->stat_print ok");
        }

        // ret = env->memp_sync(nullptr);
        // if (ret != 0) {
        //   DINGO_LOG(ERROR) << fmt::format("[bdb] env->memp_sync failed, ret: {}.", ret);
        // } else {
        //   DINGO_LOG(INFO) << fmt::format("[bdb] env->memp_sync ok");
        // }

        // ret = db->sync(0);
        // if (ret != 0) {
        //   DINGO_LOG(ERROR) << fmt::format("[bdb] db->sync failed, ret: {}.", ret);
        // } else {
        //   DINGO_LOG(ERROR) << fmt::format("[bdb] db->sync ok");
        // }
      } catch (DbException& db_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] db_stat failed, exception: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
      } catch (std::exception& std_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] db_stat failed, std exception: {}.", std_exception.what());
      }
    }

    // do checkpoint every 60s
    if (i % FLAGS_bdb_checkpoint_time_s == 0) {
      try {
        int ret = env->txn_checkpoint(0, 0, 0);
        if (ret != 0) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] checkpoint failed, ret: {}.", ret);
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(env);
        DINGO_LOG(ERROR) << fmt::format("[bdb] checkpoint failed, exception: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
      } catch (std::exception& std_exception) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] checkpoint failed, std exception: {}.", std_exception.what());
      }

      // we need to print the file_names before remove them, so we do not use log_archive to remove log files.
      // use log_archive to remove log files
      // try {
      //   char** list;
      //   int ret = env->log_archive(&list, DB_ARCH_REMOVE);
      //   if (ret != 0) {
      //     DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, ret: {}.", ret);
      //   }
      // } catch (DbException& db_exception) {
      //   DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, exception: {} {}.", db_exception.get_errno(),
      //                                   db_exception.what());
      // } catch (std::exception& std_exception) {
      //   DINGO_LOG(ERROR) << fmt::format("[bdb] log archive failed, std exception: {}.", std_exception.what());
      // }

      // use log_archive to get log files
      std::set<std::string> log_file_names_to_remove;
      GetLogFileNames(env, log_file_names_to_remove);

      if (log_file_names_to_remove.empty()) {
        DINGO_LOG(INFO) << fmt::format("[bdb] checkpoint thread sleep {}s. no log files to remove.",
                                       FLAGS_bdb_checkpoint_time_s);
      } else {
        // use DINGO_LOG to print all log file names
        std::string log_file_names;
        for (const auto& file_name : log_file_names_to_remove) {
          log_file_names.append("[").append(file_name).append("]");
        }

        DINGO_LOG(INFO) << fmt::format("[bdb] checkpoint thread sleep {}s. will remove log files: {}.",
                                       FLAGS_bdb_checkpoint_time_s, log_file_names);

        for (const auto& file_name : log_file_names_to_remove) {
          int ret = std::remove(file_name.c_str());
          if (ret != 0) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] remove log file failed, ret: {}, error: {}.", ret,
                                            std::strerror(errno));
          }
        }
      }
    }

    // bthread_usleep(1000L * 1000L);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// Iterator
Iterator::Iterator(const std::string& cf_name, const IteratorOptions& options, Dbc* cursorp,
                   std::shared_ptr<bdb::BdbSnapshot> bdb_snapshot)
    : options_(options), cursorp_(cursorp), valid_(false) {
  cf_id_ = BdbHelper::GetCfId(cf_name);
  cf_upper_bound_ = BdbHelper::EncodedCfUpperBound(cf_id_);
  // TODO: use DB_DBT_MALLOC may cause memory leak, need to fix in the furture.
  // bdb_value_.set_flags(DB_DBT_MALLOC);
  snapshot_ = bdb_snapshot;

  // setup raw_start_key
  if (!options.lower_bound.empty()) {
    raw_start_key_ = BdbHelper::EncodeKey(cf_id_, options.lower_bound);
  } else {
    raw_start_key_ = BdbHelper::EncodeCf(cf_id_);
  }

  // setup raw_end_key
  if (!options.upper_bound.empty()) {
    raw_end_key_ = BdbHelper::EncodeKey(cf_id_, options.upper_bound);
  } else {
    raw_end_key_ = cf_upper_bound_;
  }
}

Iterator::~Iterator() {
  if (cursorp_ != nullptr) {
    try {
      cursorp_->close();
      cursorp_ = nullptr;
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetEnv());
      DINGO_LOG(WARNING) << fmt::format("cursor close failed, exception: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
    }
  }
};

bool Iterator::Valid() const {
  if (!valid_) {
    return false;
  }

  // check raw_start_key and raw_end_key
  std::string_view sv((const char*)bdb_key_.get_data(), bdb_key_.get_size());

  if (raw_start_key_.compare(sv) > 0) {
    return false;
  }

  if (raw_end_key_.compare(sv) <= 0) {
    return false;
  }

  return true;
}

void Iterator::SeekToFirst() { Seek(std::string()); }

void Iterator::SeekToLast() {
  valid_ = false;
  status_ = butil::Status();

  BdbHelper::StringToDbt(cf_upper_bound_, bdb_key_);
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
    BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db seek failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::Seek(const std::string& target) {
  valid_ = false;
  status_ = butil::Status();

  std::string store_key = std::string(1, cf_id_) + target;
  BdbHelper::StringToDbt(store_key, bdb_key_);

  try {
    // find smallest key greater than or equal to the target.
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_SET_RANGE);
    if (ret == 0) {
      valid_ = true;
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
    BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db seek failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

void Iterator::SeekForPrev(const std::string& target) {
  valid_ = false;
  status_ = butil::Status();

  std::string store_key = std::string(1, cf_id_) + target;
  Dbt bdb_target_key;
  BdbHelper::StringToDbt(store_key, bdb_target_key);
  BdbHelper::StringToDbt(store_key, bdb_key_);

  try {
    // find smallest key greater than or equal to the target.
    int ret = cursorp_->get(&bdb_key_, &bdb_value_, DB_SET_RANGE);
    if (ret == 0) {
      if (BdbHelper::CompareDbt(bdb_target_key, bdb_key_) <= 0) {
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
    BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] db seek for prev failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
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
    BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] db cursor next failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
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
    BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] db cursor prev failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
    status_ = butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db cursor prev failed, {}.", db_exception.what()));
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    status_ = butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
  }
}

// Snapshot
BdbSnapshot::BdbSnapshot(Db* db, DbTxn* txn, Dbc* cursorp, ResourcePool<Db*>* db_pool)
    : db_(db), txn_(txn), cursorp_(cursorp), db_pool_(db_pool) {
  bdb_snapshot_alive_count << 1;
}

BdbSnapshot::~BdbSnapshot() {
  // close cursor first
  if (cursorp_ != nullptr) {
    try {
      cursorp_->close();
      cursorp_ = nullptr;
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetEnv());
      DINGO_LOG(WARNING) << fmt::format("cursor close failed, exception: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
    }
  }

  DEFER(BdbHelper::TxnAbort(&txn_));

  // commit or abort txn
  if (txn_ != nullptr) {
    int ret = 0;
    // commit
    try {
      ret = BdbHelper::TxnCommit(&txn_);
      DINGO_LOG(DEBUG) << fmt::format("[bdb] txn commit in snapshot destruction, ret: {}.", ret);
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      ret = BdbHelper::kCommitException;
    }

    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
    }
  }

  // if use db pool, put back db to pool
  if (FLAGS_bdb_use_db_pool && db_pool_ != nullptr) {
    db_pool_->Put(db_);
  }

  bdb_snapshot_alive_count << -1;
}

// Reader
butil::Status Reader::KvGet(const std::string& cf_name, const std::string& key, std::string& value) {
  return KvGet(cf_name, GetSnapshot(), key, value);
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
    BdbHelper::StringToDbt(store_key, bdb_key);

    Dbt bdb_value;
    bdb_value.set_flags(DB_DBT_MALLOC);

    int ret = 0;
    if (snapshot != nullptr) {
      std::shared_ptr<bdb::BdbSnapshot> ss = std::dynamic_pointer_cast<bdb::BdbSnapshot>(snapshot);
      if (ss == nullptr) {
        DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
        return butil::Status(pb::error::EINTERNAL, "snapshot pointer cast error.");
      }
      ret = ss->GetDb()->get(ss->GetDbTxn(), &bdb_key, &bdb_value, 0);
    } else {
      Db* db = GetDb();
      DEFER(PutDb(db));
      ret = db->get(nullptr, &bdb_key, &bdb_value, 0);
    }

    if (ret == 0) {
      BdbHelper::DbtToString(bdb_value, value);
      if (bdb_value.get_data() != nullptr) {
        free(bdb_value.get_data());
      }
      return butil::Status();
    } else if (ret == DB_NOTFOUND) {
      DINGO_LOG(DEBUG) << "[bdb] key not found.";
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key.");
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] db get failed, ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal get error.");
  } catch (DbException& db_exception) {
    BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
    DINGO_LOG(ERROR) << "[bdb] db exception: " << db_exception.get_errno() << " " << db_exception.what();
    return butil::Status(pb::error::EBDB_EXCEPTION, "%s", db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << "[bdb] std exception: " << std_exception.what();
    return butil::Status(pb::error::ESTD_EXCEPTION, "%s", std_exception.what());
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknow error.");
}

butil::Status Reader::KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  return KvScan(cf_name, GetSnapshot(), start_key, end_key, kvs);
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

  IteratorOptions options;
  options.lower_bound = start_key;
  options.upper_bound = end_key;
  auto iter = NewIterator(cf_name, snapshot, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] create iterator failed.");
    return butil::Status(pb::error::EINTERNAL, "Internal create iterator error.");
  }

  iter->Seek(start_key);
  while (iter->Valid()) {
    pb::common::KeyValue kv;
    kv.set_key(iter->Key().data(), iter->Key().size());
    kv.set_value(iter->Value().data(), iter->Value().size());
    kvs.push_back(std::move(kv));
    iter->Next();
  }

  return butil::Status::OK();
}

butil::Status Reader::KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                              int64_t& count) {
  return KvCount(cf_name, GetSnapshot(), start_key, end_key, count);
}

butil::Status Reader::KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                              const std::string& end_key, int64_t& count) {
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

  IteratorOptions options;
  options.lower_bound = start_key;
  options.upper_bound = end_key;
  auto iter = NewIterator(cf_name, snapshot, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] create iterator failed.");
    return butil::Status(pb::error::EINTERNAL, "Internal create iterator error.");
  }

  iter->Seek(start_key);
  while (iter->Valid()) {
    ++count;
    iter->Next();
  }

  return butil::Status::OK();
}

std::shared_ptr<dingodb::Iterator> Reader::NewIterator(const std::string& cf_name, IteratorOptions options) {
  return NewIterator(cf_name, GetSnapshot(), options);
}

std::shared_ptr<dingodb::Iterator> Reader::NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                                       IteratorOptions options) {
  CHECK(snapshot != nullptr);

  // Acquire a cursor
  Dbc* cursorp = nullptr;
  int ret = 0;
  try {
    if (snapshot != nullptr) {
      std::shared_ptr<bdb::BdbSnapshot> ss = std::dynamic_pointer_cast<bdb::BdbSnapshot>(snapshot);
      if (ss != nullptr) {
        ret = ss->GetDb()->cursor(ss->GetDbTxn(), &cursorp, DB_CURSOR_BULK | DB_TXN_SNAPSHOT);
        if (ret == 0) {
          return std::make_shared<Iterator>(cf_name, options, cursorp, ss);
        }
      } else {
        DINGO_LOG(ERROR) << "[bdb] snapshot pointer cast error.";
      }
    } else {
      Db* db = GetDb();
      DEFER(PutDb(db));
      ret = db->cursor(nullptr, &cursorp, DB_READ_COMMITTED);
      if (ret == 0) {
        return std::make_shared<Iterator>(cf_name, options, cursorp, nullptr);
      }
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] cursor create failed, ret: {}.", ret);
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] got DeadLockException, giving up.");
  } catch (DbException& db_exception) {
    BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] cursor create failed, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
  } catch (std::exception& std_exception) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
  }

  return nullptr;
}

butil::Status Reader::GetRangeKeys(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                                   std::vector<std::string>& keys) {
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

  IteratorOptions options;
  options.lower_bound = start_key;
  options.upper_bound = end_key;
  auto iter = NewIterator(cf_name, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] create iterator failed.");
    return butil::Status(pb::error::EINTERNAL, "Internal create iterator error.");
  }

  iter->Seek(start_key);
  while (iter->Valid()) {
    keys.emplace_back(iter->Key());
    iter->Next();
  }

  return butil::Status::OK();
}

butil::Status Reader::GetRangeKeyValueSize(const std::string& cf_name, const std::string& start_key,
                                           const std::string& end_key, int64_t limit, int64_t& key_size,
                                           int64_t& value_size) {
  key_size = 0;
  value_size = 0;

  DINGO_LOG(DEBUG) << fmt::format("[bdb] get range key value size, cf_name: {}, start_key: {}, end_key: {}, limit: {}.",
                                  cf_name, Helper::StringToHex(start_key), Helper::StringToHex(end_key), limit);

  if (limit == 0) {
    return butil::Status();
  }

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

  IteratorOptions options;
  options.lower_bound = start_key;
  options.upper_bound = end_key;
  auto iter = NewIterator(cf_name, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] create iterator failed.");
    return butil::Status(pb::error::EINTERNAL, "Internal create iterator error.");
  }

  iter->Seek(start_key);
  while (iter->Valid() && limit > 0) {
    key_size += iter->Key().size();
    value_size += iter->Value().size();

    limit -= 1;

    iter->Next();
  }

  return butil::Status::OK();
}

std::shared_ptr<BdbRawEngine> Reader::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[bdb] get raw engine failed.";
  }

  return raw_engine;
}

Db* Reader::GetDb() { return GetRawEngine()->GetDb(); }
void Reader::PutDb(Db* db) { return GetRawEngine()->PutDb(db); }

dingodb::SnapshotPtr Reader::GetSnapshot() { return GetRawEngine()->GetSnapshot(); }

butil::Status Reader::RetrieveByCursor(const std::string& cf_name, DbTxn* txn, const std::string& key,
                                       std::string& value) {
  CHECK(txn != nullptr);

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
          BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
          LOG(WARNING) << fmt::format("[bdb] cursor close failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
        }
      });

  try {
    // Get the cursor
    Db* db = GetDb();
    DEFER(PutDb(db));
    db->cursor(txn, &cursorp, DB_READ_COMMITTED);

    std::string store_key = BdbHelper::EncodeKey(cf_name, key);
    Dbt bdb_key;
    BdbHelper::StringToDbt(store_key, bdb_key);

    Dbt bdb_value;
    int ret = cursorp->get(&bdb_key, &bdb_value, DB_FIRST);
    if (ret == 0) {
      BdbHelper::DbtToString(bdb_value, value);
      return butil::Status();
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor failed, ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal get error.");
  } catch (DbDeadlockException&) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor, got deadlock.");
    return butil::Status(pb::error::EBDB_DEADLOCK, "retrive by cursor, got deadlock.");
  } catch (DbException& db_exception) {
    BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] retrive by cursor, got db exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
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

  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(BdbHelper::TxnAbort(&txn));

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      bdb_transaction_alive_count << 1;

      std::string store_key = BdbHelper::EncodeKey(cf_name, kv.key());
      Dbt bdb_key;
      BdbHelper::StringToDbt(store_key, bdb_key);

      Dbt bdb_value;
      BdbHelper::StringToDbt(kv.value(), bdb_value);

      ret = db->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] put failed, ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal put error.");
      }

      // commit
      try {
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          return butil::Status::OK();
        } else {
          DINGO_LOG(ERROR) << fmt::format("[bdb] txn commit failed, ret: {}.", ret);
          return butil::Status(pb::error::EINTERNAL, "Internal txn commit error.");
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION,
                           fmt::format("db put failed, {} {}.", db_exception.get_errno(), db_exception.what()));
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

  DINGO_LOG(DEBUG) << fmt::format("[bdb] batch put and delete, cf_name: {}, put size: {}, delete size: {}.", cf_name,
                                  kvs_to_put.size(), keys_to_delete.size());

  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(BdbHelper::TxnAbort(&txn));

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      bdb_transaction_alive_count << 1;

      for (const auto& kv : kvs_to_put) {
        if (BAIDU_UNLIKELY(kv.key().empty())) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty key.");
          return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
        }

        std::string store_key = BdbHelper::EncodeKey(cf_name, kv.key());
        Dbt bdb_key;
        BdbHelper::StringToDbt(store_key, bdb_key);
        Dbt bdb_value;
        BdbHelper::StringToDbt(kv.value(), bdb_value);
        ret = db->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
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
        BdbHelper::StringToDbt(store_key, bdb_key);
        db->del(txn, &bdb_key, 0);
        if (ret != 0 && ret != DB_NOTFOUND) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
          return butil::Status(pb::error::EINTERNAL, "Internal put error.");
        }
      }

      // commit
      try {
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          DINGO_LOG(DEBUG) << fmt::format(
              "[bdb] batch put and delete success, cf_name: {}, put size: {}, delete size: {}.", cf_name,
              kvs_to_put.size(), keys_to_delete.size());
          return butil::Status();
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db put failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  DINGO_LOG(INFO) << fmt::format("[bdb] batch put and delete unknown, cf_name: {}, put size: {}, delete size: {}.",
                                 cf_name, kvs_to_put.size(), keys_to_delete.size());

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvBatchPutAndDelete(
    const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) {
  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;

  // release txn if commit failed.
  DEFER(BdbHelper::TxnAbort(&txn));

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      bdb_transaction_alive_count << 1;

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
          BdbHelper::StringToDbt(store_key, bdb_key);
          Dbt bdb_value;
          BdbHelper::StringToDbt(kv.value(), bdb_value);
          ret = db->put(txn, &bdb_key, &bdb_value, DB_OVERWRITE_DUP);
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
          BdbHelper::StringToDbt(store_key, bdb_key);
          db->del(txn, &bdb_key, 0);
          if (ret != 0 && ret != DB_NOTFOUND) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
            return butil::Status(pb::error::EINTERNAL, "Internal put error.");
          }
        }
      }

      // commit
      try {
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          return butil::Status::OK();
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db put failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db put failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvBatchDelete(const std::string& cf_name, const std::vector<std::string>& keys) {
  if (BAIDU_UNLIKELY(keys.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] not support empty keys.");
    return butil::Status(pb::error::EKEY_EMPTY, "Keys is empty");
  }

  std::vector<std::string> raw_keys;
  raw_keys.reserve(keys.size());
  for (const auto& key : keys) {
    raw_keys.emplace_back(BdbHelper::EncodeKey(cf_name, key));
    DINGO_LOG(DEBUG) << fmt::format("[bdb] delete raw key: {} {}.", Helper::StringToHex(key),
                                    Helper::StringToHex(BdbHelper::EncodeKey(cf_name, key)));
  }

  DINGO_LOG(DEBUG) << " raw_keys.size: " << raw_keys.size() << ", keys.size: " << keys.size() << ".";

  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;

  Dbt keys_to_delete;
  auto keys_size = 0;  // BdbHelper::GetKeysSize(raw_keys);

  for (const auto& key : raw_keys) {
    keys_size += sizeof(uint32_t) + key.size();
  }

  uint64_t keys_buf_size = sizeof(uint32_t) * raw_keys.size() + keys_size + sizeof(uint32_t);
  keys_buf_size += 1024 - (keys_buf_size % 1024);

  /*
   * In order to use a bulk DBT for get() calls, it must be at least as
   * large as the database's pages. Make sure that it as least 64KB, the
   * maximum database page size.
   */
  if (keys_buf_size < DB_MAXIMUM_PAGESIZE) {
    keys_buf_size = DB_MAXIMUM_PAGESIZE;
  }

  if (BAIDU_UNLIKELY(keys_buf_size > UINT32_MAX)) {
    DINGO_LOG(FATAL) << fmt::format("[bdb] keys_buf_size: {} > UINT32_MAX.", keys_buf_size);
  }

  void* keys_buf = malloc(keys_buf_size);
  if (keys_buf == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] malloc failed, size: {}.", keys_size);
    return butil::Status(pb::error::EINTERNAL, "Internal malloc error.");
  }

  memset(keys_buf, 0, keys_buf_size);
  keys_to_delete.set_ulen(keys_buf_size);
  keys_to_delete.set_flags(DB_DBT_USERMEM | DB_DBT_BULK);
  keys_to_delete.set_data(keys_buf);

  DEFER(  // FOR_CLANG_FORMAT
      if (keys_buf != nullptr) {
        free(keys_buf);
        keys_buf = nullptr;
      });

  DINGO_LOG(DEBUG) << "raw_keys.size = " << raw_keys.size() << ", keys_buf_size = " << keys_buf_size;

  DbMultipleDataBuilder* db_multi_data_builder = new DbMultipleDataBuilder(keys_to_delete);
  if (db_multi_data_builder == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] DbMultipleKeyDataBuilder failed.");
    return butil::Status(pb::error::EINTERNAL, "Internal DbMultipleKeyDataBuilder error.");
  }

  for (const auto& raw_key : raw_keys) {
    if (!db_multi_data_builder->append((void*)raw_key.data(), raw_key.size())) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] DbMultipleKeyDataBuilder append failed.");
      return butil::Status(pb::error::EINTERNAL, "Internal DbMultipleKeyDataBuilder append error.");
    }
  }

  // release txn if commit failed.
  DEFER(  // FOR_CLANG_FORMAT
      BdbHelper::TxnAbort(&txn);

      if (db_multi_data_builder != nullptr) {
        delete db_multi_data_builder;
        db_multi_data_builder = nullptr;
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

      bdb_transaction_alive_count << 1;

      ret = db->del(txn, &keys_to_delete, DB_MULTIPLE);
      if (ret != 0 && ret != DB_NOTFOUND) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal delete error.");
      }

      // commit
      try {
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          return butil::Status::OK();
        } else {
          DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
          return butil::Status(pb::error::EBDB_COMMIT, "error on txn commit.");
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db delete failed, {}.", db_exception.what()));
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

  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;
  // release txn if commit failed.
  DEFER(BdbHelper::TxnAbort(&txn));

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      bdb_transaction_alive_count << 1;

      std::string store_key = BdbHelper::EncodeKey(cf_name, key);
      Dbt bdb_key;
      BdbHelper::StringToDbt(store_key, bdb_key);

      ret = db->del(txn, &bdb_key, 0);
      if (ret != 0 && ret != DB_NOTFOUND) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] delete failed, ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal delete error.");
      }

      // commit
      try {
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          return butil::Status::OK();
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
        DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                        db_exception.what());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
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
#ifdef BDB_BUILD_USE_BULK_DELETE
  return KvBatchDeleteRangeBulk(range_with_cfs);
#else
  return KvBatchDeleteRangeNormal(range_with_cfs);
#endif
}

butil::Status Writer::KvBatchDeleteRangeNormal(
    const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) {
  Db* db = GetDb();
  DEFER(PutDb(db));

  DbEnv* envp = db->get_env();
  DbTxn* txn = nullptr;

  // release txn if commit failed.
  DEFER(BdbHelper::TxnAbort(&txn));

  bool retry = true;
  int32_t retry_count = 0;

  while (retry) {
    try {
      int ret = envp->txn_begin(nullptr, &txn, 0);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return butil::Status(pb::error::EINTERNAL, "Internal txn begin error.");
      }

      bdb_transaction_alive_count << 1;

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
        ret = BdbHelper::TxnCommit(&txn);
        if (ret == 0) {
          return butil::Status::OK();
        }
      } catch (DbException& db_exception) {
        BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
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
        BdbHelper::TxnAbort(&txn);

        DINGO_LOG(WARNING) << fmt::format(
            "[bdb] writer got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.", retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db delete failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status(pb::error::EBDB_UNKNOW, "unknown error.");
}

butil::Status Writer::KvBatchDeleteRangeBulk(
    const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) {
  bool retry = true;
  int32_t retry_count = 0;
  auto reader = std::dynamic_pointer_cast<bdb::Reader>(this->GetRawEngine()->Reader());

  while (retry) {
    try {
      for (const auto& [cf_name, ranges] : range_with_cfs) {
        for (const auto& range : ranges) {
          if (range.start_key().empty() || range.end_key().empty()) {
            return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
          }
          if (range.start_key() >= range.end_key()) {
            return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
          }

          std::vector<std::string> keys;
          auto status = reader->GetRangeKeys(cf_name, range.start_key(), range.end_key(), keys);
          if (!status.ok()) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] get keys in range: {}.", status.error_cstr());
            return status;
          }

          if (keys.empty()) {
            continue;
          }

          auto ret = KvBatchDelete(cf_name, keys);
          if (!ret.ok()) {
            DINGO_LOG(ERROR) << fmt::format("[bdb] delete keys: {}.", ret.error_cstr());
            return ret;
          } else {
            DINGO_LOG(DEBUG) << fmt::format("[bdb] delete keys: {}.", keys.size());
            continue;
          }
        }
      }

      return butil::Status::OK();

    } catch (DbDeadlockException&) {
      // Now we decide if we want to retry the operation.
      // If we have retried less than FLAGS_bdb_max_retries,
      // increment the retry count and goto retry.
      if (retry_count < FLAGS_bdb_max_retries) {
        DINGO_LOG(WARNING) << fmt::format("[bdb] got DB_LOCK_DEADLOCK. retrying delete operation, retry_count: {}.",
                                          retry_count);
        retry_count++;
        DelayBeforeNextRetry(retry_count);
        retry = true;
      } else {
        // Otherwise, just give up.
        DINGO_LOG(ERROR) << fmt::format("[bdb] writer got DeadLockException and out of retries: {}. giving up.",
                                        retry_count);
        return butil::Status(pb::error::EBDB_DEADLOCK, "writer got DeadLockException and out of retries. giving up.");
      }
    } catch (DbException& db_exception) {
      BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db delete failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return butil::Status(pb::error::EBDB_EXCEPTION, fmt::format("db delete failed, {}.", db_exception.what()));
    } catch (std::exception& std_exception) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] std exception, {}.", std_exception.what());
      return butil::Status(pb::error::ESTD_EXCEPTION, fmt::format("std exception, {}.", std_exception.what()));
    }
  }

  return butil::Status::OK();
}

butil::Status Writer::DeleteRangeByCursor(const std::string& cf_name, const pb::common::Range& range, DbTxn* txn) {
  CHECK(txn != nullptr);

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
          BdbHelper::PrintEnvStat(GetRawEngine()->GetEnv());
          LOG(WARNING) << fmt::format("cursor close failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
        }
      });

  Db* db = GetDb();
  DEFER(PutDb(db));

  db->cursor(txn, &cursorp, DB_CURSOR_BULK | DB_TXN_SNAPSHOT);

  std::string store_key = BdbHelper::EncodeKey(cf_name, range.start_key());
  Dbt bdb_key;
  BdbHelper::StringToDbt(store_key, bdb_key);

  std::string store_end_key = BdbHelper::EncodeKey(cf_name, range.end_key());
  Dbt bdb_end_key;
  BdbHelper::StringToDbt(store_end_key, bdb_end_key);

  Dbt bdb_value;
  // bdb_value.set_flags(DB_DBT_MALLOC);

  // find fist postion
  int ret = cursorp->get(&bdb_key, &bdb_value, DB_SET_RANGE);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[bdb] txn get failed ret: {}.", ret);
    return butil::Status(pb::error::EINTERNAL, "Internal txn get error.");
  }

  DINGO_LOG(INFO) << fmt::format(
      "[bdb] get key: {}, value: {}",
      Helper::StringToHex(std::string_view((const char*)bdb_key.get_data(), bdb_key.get_size())),
      Helper::StringToHex(std::string_view((const char*)bdb_value.get_data(), bdb_value.get_size())));

  if (BdbHelper::CompareDbt(bdb_key, bdb_end_key) < 0) {
    cursorp->del(0);
  } else {
    return butil::Status();
  }

  while (cursorp->get(&bdb_key, &bdb_value, DB_NEXT) == 0) {
    if (BdbHelper::CompareDbt(bdb_key, bdb_end_key) < 0) {
      cursorp->del(0);
    } else {
      break;
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

Db* Writer::GetDb() { return GetRawEngine()->GetDb(); }
void Writer::PutDb(Db* db) { return GetRawEngine()->PutDb(db); }

}  // namespace bdb

// Open a BDB database
int32_t BdbRawEngine::OpenDb(Db** dbpp, const char* file_name, DbEnv* envp, uint32_t extra_flags) {
  int ret;
  uint32_t open_flags;

  DbTxn* txn = nullptr;
  try {
    int ret = envp->txn_begin(nullptr, &txn, 0);
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
      return -1;
    }

    bdb_transaction_alive_count << 1;

    Db* db = new Db(envp, 0);

    // Point to the new'd Db
    *dbpp = db;
    db->set_pagesize(FLAGS_bdb_page_size);

    if (extra_flags != 0) {
      ret = db->set_flags(extra_flags);
    }

    // Now open the database */
    open_flags = DB_CREATE |        // Allow database creation
                                    //  DB_READ_UNCOMMITTED |  // Allow uncommitted reads
                                    //  DB_AUTO_COMMIT |   // Allow autocommit
                 DB_MULTIVERSION |  // Multiversion concurrency control
                 DB_THREAD;         // Cause the database to be free-threade1

    db->open(txn,         // Txn pointer
             file_name,   // File name
             nullptr,     // Logical db name
             DB_BTREE,    // Database type (using btree)
             open_flags,  // Open flags
             0);          // File mode. Using defaults

    // commit
    try {
      ret = bdb::BdbHelper::TxnCommit(&txn);
      if (ret == 0) {
        return 0;
      } else {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn commit failed, ret: {}.", ret);
      }
    } catch (DbException& db_exception) {
      bdb::BdbHelper::PrintEnvStat(envp);
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      ret = -1;
    }

    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error on txn commit, ret: {}.", ret);
      return -1;
    }

  } catch (DbException& db_exception) {
    bdb::BdbHelper::PrintEnvStat(envp);
    DINGO_LOG(ERROR) << fmt::format("OpenDb: db open failed: {} {}.", db_exception.get_errno(), db_exception.what());
    bdb::BdbHelper::TxnAbort(&txn);
    return -1;
  }

  return 0;
}

Db* BdbRawEngine::GetDb() {
  Db* db = nullptr;
  if (FLAGS_bdb_use_db_pool && !db_handles_.empty()) {
    db = db_pool_.Get();
    return db;
  } else {
    return db_;
  }
}

void BdbRawEngine::PutDb(Db* db) {
  if (FLAGS_bdb_use_db_pool && !db_handles_.empty()) {
    db_pool_.Put(db);
  }
}

void LogBDBError(const DbEnv*, const char* /*errpfx*/, const char* msg) {
  DINGO_LOG(ERROR) << "[bdb] error msg: " << msg;
}

void LogBDBMsg(const DbEnv*, const char* msg) { DINGO_LOG(INFO) << "[bdb] bdb info msg: " << msg; }

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

  if (FLAGS_bdb_db_pool_size < bthread::FLAGS_bthread_concurrency) {
    DINGO_LOG(WARNING) << fmt::format("[bdb] db pool size: {} < brpc_concurrency: {}, set db pool size to {}.",
                                      FLAGS_bdb_db_pool_size, bthread::FLAGS_bthread_concurrency,
                                      bthread::FLAGS_bthread_concurrency);
    FLAGS_bdb_db_pool_size = bthread::FLAGS_bthread_concurrency;
  }

  // Initialize our handles
  const char* file_name = "dingo.db";

  // Env open flags
  uint32_t env_flags =
      DB_CREATE |        // Create the environment if it does not exist
      DB_RECOVER |       // Run normal recovery.
      DB_INIT_LOCK |     // Initialize the locking subsystem
      DB_INIT_LOG |      // Initialize the logging subsystem
      DB_INIT_TXN |      // Initialize the transactional subsystem. This
                         // also turns on logging.
      DB_INIT_MPOOL |    // Initialize the memory pool (in-memory cache)
      DB_MULTIVERSION |  // Multiversion concurrency control
      //  DB_PRIVATE |  // Region files are not backed by the filesystem, but instead are backed by heap
      //                // memory. This flag is required for the in-memory cache.
      DB_THREAD;  // Cause the environment to be free-threaded
  try {
    // Create and open the environment
    envp_ = new DbEnv((uint32_t)0);

    // Indicate that we want db to internally perform deadlock
    // detection.  Also indicate that the transaction with
    // the fewest number of write locks will receive the
    // deadlock notification in the event of a deadlock.
    envp_->set_lk_detect(DB_LOCK_MINWRITE);

    if (FLAGS_bdb_lk_max_lockers > 0) {
      envp_->set_memory_init(DB_MEM_LOCKER, FLAGS_bdb_lk_max_lockers);
    }

    if (FLAGS_bdb_lk_max_locks > 0) {
      envp_->set_memory_init(DB_MEM_LOCK, FLAGS_bdb_lk_max_locks);
    }

    if (FLAGS_bdb_lk_max_objects > 0) {
      envp_->set_memory_init(DB_MEM_LOCKOBJECT, FLAGS_bdb_lk_max_objects);
    }

    if (FLAGS_bdb_mutex_max > 0) {
      envp_->mutex_set_max(FLAGS_bdb_mutex_max);
    }

    if (FLAGS_bdb_mutex_increment > 0) {
      envp_->mutex_set_increment(FLAGS_bdb_mutex_increment);
    }

    if (FLAGS_bdb_lk_partitions > 0) {
      envp_->set_lk_partitions(FLAGS_bdb_lk_partitions);
    }

    if (FLAGS_bdb_txn_memory_max > 0) {
      envp_->set_memory_init(DB_MEM_TRANSACTION, FLAGS_bdb_txn_memory_max);
    }

    if (FLAGS_bdb_env_cache_size_bytes > 0) {
      envp_->set_memory_max(0, FLAGS_bdb_env_cache_size_bytes);
    }

    // envp_->set_lk_max_lockers(FLAGS_bdb_lk_max_lockers);
    // envp_->set_lk_max_locks(FLAGS_bdb_lk_max_locks);
    // envp_->set_lk_max_objects(FLAGS_bdb_lk_max_objects);

    // envp_->set_cachesize(FLAGS_bdb_env_cache_size_gb, 0, 1);
    if (FLAGS_bdb_env_cache_size_gb > 0 || FLAGS_bdb_env_cache_size_bytes > 0) {
      envp_->set_cachesize(FLAGS_bdb_env_cache_size_gb, FLAGS_bdb_env_cache_size_bytes, 1);
    }

    // set error call back
    envp_->set_errcall(LogBDBError);
    envp_->set_msgcall(LogBDBMsg);

    // print txn_max
    uint32_t txn_max = 0;
    envp_->get_tx_max(&txn_max);

    DINGO_LOG(INFO) << fmt::format("[bdb] default txn_max: {}.", txn_max);

    if (FLAGS_bdb_txn_max > 0) {
      envp_->set_tx_max(FLAGS_bdb_txn_max);
    }
    envp_->get_tx_max(&txn_max);

    // set lock timeout to 5s, the first parameter is microsecond
    envp_->set_timeout(5 * 1000 * 1000, DB_SET_LOCK_TIMEOUT);

    DINGO_LOG(INFO) << fmt::format("[bdb] set txn_max to: {}.", txn_max);

    envp_->open((const char*)bdb_path.c_str(), env_flags, 0);

    // If we had utility threads (for running checkpoints or
    // deadlock detection, for example) we would spawn those
    // here.

    // Open the database
    // TODO: when set DB_REVSPLITOFF flag, store process will meet txn_begin cannot allocate memory erro in the second
    // round sdk integration test.
    int ret = OpenDb(&db_, file_name, envp_, 0);
    if (ret < 0) {
      DINGO_LOG(ERROR) << fmt::format("[bdb] error opening database: {}/{}, ret: {}.", bdb_path, file_name, ret);
      return false;
    }

    if (FLAGS_bdb_use_db_pool) {
      for (int i = 0; i < FLAGS_bdb_db_pool_size; i++) {
        Db* db_in_pool = nullptr;
        ret = OpenDb(&db_in_pool, file_name, envp_, 0);
        if (ret < 0) {
          DINGO_LOG(ERROR) << fmt::format("[bdb] error opening database: {}/{}, ret: {}.", bdb_path, file_name, ret);
          return false;
        }

        db_pool_.Put(db_in_pool);
        db_handles_.emplace_back(db_in_pool);
      }
    }

    DINGO_LOG(INFO) << fmt::format("[bdb] db pool init OK, size: {}.", FLAGS_bdb_db_pool_size);

  } catch (DbException& db_exctption) {
    DINGO_LOG(FATAL) << fmt::format("[bdb] error opening database environment: {}, exception: {} {}.", db_path_,
                                    db_exctption.get_errno(), db_exctption.what());
    return false;
  }

  db_path_ = bdb_path + "/" + file_name;

  reader_ = std::make_shared<bdb::Reader>(GetSelfPtr());
  writer_ = std::make_shared<bdb::Writer>(GetSelfPtr());
  DINGO_LOG(INFO) << fmt::format("[bdb] db path: {}", db_path_);

  std::thread checkpoint_thread([this]() {
    pthread_setname_np(pthread_self(), "bdb_chkpt");
    bdb::BdbHelper::CheckpointThread(envp_, db_, is_close_);
  });
  checkpoint_thread.detach();

  DINGO_LOG(INFO) << fmt::format("[bdb] db path: {}", db_path_);

  return true;
}

void BdbRawEngine::Close() {
  try {
    is_close_.store(true);

    // Close our database handle if it was opened.
    if (db_ != nullptr) {
      db_->close(0);
    }
    DINGO_LOG(INFO) << "[bdb] db closed.";

    // Close all db in db_handles_
    for (auto& db : db_handles_) {
      if (db != nullptr) {
        db->close(0);
      }
    }
    DINGO_LOG(INFO) << "[bdb] db handles closed.";

    // Close our environment if it was opened.
    if (envp_ != nullptr) {
      envp_->close(0);
      delete envp_;
      envp_ = nullptr;
    }

  } catch (DbException& db_exception) {
    bdb::BdbHelper::PrintEnvStat(envp_);
    DINGO_LOG(ERROR) << fmt::format("[bdb] error closing database and environment, exception: {} {}.",
                                    db_exception.get_errno(), db_exception.what());
  }

  DINGO_LOG(INFO) << "[bdb] I'm all done.";
}

std::string BdbRawEngine::GetName() { return pb::common::RawEngine_Name(pb::common::RAW_ENG_BDB); }

pb::common::RawEngine BdbRawEngine::GetRawEngineType() { return pb::common::RAW_ENG_BDB; }

dingodb::SnapshotPtr BdbRawEngine::GetSnapshot() {
  bool retry = true;
  int32_t retry_count = 0;

  Db* db = GetDb();
  bool db_need_put_back = true;
  DEFER(if (db_need_put_back) { PutDb(db); });

  DbEnv* envp = db->get_env();

  while (retry) {
    try {
      DbTxn* txn = nullptr;
      int ret = envp->txn_begin(nullptr, &txn, DB_TXN_SNAPSHOT);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn begin failed ret: {}.", ret);
        return nullptr;
      }

      bdb_transaction_alive_count << 1;

      // It seems bdb do a snapshot after a cursor with the txn do its first get.
      // So we need to init a cursor and do a get to make sure the snapshot is valid.
      // Acquire a cursor
      Dbc* cursorp = nullptr;
      ret = db->cursor(txn, &cursorp, DB_CURSOR_BULK | DB_TXN_SNAPSHOT);
      if (ret != 0) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] cursor failed ret: {}.", ret);
        return nullptr;
      }

      Dbt bdb_key, bdb_value;
      ret = cursorp->get(&bdb_key, &bdb_value, DB_FIRST);
      if (ret != 0 && ret != DB_NOTFOUND) {
        DINGO_LOG(ERROR) << fmt::format("[bdb] txn cursort get DB_NEXT failed ret: {}.", ret);
        return nullptr;
      }

      cursorp->close();
      cursorp = nullptr;

      db_need_put_back = false;
      return std::make_shared<bdb::BdbSnapshot>(db, txn, cursorp, &db_pool_);

    } catch (DbDeadlockException&) {
      DINGO_LOG(WARNING) << fmt::format(
          "[bdb] get snapshot got DB_LOCK_DEADLOCK. retrying write operation, retry_count: {}.", retry_count);
      retry_count++;
      bdb::DelayBeforeNextRetry(retry_count);
      retry = true;
    } catch (DbException& db_exception) {
      bdb::BdbHelper::PrintEnvStat(GetEnv());
      DINGO_LOG(ERROR) << fmt::format("[bdb] db txn_begin failed, exception: {} {}.", db_exception.get_errno(),
                                      db_exception.what());
      return nullptr;
    } catch (std::exception& std_exception) {
      DINGO_LOG(FATAL) << fmt::format("[bdb] std exception, {}.", std_exception.what());
    }
  }

  DINGO_LOG(ERROR) << "unknown error.";
  return nullptr;
}

RawEngine::CheckpointPtr BdbRawEngine::NewCheckpoint() {
  DINGO_LOG(FATAL) << "BdbRawEngine not support checkpoint.";
  return nullptr;
}

butil::Status BdbRawEngine::MergeCheckpointFiles(const std::string& /*path*/,
                                                 const pb::common::Range& range,               // NOLINT
                                                 const std::vector<std::string>& cf_names,     // NOLINT
                                                 std::vector<std::string>& merge_sst_paths) {  // NOLINT
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
      // DINGO_LOG(DEBUG) << fmt::format("[bdb] ingesting cf_name: {}, sst file name: {},
      // KV: {} | {}.", cf_name,
      //                                 file_name, iter->key().ToStringView(),
      //                                 iter->value().ToStringView());
      pb::common::KeyValue kv;
      kv.set_key(iter->key().data(), iter->key().size());
      kv.set_value(iter->value().data(), iter->value().size());

      kvs.emplace_back(kv);

      if (kvs.size() >= FLAGS_bdb_ingest_external_file_batch_put_count) {
        butil::Status s = writer_->KvBatchPutAndDelete(cf_name, kvs, {});
        if (BAIDU_UNLIKELY(!s.ok())) {
          DINGO_LOG(ERROR) << fmt::format(
              "[bdb] batch put failed, cf_name: {}, sst file name: {}, status code: {}, "
              "message: {}",
              cf_name, file_name, s.error_code(), s.error_str());
          return butil::Status(pb::error::EINTERNAL, "Internal ingest external file error.");
        }

        kvs.clear();
      }
    }

    if (!kvs.empty()) {
      butil::Status s = writer_->KvBatchPutAndDelete(cf_name, kvs, {});
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format(
            "[bdb] batch put failed, cf_name: {}, sst file name: {}, status code: {}, "
            "message: {}",
            cf_name, file_name, s.error_code(), s.error_str());
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

    envp_->stat_print(DB_STAT_SUBSYSTEM);
  } catch (DbException& db_exception) {
    bdb::BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] error flushing, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
  }
}

butil::Status BdbRawEngine::Compact(const std::string& cf_name) {
  std::string encoded_cf_prefix_lower_bound = bdb::BdbHelper::EncodeCf(cf_name);
  std::string encoded_cf_prefix_upper_bound = bdb::BdbHelper::EncodedCfUpperBound(cf_name);

  Dbt start, stop;
  bdb::BdbHelper::StringToDbt(encoded_cf_prefix_lower_bound, start);
  bdb::BdbHelper::StringToDbt(encoded_cf_prefix_upper_bound, stop);

  try {
    DB_COMPACT compact_data;
    memset(&compact_data, 0, sizeof(DB_COMPACT));
    compact_data.compact_fillpercent = 80;

    int ret = db_->compact(nullptr, &start, &stop, &compact_data, 0, nullptr);
    if (ret == 0) {
      DINGO_LOG(INFO) << fmt::format(
          "[bdb] compact done! number of pages freed: {}, number of pages examine: {}, "
          "number of levels removed: "
          "{}, "
          "number of deadlocks: {}, pages truncated to OS: {}, page number for "
          "truncation: {}",
          compact_data.compact_pages_free, compact_data.compact_pages_examine, compact_data.compact_levels,
          compact_data.compact_deadlock, compact_data.compact_pages_truncated, compact_data.compact_truncate);
      return butil::Status();
    }

    DINGO_LOG(ERROR) << fmt::format("[bdb] compact failed, ret: {}.", ret);
  } catch (DbException& db_exception) {
    bdb::BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] error compacting, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
  }

  return butil::Status(pb::error::EINTERNAL, "Internal compact error.");
}

// TODO: using DB_FAST_STAT to get approximate size is not accurate and very limited for btree.
//       we need to implement our own approximate size function in the future.
//       now using GetRangeKeyValueSize to get approximate size, this is very slow for large range.
//       we skip GetApproximateSizes in metrics collect crontab for regions has lesser raft apply count than 10, so the
//       performace penalty is acceptable.
std::vector<int64_t> BdbRawEngine::GetApproximateSizes(const std::string& cf_name,
                                                       std::vector<pb::common::Range>& ranges) {
  std::vector<int64_t> result_sizes;

  std::shared_ptr<bdb::Reader> bdb_reader = std::dynamic_pointer_cast<bdb::Reader>(reader_);
  if (bdb_reader == nullptr) {
    DINGO_LOG(ERROR) << "[bdb] reader pointer cast error.";
    return result_sizes;
  }

#ifndef BDB_FAST_GET_APROX_SIZE

  for (const auto& range : ranges) {
    // Calculate the size of keys and values for each range
    // we use the first 100 keys to calculate the average size of keys and values
    int64_t key_size = 0;
    int64_t value_size = 0;
    auto ret =
        bdb_reader->GetRangeKeyValueSize(cf_name, range.start_key(), range.end_key(), INT64_MAX, key_size, value_size);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[bdb] get range key value size failed, cf_name: {}, status code: {}, message: {}", cf_name, ret.error_code(),
          ret.error_str());
    }

    DINGO_LOG(DEBUG) << fmt::format("[bdb] cf_name: {}, key_size: {}, value_size: {}.", cf_name, key_size, value_size);

    result_sizes.push_back(key_size + value_size);
  }

#else
  // DB_FAST_STAT is very limited for btree, it is based on the last metrics the database has full stat.

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
      bdb::BdbHelper::StringToDbt(store_start_key, bdb_start_key);

      std::string store_end_key = bdb::BdbHelper::EncodeKey(cf_name, range.end_key());
      bdb::BdbHelper::StringToDbt(store_end_key, bdb_end_key);

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

      if (count >= 0) {
        DINGO_LOG(INFO) << fmt::format(
            "[bdb] cf_name: {}, count: {}, bdb total key count: {}, range_result: {}, "
            "range_start.less: {}, "
            "range_end.equal: "
            "{}, range_end.greater: {}.",
            cf_name, count, bdb_total_key_count, range_result, range_start.less, range_end.equal, range_end.greater);
      }

      // Calculate the size of keys and values for each range
      // we use the first 100 keys to calculate the average size of keys and values
      int64_t key_size = 0;
      int64_t value_size = 0;
      int64_t limit_count = 100;
      auto ret1 = bdb_reader->GetRangeKeyValueSize(cf_name, range.start_key(), range.end_key(), limit_count, key_size,
                                                   value_size);
      if (!ret1.ok()) {
        DINGO_LOG(ERROR) << fmt::format(
            "[bdb] get range key value size failed, cf_name: {}, status code: {}, message: {}", cf_name,
            ret1.error_code(), ret1.error_str());
      }

      result_sizes.push_back(count * (key_size + value_size) / limit_count);
      DINGO_LOG(INFO) << fmt::format("[bdb] cf_name: {}, count: {}, key_size: {}, value_size: {}.", cf_name, count,
                                     key_size, value_size);
    }
  } catch (DbException& db_exception) {
    bdb::BdbHelper::PrintEnvStat(GetEnv());
    DINGO_LOG(ERROR) << fmt::format("[bdb] error get approximate sizes, exception: {} {}.", db_exception.get_errno(),
                                    db_exception.what());
    return std::vector<int64_t>();
  }

#endif

  return result_sizes;
}

}  // namespace dingodb
