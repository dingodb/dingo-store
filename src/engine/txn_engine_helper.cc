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

#include "engine/txn_engine_helper.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "bvar/latency_recorder.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/stream.h"
#include "coprocessor/coprocessor_v2.h"
#include "document/codec.h"
#include "engine/gc_safe_point.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "vector/codec.h"

namespace dingodb {

DEFINE_int64(max_short_value_in_write_cf, 256, "max short value in write cf");
DEFINE_int64(max_batch_get_count, 4096, "max batch get count");
DEFINE_int64(max_batch_get_memory_size, 60 * 1024 * 1024, "max batch get memory size");
DEFINE_int64(max_prewrite_count, 4096, "max prewrite count");
DEFINE_int64(max_commit_count, 4096, "max commit count");
DEFINE_int64(max_rollback_count, 4096, "max rollback count");
DEFINE_int64(max_resolve_count, 4096, "max rollback count");
DEFINE_int64(max_pessimistic_count, 4096, "max pessimistic count");
DEFINE_int64(gc_delete_batch_count, 32768, "gc delete batch count");

DEFINE_bool(dingo_log_switch_txn_detail, false, "txn detail log");
DEFINE_bool(dingo_log_switch_txn_gc_detail, false, "txn gc detail log");

DECLARE_int64(stream_message_max_bytes);
DECLARE_int64(stream_message_max_limit_size);

butil::Status TxnReader::Init() {
  if (is_initialized_) {
    return butil::Status::OK();
  }

  if (snapshot_ == nullptr) {
    snapshot_ = raw_engine_->GetSnapshot();
    if (snapshot_ == nullptr) {
      DINGO_LOG(ERROR) << "[txn]Scan GetSnapshot failed";
      return butil::Status(pb::error::Errno::EINTERNAL, "get snapshot failed");
    }
  }

  if (reader_ == nullptr) {
    reader_ = raw_engine_->Reader();
    if (reader_ == nullptr) {
      DINGO_LOG(ERROR) << "[txn]Scan Reader failed";
      return butil::Status(pb::error::Errno::EINTERNAL, "get reader failed");
    }
  }

  // construct write iter
  IteratorOptions write_iter_options;
  write_iter_ = reader_->NewIterator(Constant::kTxnWriteCF, snapshot_, write_iter_options);
  if (write_iter_ == nullptr) {
    DINGO_LOG(ERROR) << "[txn]Scan NewIterator write failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "new iterator failed");
  }

  is_initialized_ = true;

  return butil::Status::OK();
}

butil::Status TxnReader::GetLockInfo(const std::string &key, pb::store::LockInfo &lock_info) {
  if (!is_initialized_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "txn reader is not initialized");
  }

  std::string lock_value;
  auto status =
      reader_->KvGet(Constant::kTxnLockCF, snapshot_, mvcc::Codec::EncodeKey(key, Constant::kLockVer), lock_value);
  // if lock_value is not found or it is empty, then the key is not locked
  // else the key is locked, return WriteConflict
  if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // key is not exists, the key is not locked
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetLockInfo key: " << Helper::StringToHex(key) << " is not locked, lock_key is not exist";
    return butil::Status::OK();
  }

  if (!status.ok()) {
    // other error, return error
    DINGO_LOG(ERROR) << "[txn]GetLockInfo read lock_key failed, lock_key: " << Helper::StringToHex(key)
                     << ", status: " << status.error_str();
    return butil::Status(status.error_code(), status.error_str());
  }

  if (lock_value.empty()) {
    // lock_value is empty, the key is not locked
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetLockInfo key: " << Helper::StringToHex(key) << " is not locked, lock_value is null";
    return butil::Status::OK();
  }

  auto ret = lock_info.ParseFromString(lock_value);
  if (!ret) {
    DINGO_LOG(FATAL) << "[txn]GetLockInfo parse lock info failed, lock_key: " << Helper::StringToHex(key)
                     << ", lock_value: " << Helper::StringToHex(lock_value);
  }

  return butil::Status::OK();
}

butil::Status TxnReader::GetDataValue(const std::string &key, std::string &value) {
  if (!is_initialized_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "txn reader is not initialized");
  }

  // data_iter_->Seek(key);
  // if (!data_iter_->Valid() || data_iter_->Key() != key) {
  //   // key is not exists, the key is not locked
  //   DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
  //       << "[txn]GetDataValue key: " << Helper::StringToHex(key) << " key is not exist";
  //   return butil::Status(pb::error::Errno::EKEY_NOT_FOUND, "key is not exist");
  // }

  // value = data_iter_->Value();

  auto ret = reader_->KvGet(Constant::kTxnDataCF, snapshot_, key, value);
  if (ret.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // key is not exists, the key is not locked
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetDataValue key: " << Helper::StringToHex(key) << " key is not exist";
    return butil::Status(pb::error::Errno::EKEY_NOT_FOUND, "key is not exist");
  }

  if (value.empty()) {
    // value is empty, the key is not locked
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetDataValue key: " << Helper::StringToHex(key) << " value is null";
    return butil::Status::OK();
  }

  return butil::Status::OK();
}

butil::Status TxnReader::GetWriteInfo(int64_t min_commit_ts, int64_t max_commit_ts, int64_t start_ts,
                                      const std::string &key, bool include_rollback, bool include_delete,
                                      bool include_put, pb::store::WriteInfo &write_info, int64_t &commit_ts) {
  if (!is_initialized_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "txn reader is not initialized");
  }

  IteratorOptions iter_options;
  iter_options.lower_bound = mvcc::Codec::EncodeKey(key, max_commit_ts);
  iter_options.upper_bound = mvcc::Codec::EncodeKey(key, min_commit_ts);

  // if the key is committed after start_ts, return WriteConflict
  pb::store::WriteInfo tmp_write_info;
  write_iter_->Seek(iter_options.lower_bound);
  while (write_iter_->Valid() && write_iter_->Key() < iter_options.upper_bound) {
    if (write_iter_->Key().length() <= 8) {
      DINGO_LOG(ERROR) << "invalid write_key, key: " << Helper::StringToHex(write_iter_->Key())
                       << ", write_key is less than 8 bytes: " << Helper::StringToHex(write_iter_->Key());
      return butil::Status(pb::error::Errno::EINTERNAL, "invalid write_key");
    }

    std::string write_key;
    int64_t write_ts;
    mvcc::Codec::DecodeKey(write_iter_->Key(), write_key, write_ts);

    if (write_ts < min_commit_ts) {
      break;
    }

    auto ret = tmp_write_info.ParseFromArray(write_iter_->Value().data(), write_iter_->Value().size());
    if (!ret) {
      DINGO_LOG(ERROR) << "cannot parse tmp_write_info, key: " << Helper::StringToHex(write_iter_->Key())
                       << ", write_ts: " << write_ts << ", write_key: " << Helper::StringToHex(write_iter_->Key())
                       << ", write_value(hex): " << Helper::StringToHex(write_iter_->Value());
      return butil::Status(pb::error::Errno::EINTERNAL, "cannot parse tmp_write_info");
    }

    if (start_ts > 0) {
      if (tmp_write_info.start_ts() != start_ts) {
        write_iter_->Next();
        continue;
      }
    }

    if (tmp_write_info.op() == pb::store::Op::Rollback) {
      if (!include_rollback) {
        write_iter_->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Delete) {
      if (!include_delete) {
        write_iter_->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Put) {
      if (!include_put) {
        write_iter_->Next();
        continue;
      }
    } else {
      DINGO_LOG(ERROR) << "invalid write op, key: " << Helper::StringToHex(write_iter_->Key())
                       << ", write_ts: " << write_ts << ", write_key: " << Helper::StringToHex(write_iter_->Key())
                       << ", write_value(hex): " << Helper::StringToHex(write_iter_->Value());
      write_iter_->Next();
      continue;
    }

    write_info = tmp_write_info;
    commit_ts = write_ts;
    break;
  }

  return butil::Status::OK();
}

butil::Status TxnReader::GetRollbackInfo(int64_t start_ts, const std::string &key, pb::store::WriteInfo &write_info) {
  if (!is_initialized_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "txn reader is not initialized");
  }

  std::string write_value;
  auto ret = reader_->KvGet(Constant::kTxnWriteCF, snapshot_, mvcc::Codec::EncodeKey(key, start_ts), write_value);
  if (ret.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // no rollback
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "not find a rollback write line, key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts;

    return butil::Status::OK();
  }

  if (write_value.empty()) {
    DINGO_LOG(ERROR) << "find a rollback write line, but write_value is empty, key: " << Helper::StringToHex(key)
                     << ", start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "write_value is empty");
  }

  pb::store::WriteInfo tmp_write_info;
  auto ret1 = tmp_write_info.ParseFromArray(write_value.data(), write_value.size());
  if (!ret1) {
    DINGO_LOG(ERROR) << "parse write info failed, key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts
                     << ", tmp_write_info: " << tmp_write_info.ShortDebugString()
                     << ", write_value: " << Helper::StringToHex(write_value);
    return butil::Status(pb::error::Errno::EINTERNAL, "parse write info failed");
  }

  if (tmp_write_info.start_ts() != start_ts) {
    return butil::Status::OK();
  }

  if (tmp_write_info.op() == pb::store::Op::Rollback) {
    if (tmp_write_info.start_ts() == start_ts) {
      write_info = tmp_write_info;
      return butil::Status::OK();
    } else {
      DINGO_LOG(ERROR)
          << "find a rollback write line, but not the same start_ts, there my be FATAL error, write_info : "
          << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts;
    }
  } else {
    DINGO_LOG(ERROR) << "find a rollback write line, but not rollback op, there may be FATAL error, write_info: "
                     << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key)
                     << ", start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "find a rollback write line, but not rollback op");
  }

  return butil::Status::OK();
}

butil::Status TxnReader::GetOldValue(const std::string &key, int64_t start_ts, bool prev_write_load,
                                     pb::store::WriteInfo &prev_write_info, std::vector<pb::common::KeyValue> &kvs) {
  pb::common::KeyValue kv;
  kv.set_key(key);
  if (prev_write_load) {
    if (prev_write_info.op() == pb::store::Op::Put) {
      if (!prev_write_info.short_value().empty()) {
        kv.set_value(prev_write_info.short_value());
        kvs.emplace_back(kv);
        return butil::Status::OK();
      }
      auto ret1 = GetDataValue(mvcc::Codec::EncodeKey(key, prev_write_info.start_ts()), *kv.mutable_value());
      if (!ret1.ok() && ret1.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << "[txn]GetOldValue read data failed, key: " << Helper::StringToHex(key)
                         << ", status: " << ret1.error_str();
      } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(INFO) << "[txn]GetOldValue read data failed, data is illegally not found, key: "
                        << Helper::StringToHex(key) << ", status: " << ret1.error_str()
                        << ", ts: " << prev_write_info.start_ts();
        kv.set_value(std::string());
      }
    } else {
      DINGO_LOG(INFO) << "[txn]GetOldValue prev_write op: " << prev_write_info.op()
                      << ", value should be empty, key: " << Helper::StringToHex(key)
                      << ", ts: " << prev_write_info.start_ts();
    }
  } else {
    pb::store::WriteInfo write_info;
    int64_t commit_ts = 0;
    auto ret2 = GetWriteInfo(0, start_ts, 0, key, false, true, true, write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(ERROR) << "[txn]GetOldValue get write info failed, key: " << Helper::StringToHex(key)
                       << ", start_ts: " << start_ts << ", status: " << ret2.error_str();
      return ret2;
    }
    if (write_info.op() == pb::store::Op::Put) {
      if (!write_info.short_value().empty()) {
        kv.set_value(write_info.short_value());
        kvs.emplace_back(kv);
        return butil::Status::OK();
      }
      auto ret3 = GetDataValue(mvcc::Codec::EncodeKey(key, write_info.start_ts()), *kv.mutable_value());
      if (!ret3.ok() && ret3.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << "[txn]GetOldValue read data failed, key: " << Helper::StringToHex(key)
                         << ", status: " << ret3.error_str();
      } else if (ret3.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(INFO) << "[txn]GetOldValue read data failed, data is illegally not found, key: "
                        << Helper::StringToHex(key) << ", status: " << ret3.error_str()
                        << ", ts: " << write_info.start_ts();
        kv.set_value(std::string());
      }
    } else {
      DINGO_LOG(INFO) << "[txn]GetOldValue read data failed, data is illegally not found, key: "
                      << Helper::StringToHex(key) << ", ts: " << write_info.start_ts();
    }
  }
  kvs.emplace_back(kv);
  return butil::Status::OK();
}

butil::Status TxnIterator::Init() {
  snapshot_ = raw_engine_->GetSnapshot();
  if (snapshot_ == nullptr) {
    DINGO_LOG(ERROR) << "[txn]Scan GetSnapshot failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "get snapshot failed");
  }
  reader_ = raw_engine_->Reader();
  if (reader_ == nullptr) {
    DINGO_LOG(ERROR) << "[txn]Scan Reader failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "get reader failed");
  }

  // construct write iter
  IteratorOptions write_iter_options;
  write_iter_options.lower_bound = mvcc::Codec::EncodeKey(range_.start_key(), Constant::kMaxVer);
  write_iter_options.upper_bound = mvcc::Codec::EncodeKey(range_.end_key(), Constant::kMaxVer);

  write_iter_ = reader_->NewIterator(Constant::kTxnWriteCF, snapshot_, write_iter_options);
  if (write_iter_ == nullptr) {
    DINGO_LOG(ERROR) << "[txn]Scan NewIterator write failed, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    return butil::Status(pb::error::Errno::EINTERNAL, "new iterator failed");
  }

  // construct lock iter
  IteratorOptions lock_iter_options;
  lock_iter_options.lower_bound = mvcc::Codec::EncodeKey(range_.start_key(), Constant::kLockVer);
  lock_iter_options.upper_bound = mvcc::Codec::EncodeKey(range_.end_key(), Constant::kLockVer);

  lock_iter_ = reader_->NewIterator(Constant::kTxnLockCF, snapshot_, lock_iter_options);
  if (lock_iter_ == nullptr) {
    DINGO_LOG(ERROR) << "[txn]Scan NewIterator lock failed, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    return butil::Status(pb::error::Errno::EINTERNAL, "new iterator failed");
  }

  // iter write and lock iter, if lock_ts < start_ts, return LockInfo
  write_iter_->Seek(write_iter_options.lower_bound);
  lock_iter_->Seek(lock_iter_options.lower_bound);

  if ((!write_iter_->Valid()) && (!lock_iter_->Valid())) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]write_iter is not valid and lock_iter is not valid, start_ts: " << start_ts_
        << ", seek_ts: " << seek_ts_ << ", write_iter->Valid(): " << write_iter_->Valid()
        << ", lock_iter->Valid(): " << lock_iter_->Valid();
  }

  return butil::Status::OK();
}

butil::Status TxnIterator::Seek(const std::string &key) {
  auto ret = InnerSeek(key);
  if (!ret.ok()) {
    if (ret.error_code() == pb::error::Errno::ETXN_LOCK_CONFLICT) {
      DINGO_LOG(INFO) << "[txn]InnerSeek failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    } else {
      DINGO_LOG(ERROR) << "[txn]InnerSeek failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    }
    return ret;
  }

  while (value_.empty()) {
    ret = InnerNext();
    if (ret.error_code() == pb::error::Errno::ETXN_SCAN_FINISH) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]InnerNext stopped, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
      return butil::Status::OK();
    } else if (!ret.ok()) {
      DINGO_LOG(ERROR) << "[txn]InnerNext failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
      return ret;
    }

    if (!value_.empty()) {
      return butil::Status::OK();
    } else {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]InnerNext value is empty, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
          << ", key_: " << Helper::StringToHex(key_);
      continue;
    }
  }

  return butil::Status::OK();
}

butil::Status TxnIterator::InnerSeek(const std::string &key) {
  key_.clear();
  value_.clear();
  last_lock_key_.clear();
  last_write_key_.clear();

  lock_iter_->Seek(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
  int64_t lock_ts = 0;
  if (lock_iter_->Valid()) {
    auto ret = mvcc::Codec::DecodeKey(lock_iter_->Key(), last_lock_key_, lock_ts);
    if (!ret) {
      DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, lock_iter->key: " << Helper::StringToHex(lock_iter_->Key())
                       << ", start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    }
  } else {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan lock_iter is invalid, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
        << ", last_lock_key: " << Helper::StringToHex(last_lock_key_);
  }

  write_iter_->Seek(mvcc::Codec::EncodeKey(key, seek_ts_));
  int64_t write_ts = 0;
  if (write_iter_->Valid()) {
    auto ret = mvcc::Codec::DecodeKey(write_iter_->Key(), last_write_key_, write_ts);
    if (!ret) {
      DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, write_iter->key: "
                       << Helper::StringToHex(write_iter_->Key()) << ", start_ts: " << start_ts_
                       << ", seek_ts: " << seek_ts_;
    }
  } else {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan write_iter is invalid, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
        << ", last_write_key: " << Helper::StringToHex(last_write_key_);
  }

  if (last_lock_key_.empty() && last_write_key_.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan last_lock_key_ and last_write_key_ are empty, start_ts: " << start_ts_
        << ", seek_ts: " << seek_ts_ << ", key: " << Helper::StringToHex(key);
    return butil::Status::OK();
  }

  auto ret = GetCurrentValue();
  if (ret.ok()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetCurrentValue OK, key_: " << Helper::StringToHex(key_) << ", value_: " << Helper::StringToHex(value_)
        << ", start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    return butil::Status::OK();
  } else if (ret.error_code() == pb::error::Errno::ETXN_LOCK_CONFLICT) {
    DINGO_LOG(INFO) << "[txn]GetCurrentValue failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    return ret;
  } else {
    DINGO_LOG(ERROR) << "[txn]GetCurrentValue failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    return ret;
  }
}

butil::Status TxnIterator::Next() {
  butil::Status ret;

  while (ret.ok()) {
    ret = InnerNext();
    if (ret.error_code() == pb::error::Errno::ETXN_SCAN_FINISH) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]InnerNext stopped, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
      return butil::Status::OK();
    } else if (!ret.ok()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]InnerNext stopped, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
      return ret;
    }

    if (!value_.empty()) {
      return butil::Status::OK();
    } else {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]InnerNext value is empty, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
          << ", key_: " << Helper::StringToHex(key_);
      continue;
    }
  }

  return ret;
}

butil::Status TxnIterator::InnerNext() {
  if (key_.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan Next key_ is empty, scan is finished, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    return butil::Status(pb::error::Errno::ETXN_SCAN_FINISH, "key_ is empty");
  }

  if (txn_result_info_.ByteSizeLong() > 0) {
    DINGO_LOG(ERROR) << "[txn]Scan Next txn_result_info_ is not empty, start_ts: " << start_ts_
                     << ", seek_ts: " << seek_ts_;
    return butil::Status(pb::error::Errno::ETXN_RESULT_INFO_NOT_NULL, "key_ is empty");
  }

  value_.clear();

  if (lock_iter_->Valid() && key_ >= last_lock_key_) {
    while (lock_iter_->Valid()) {
      lock_iter_->Next();
      int64_t lock_ts = 0;
      if (lock_iter_->Valid()) {
        auto ret = mvcc::Codec::DecodeKey(lock_iter_->Key(), last_lock_key_, lock_ts);
        if (!ret) {
          DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, lock_iter->key: "
                           << Helper::StringToHex(lock_iter_->Key()) << ", start_ts: " << start_ts_
                           << ", seek_ts: " << seek_ts_;
        }
        if (last_lock_key_ > key_) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]Scan last_lock_key_ > key_, find next key, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
              << ", last_lock_key: " << Helper::StringToHex(last_lock_key_) << ", key_: " << Helper::StringToHex(key_);
          break;
        }
      } else {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "[txn]Scan lock_iter is invalid, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
            << ", last_lock_key: " << Helper::StringToHex(last_lock_key_) << ", will set last_lock_key to empty";
        last_lock_key_ = std::string();
      }
    }
  }

  if (key_ >= last_write_key_ && write_iter_->Valid()) {
    DINGO_LOG(FATAL) << "[txn]Scan write_iter is valid, start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_
                     << ", last_write_key: " << Helper::StringToHex(last_write_key_);
  }
  // // CAUTION: we do writer_iter_->Next() in GetCurrentValue(), so we don't need to do writer_iter_->Next() here
  // int64_t write_ts = 0;
  // if (write_iter_->Valid()) {
  //   auto ret = mvcc::Codec::DecodeKey(write_iter_->Key(), last_write_key_, write_ts);
  //   if (!ret) {
  //     DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, write_iter->key: "
  //                      << Helper::StringToHex(write_iter_->Key()) << ", start_ts: " << start_ts_
  //                      << ", seek_ts: " << seek_ts_;
  //   }
  //   CHECK(last_write_key_ > key_);
  // } else {
  //   DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << "[txn]Scan write_iter is invalid, start_ts: " <<
  //   start_ts_ << ", seek_ts: " << seek_ts_
  //                   << ", last_write_key: " << Helper::StringToHex(last_write_key_);
  // }

  if (last_lock_key_.empty() && last_write_key_.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan last_lock_key_ and last_write_key_ are empty, start_ts: " << start_ts_
        << ", seek_ts: " << seek_ts_ << ", key_: " << Helper::StringToHex(key_);

    if (!lock_iter_->Valid() && !write_iter_->Valid()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]Scan lock_iter_ and write_iter_ are invalid, the iterator is finished, start_ts: " << start_ts_
          << ", seek_ts: " << seek_ts_ << ", key_: " << Helper::StringToHex(key_);
      key_.clear();
      return butil::Status::OK();
    }

    return butil::Status::OK();
  }

  if (last_lock_key_ <= key_ && last_write_key_ <= key_) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]Scan last_lock_key_ <= key_ && last_write_key_ <= key_, no key found, start_ts: " << start_ts_
        << ", seek_ts: " << seek_ts_ << ", last_lock_key: " << Helper::StringToHex(last_lock_key_)
        << ", last_write_key: " << Helper::StringToHex(last_write_key_) << ", key_: " << Helper::StringToHex(key_);
    key_.clear();
    return butil::Status::OK();
  }

  auto ret = GetCurrentValue();
  if (ret.ok()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]GetCurrentValue OK, key_: " << Helper::StringToHex(key_) << ", value_: " << Helper::StringToHex(value_)
        << ", start_ts: " << start_ts_ << ", seek_ts: " << seek_ts_;
    return butil::Status::OK();
  } else if (ret.error_code() == pb::error::Errno::ETXN_LOCK_CONFLICT) {
    DINGO_LOG(INFO) << "[txn]GetCurrentValue failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    return ret;
  } else {
    DINGO_LOG(ERROR) << "[txn]GetCurrentValue failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    return ret;
  }
}

butil::Status TxnIterator::GotoNextUserKeyInWriteIter(std::shared_ptr<Iterator> write_iter, std::string prev_user_key,
                                                      std::string &last_write_key) {
  while (write_iter->Valid()) {
    write_iter->Next();
    if (write_iter->Valid()) {
      int64_t commit_ts;
      auto ret = mvcc::Codec::DecodeKey(write_iter->Key(), last_write_key, commit_ts);
      if (!ret) {
        DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, write_iter->key: "
                         << Helper::StringToHex(write_iter->Key());
      }

      if (last_write_key > prev_user_key) {
        break;
      }
    }
  }

  return butil::Status::OK();
}

std::string TxnIterator::GetUserKey(std::shared_ptr<Iterator> write_iter) {
  std::string user_key;
  if (write_iter->Valid()) {
    int64_t commit_ts;
    auto ret = mvcc::Codec::DecodeKey(write_iter->Key(), user_key, commit_ts);
    if (!ret) {
      DINGO_LOG(FATAL) << "[txn]GetUserKey failed, write_iter->key: " << Helper::StringToHex(write_iter->Key());
    }
  }

  return std::move(user_key);
}

butil::Status TxnIterator::GetUserValueInWriteIter(std::shared_ptr<Iterator> write_iter, RawEngine::ReaderPtr reader,
                                                   pb::store::IsolationLevel isolation_level, int64_t seek_ts,
                                                   int64_t start_ts, const std::string &user_key,
                                                   std::string &last_write_key, bool &is_value_found,
                                                   std::string &user_value) {
  is_value_found = false;
  while (write_iter->Valid()) {
    int64_t commit_ts;
    auto ret1 = mvcc::Codec::DecodeKey(write_iter->Key(), last_write_key, commit_ts);
    if (!ret1) {
      DINGO_LOG(FATAL) << "[txn]Scan decode txn key failed, write_iter->key: " << Helper::StringToHex(write_iter->Key())
                       << ", start_ts: " << start_ts << ", seek_ts: " << seek_ts;
    }

    if (last_write_key > user_key) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]Scan last_write_key > user_key, means no value, start_ts: " << start_ts << ", seek_ts: " << seek_ts
          << ", last_write_key: " << Helper::StringToHex(last_write_key)
          << ", user_key: " << Helper::StringToHex(user_key);
      return butil::Status::OK();
    }

    // check isolation_level
    if (isolation_level == pb::store::IsolationLevel::SnapshotIsolation) {
      if (commit_ts > start_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "[txn]Scan commit_ts > start_ts, means this value is not accepted, will go to next, start_ts: "
            << start_ts << ", commit_ts: " << commit_ts << ", user_key: " << Helper::StringToHex(user_key);
        write_iter->Next();

        // we need to setup is_value_found to true, so that the caller can go to next user_key
        // is_value_found means the value is found, not means the value is valid
        // the user_value is not valid, so we need to go to next user_key
        is_value_found = true;
        user_value = std::string();
        continue;
      }
    } else if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]Scan RC, commit_ts: " << commit_ts << ", start_ts: " << start_ts << ", seek_ts: " << seek_ts
          << ", user_key: " << Helper::StringToHex(user_key);
    } else {
      DINGO_LOG(ERROR) << "[txn]BatchGet invalid isolation_level: " << isolation_level;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid isolation_level");
    }

    pb::store::WriteInfo write_info;
    auto ret2 = write_info.ParseFromArray(write_iter->Value().data(), write_iter->Value().size());
    if (!ret2) {
      DINGO_LOG(FATAL) << "[txn]Scan parse write info failed, write_key: " << Helper::StringToHex(write_iter->Key())
                       << ", write_value(hex): " << Helper::StringToHex(write_iter->Value());
    }

    if (write_info.op() == pb::store::Op::Delete) {
      // if op is delete, value is null
      user_value = std::string();

      // before return, go to next user_key
      GotoNextUserKeyInWriteIter(write_iter, last_write_key, last_write_key);
      is_value_found = true;
      return butil::Status::OK();
    } else if (write_info.op() == pb::store::Op::Rollback) {
      // if op is rollback, go to next write
      write_iter->Next();

      // we need to setup is_value_found to true, so that the caller can go to next user_key
      // is_value_found means the value is found, not means the value is valid
      // the user_value is not valid, so we need to go to next user_key
      is_value_found = true;
      user_value = std::string();
    } else if (write_info.op() == pb::store::Op::Put) {
      // use write_ts to get data from data_cf
      if (!write_info.short_value().empty()) {
        user_value = write_info.short_value();

        // before return, go to next user_key
        GotoNextUserKeyInWriteIter(write_iter, last_write_key, last_write_key);
        is_value_found = true;
        return butil::Status::OK();
      } else {
        auto ret3 =
            reader->KvGet(Constant::kTxnDataCF, mvcc::Codec::EncodeKey(user_key, write_info.start_ts()), user_value);
        if (!ret3.ok() && ret3.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "[txn]Scan read data failed, key: " << Helper::StringToHex(user_key)
                           << ", status: " << ret3.error_str();
          return butil::Status(pb::error::Errno::EINTERNAL, "read data failed");
        } else if (ret3.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(ERROR) << "[txn]Scan read data failed, data is illegally not found, key: "
                           << Helper::StringToHex(user_key) << ", status: " << ret3.error_str()
                           << ", ts: " << write_info.start_ts();
          return butil::Status(pb::error::Errno::EINTERNAL, "data is illegally not found");
        } else {
          // before return, go to next user_key
          GotoNextUserKeyInWriteIter(write_iter, last_write_key, last_write_key);
          is_value_found = true;
          return ret3;
        }
      }
    } else {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]Scan write_iter meet illegal op, start_ts: " << start_ts << ", seek_ts: " << seek_ts
          << ", last_write_key: " << Helper::StringToHex(last_write_key)
          << ", tmp_key: " << Helper::StringToHex(user_key) << ", write_info.op: " << write_info.op();
      write_iter->Next();
    }
  }

  return butil::Status::OK();
}

butil::Status TxnIterator::GetCurrentValue() {
  bool check_lock_cf_first = false;

  CHECK(!(last_write_key_.empty() && last_lock_key_.empty()));

  if (last_write_key_.empty()) {
    check_lock_cf_first = true;
  } else if (!last_lock_key_.empty()) {
    if (last_lock_key_ <= last_write_key_) {
      check_lock_cf_first = true;
    } else if (!write_iter_->Valid()) {
      check_lock_cf_first = true;
    }
  }

  if (check_lock_cf_first) {
    key_ = last_lock_key_;

    // get lock info
    pb::store::LockInfo lock_info;
    auto lock_value = lock_iter_->Value();
    auto ret1 = lock_info.ParseFromArray(lock_value.data(), lock_value.size());
    if (!ret1) {
      DINGO_LOG(FATAL) << "[txn]Scan parse lock info failed, lock_key: " << Helper::StringToHex(lock_iter_->Key())
                       << ", lock_value(hex): " << Helper::StringToHex(lock_value);
    }

    auto is_lock_conflict =
        TxnEngineHelper::CheckLockConflict(lock_info, isolation_level_, start_ts_, resolved_locks_, txn_result_info_);
    if (is_lock_conflict) {
      DINGO_LOG(WARNING) << "[txn]Scan CheckLockConflict return conflict, key: " << Helper::StringToHex(lock_info.key())
                         << ", isolation_level: " << isolation_level_ << ", start_ts: " << start_ts_
                         << ", seek_ts: " << seek_ts_ << ", lock_info: " << lock_info.ShortDebugString();
      key_.clear();
      value_.clear();
      return butil::Status(pb::error::Errno::ETXN_LOCK_CONFLICT, "lock conflict");
    }

    // if lock_key == write_key, then we can get data from write_cf
    if (last_lock_key_ == last_write_key_) {
      bool is_value_found = false;
      GetUserValueInWriteIter(write_iter_, reader_, isolation_level_, seek_ts_, start_ts_, key_, last_write_key_,
                              is_value_found, value_);

      if (is_value_found) {
        return butil::Status::OK();
      } else {
        // no valid value is found, so the txn iterator will be invalid
        // clear the key_ to make the txn iterator invalid
        key_.clear();
        return butil::Status::OK();
      }
    } else {
      // lock_key < write_key, there is no data
      return butil::Status::OK();
    }
  } else {
    key_ = last_write_key_;

    bool is_value_found = false;
    GetUserValueInWriteIter(write_iter_, reader_, isolation_level_, seek_ts_, start_ts_, key_, last_write_key_,
                            is_value_found, value_);

    if (is_value_found) {
      return butil::Status::OK();
    } else {
      // no valid value is found, so the txn iterator will be invalid
      // clear the key_ to make the txn iterator invalid
      key_.clear();
      return butil::Status::OK();
    }
  }

  return butil::Status::OK();
}

bool TxnIterator::Valid(pb::store::TxnResultInfo &txn_result_info) {
  if (txn_result_info_.ByteSizeLong() > 0) {
    txn_result_info = txn_result_info_;
    return false;
  }

  if (key_.empty()) {
    return false;
  }

  return true;
}

std::string TxnIterator::Key() { return key_; }

std::string TxnIterator::Value() { return value_; }

bool TxnEngineHelper::CheckLockConflict(const pb::store::LockInfo &lock_info, pb::store::IsolationLevel isolation_level,
                                        int64_t start_ts, const std::set<int64_t> &resolved_locks,
                                        pb::store::TxnResultInfo &txn_result_info) {
  DINGO_LOG(DEBUG) << "[txn]CheckLockConflict lock_info: " << lock_info.ShortDebugString()
                   << ", isolation_level: " << isolation_level << ", start_ts: " << start_ts
                   << ", resolved_locks size: " << resolved_locks.size();

  // if lock_info is resolved, return false, means the executor has used CheckTxnStatus to check the lock_info and
  // updated the min_commit_ts of the primary lock.
  if (!resolved_locks.empty() && resolved_locks.find(lock_info.lock_ts()) != resolved_locks.end()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "[txn]CheckLockConflict lock_info: " << lock_info.ShortDebugString()
        << ", isolation_level: " << isolation_level << ", start_ts: " << start_ts
        << ", resolved_locks size: " << resolved_locks.size() << ", lock_ts: " << lock_info.lock_ts()
        << " is resolved, return false";
    return false;
  }

  if (lock_info.lock_ts() > 0) {
    if (isolation_level == pb::store::IsolationLevel::SnapshotIsolation) {
      // for pessimistic, check for_update_ts
      if (lock_info.for_update_ts() > 0) {
        if (lock_info.for_update_ts() < start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]CheckLockConflict SI lock_info.for_update_ts() > 0, it's conflict, lock_info: "
              << lock_info.ShortDebugString() << ", start_ts: " << start_ts;
          // for_update_ts < start_ts, return lock_info
          *(txn_result_info.mutable_locked()) = lock_info;
          return true;
        }
      } else {
        if (lock_info.lock_ts() < start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]CheckLockConflict SI lock_info.for_update_ts() == 0, it's conflict, lock_info: "
              << lock_info.ShortDebugString() << ", start_ts: " << start_ts;
          // lock_ts < start_ts, return lock_info
          *(txn_result_info.mutable_locked()) = lock_info;
          return true;
        }
      }
      return false;

    } else if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
      // for pessimistic lock, if just on lock stage, there is no confict
      // if on prewrite stage, need to check the for_update_ts
      // for optimistic lock, need to check the lock_ts
      if (lock_info.for_update_ts() > 0) {
        if (lock_info.lock_type() == pb::store::Lock) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]CheckLockConflict RC lock_info.for_update_ts() > 0, but only on LOCK stage, it's ok, "
                 "lock_info: "
              << lock_info.ShortDebugString() << ", start_ts: " << start_ts;
          return false;
        }

        if (lock_info.for_update_ts() < start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]CheckLockConflict RC lock_info.for_update_ts() > 0, on PREWRITE stage, it's "
                 "conflict, lock_info: "
              << lock_info.ShortDebugString() << ", start_ts: " << start_ts;
          // for_update_ts < start_ts, return lock_info
          *(txn_result_info.mutable_locked()) = lock_info;
          return true;
        }
        return false;

      } else {
        if (lock_info.lock_ts() < start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]CheckLockConflict RC lock_info.for_update_ts() == 0, it's conflict, lock_info: "
              << lock_info.ShortDebugString() << ", start_ts: " << start_ts;
          // lock_ts < start_ts, return lock_info
          *(txn_result_info.mutable_locked()) = lock_info;
          return true;
        }
        return false;
      }
    } else {
      DINGO_LOG(FATAL) << "[txn]CheckLockConflict invalid isolation_level: " << isolation_level;
    }
  }

  return false;
}

bvar::LatencyRecorder g_txn_scan_lock_latency("dingo_txn_scan_lock");

class TxnScanLockStreamState;
using TxnScanLockStreamStatePtr = std::shared_ptr<TxnScanLockStreamState>;

class TxnScanLockStreamState : public StreamState {
 public:
  TxnScanLockStreamState(IteratorPtr iter) : iter(iter) {}
  ~TxnScanLockStreamState() override = default;

  static TxnScanLockStreamStatePtr New(RawEnginePtr engine, const pb::common::Range &range) {
    IteratorOptions iter_options;
    iter_options.lower_bound = mvcc::Codec::EncodeKey(range.start_key(), Constant::kLockVer);
    iter_options.upper_bound = mvcc::Codec::EncodeKey(range.end_key(), Constant::kLockVer);

    auto iter = engine->Reader()->NewIterator(Constant::kTxnLockCF, iter_options);
    CHECK(iter != nullptr) << "[txn] GetLockInfo NewIterator failed, range: " << Helper::RangeToString(range);
    iter->Seek(iter_options.lower_bound);
    return std::make_shared<TxnScanLockStreamState>(iter);
  }

  IteratorPtr iter;
};

butil::Status TxnEngineHelper::ScanLockInfo(StreamPtr stream, RawEnginePtr engine, int64_t min_lock_ts,
                                            int64_t max_lock_ts, const pb::common::Range &range, int64_t limit,
                                            std::vector<pb::store::LockInfo> &lock_infos, bool &has_more,
                                            std::string &end_scan_key) {
  BvarLatencyGuard bvar_guard(&g_txn_scan_lock_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][{}] ScanLockInfo lock_ts: [{},{}] range: {} limit: {}.", stream->StreamId(), min_lock_ts,
                     max_lock_ts, Helper::RangeToString(range), limit);

  auto stream_state = std::dynamic_pointer_cast<TxnScanLockStreamState>(
      stream->GetOrNewStreamState([&]() -> StreamStatePtr { return TxnScanLockStreamState::New(engine, range); }));
  IteratorPtr iter = stream_state->iter;
  CHECK(iter != nullptr) << fmt::format("[txn][{}] Scan stream_state->iter is nullptr.", stream->StreamId());

  auto stop_checker = [&stream](size_t size, size_t bytes) -> bool { return stream->Check(size, bytes); };
  size_t bytes = 0;
  while (iter->Valid()) {
    auto lock_value = iter->Value();
    CHECK(lock_value.length() > 8) << fmt::format(
        "[txn][{}] invalid lock_value, key: {} min_lock_ts: {} lock_value is less than 8 bytes: {}.",
        stream->StreamId(), Helper::StringToHex(iter->Key()), min_lock_ts, Helper::StringToHex(lock_value));

    pb::store::LockInfo lock_info;
    auto ret = lock_info.ParseFromArray(lock_value.data(), lock_value.size());
    CHECK(ret) << fmt::format("[txn][{}] parse lock info failed, key: {} lock_value(hex): {}.", stream->StreamId(),
                              Helper::StringToHex(iter->Key()), Helper::StringToHex(lock_value));

    end_scan_key = lock_info.key();

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][{}] get lock_info lock_ts: {} lock_info: {} iter->key: {} lock_key: {}.",
                       stream->StreamId(), lock_info.lock_ts(), lock_info.ShortDebugString(),
                       Helper::StringToHex(iter->Key()), Helper::StringToHex(lock_info.key()));

    // if lock is not exist, nothing to do
    if (lock_info.lock_ts() == 0) {
      DINGO_LOG(WARNING) << fmt::format("[txn][{}] txn_not_found with lock_info empty, iter->key: {}.",
                                        stream->StreamId(), Helper::StringToHex(iter->Key()));
      iter->Next();
      continue;
    }

    if (lock_info.lock_ts() < min_lock_ts || lock_info.lock_ts() >= max_lock_ts) {
      DINGO_LOG(WARNING) << fmt::format(
          "[txn][{}] txn_not_found with lock_info.lock_ts not in range, iter->key:  lock_ts: [{},{}] lock_info: {}.",
          stream->StreamId(), Helper::StringToHex(iter->Key()), min_lock_ts, max_lock_ts, lock_info.ShortDebugString());
      iter->Next();
      continue;
    }

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][{}] ScanLock push_back lock_info: {}.", stream->StreamId(), lock_info.ShortDebugString());

    bytes += lock_info.ByteSizeLong();
    lock_infos.push_back(std::move(lock_info));

    if (stop_checker(lock_infos.size(), bytes)) {
      has_more = true;

      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << fmt::format(
          "[txn][{}] ScanLockInfo has_more: {} lock_infos size: {} limit: {} max_scan_lock_limit: {} "
          "max_scan_memory_size: {}.",
          stream->StreamId(), has_more, lock_infos.size(), limit, FLAGS_stream_message_max_limit_size, bytes,
          FLAGS_stream_message_max_bytes);
      break;
    }

    iter->Next();
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_batch_get_latency("dingo_txn_batch_get");

butil::Status TxnEngineHelper::BatchGet(RawEnginePtr engine, const pb::store::IsolationLevel &isolation_level,
                                        int64_t start_ts, const std::vector<std::string> &keys,
                                        const std::set<int64_t> &resolved_locks,
                                        pb::store::TxnResultInfo &txn_result_info,
                                        std::vector<pb::common::KeyValue> &kvs) {
  BvarLatencyGuard bvar_guard(&g_txn_batch_get_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << "[txn]BatchGet keys_count: " << keys.size() << ", isolation_level: " << isolation_level
      << ", start_ts: " << start_ts << ", first_key: " << Helper::StringToHex(keys[0])
      << ", last_key: " << Helper::StringToHex(keys[keys.size() - 1]);

  if (keys.empty()) {
    return butil::Status::OK();
  }

  if (engine == nullptr) {
    DINGO_LOG(FATAL) << "[txn]BatchGet engine is null";
  }

  if (!kvs.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "kvs is not empty");
  }

  if (keys.size() > FLAGS_max_batch_get_count) {
    DINGO_LOG(ERROR) << "[txn]BatchGet keys_count: " << keys.size()
                     << " is too large, max_batch_get_count: " << FLAGS_max_batch_get_count;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "keys_count is too large");
  }

  if (isolation_level != pb::store::SnapshotIsolation && isolation_level != pb::store::ReadCommitted) {
    DINGO_LOG(ERROR) << "[txn]BatchGet invalid isolation_level: " << isolation_level;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid isolation_level");
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "txn_result_info is not empty");
  }

  TxnReader txn_reader(engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << "[txn]BatchGet txn_reader.Init failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "txn_reader.Init failed");
  }

  int64_t response_memory_size = 0;

  auto write_iter = txn_reader.GetWriteIter();
  if (write_iter == nullptr) {
    DINGO_LOG(ERROR) << "[txn]BatchGet GetWriteIter failed, start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "GetWriteIter failed");
  }

  // for every key in keys, get lock info, if lock_ts < start_ts, return LockInfo
  // else find the latest write below our start_ts
  // then read data from data_cf
  for (const auto &key : keys) {
    pb::common::KeyValue kv;
    kv.set_key(key);

    pb::store::LockInfo lock_info;
    auto ret = txn_reader.GetLockInfo(key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << "[txn]BatchGet GetLockInfo failed, key: " << Helper::StringToHex(key)
                       << ", status: " << ret.error_str();
    }

    auto is_lock_conflict = CheckLockConflict(lock_info, isolation_level, start_ts, resolved_locks, txn_result_info);
    if (is_lock_conflict) {
      DINGO_LOG(WARNING) << "[txn]BatchGet CheckLockConflict return conflict, key: " << Helper::StringToHex(key)
                         << ", isolation_level: " << isolation_level << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();
      return butil::Status::OK();
    }

    int64_t iter_start_ts;
    bool is_valid = false;
    if (isolation_level == pb::store::IsolationLevel::SnapshotIsolation) {
      iter_start_ts = start_ts;
    } else if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
      iter_start_ts = Constant::kMaxVer;
    } else {
      DINGO_LOG(ERROR) << "[txn]BatchGet invalid isolation_level: " << isolation_level;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid isolation_level");
    }

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "key: " << Helper::StringToHex(key) << ", iter_start_ts: " << iter_start_ts;

    IteratorOptions iter_options;
    iter_options.lower_bound = mvcc::Codec::EncodeKey(key, iter_start_ts);
    iter_options.upper_bound = mvcc::Codec::EncodeKey(key, 0);

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << "iter_options.lower_bound: " << Helper::StringToHex(iter_options.lower_bound)
        << ", iter_options.upper_bound: " << Helper::StringToHex(iter_options.upper_bound);

    // check isolation level and return value
    write_iter->Seek(iter_options.lower_bound);
    while (write_iter->Valid() && write_iter->Key() < iter_options.upper_bound) {
      if (write_iter->Key().length() <= 8) {
        DINGO_LOG(ERROR) << ", invalid write_key, key: " << Helper::StringToHex(write_iter->Key())
                         << ", start_ts: " << start_ts
                         << ", write_key is less than 8 bytes: " << Helper::StringToHex(write_iter->Key());
        return butil::Status(pb::error::Errno::EINTERNAL, "invalid write_key");
      }
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
      } else {
        DINGO_LOG(ERROR) << "[txn]BatchGet invalid isolation_level: " << isolation_level;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid isolation_level");
      }

      if (is_valid) {
        // write_ts <= start_ts, return write_info
        pb::store::WriteInfo write_info;
        auto ret = write_info.ParseFromArray(write_iter->Value().data(), write_iter->Value().size());
        if (!ret) {
          DINGO_LOG(FATAL) << "[txn]BatchGet parse write info failed, key: " << Helper::StringToHex(key)
                           << ", write_key: " << Helper::StringToHex(write_iter->Key())
                           << ", write_value(hex): " << Helper::StringToHex(write_iter->Value());
        }

        // check the op type of write_info
        if (write_info.op() == pb::store::Op::Rollback) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]BatchGet write_info.op() == pb::store::Op::Rollback, go to next line, key: "
              << Helper::StringToHex(key) << ", write_key: " << Helper::StringToHex(write_iter->Key())
              << ", write_info: " << write_info.ShortDebugString();
          // goto next write line
          write_iter->Next();
          continue;
        } else if (write_info.op() == pb::store::Op::Delete) {
          // if op is delete, value is null
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << "[txn]BatchGet write_info.op() == pb::store::Op::Delete, so value is null, key: "
              << Helper::StringToHex(key) << ", write_key: " << Helper::StringToHex(write_iter->Key())
              << ", write_info: " << write_info.ShortDebugString();
          kv.set_value(std::string());
          break;
        } else if (write_info.op() != pb::store::Op::Put) {
          DINGO_LOG(ERROR) << "[txn]BatchGet meet invalid write_info.op: " << write_info.op()
                           << ", key: " << Helper::StringToHex(key)
                           << ", write_info: " << write_info.ShortDebugString();
          return butil::Status(pb::error::Errno::EINTERNAL, "invalid write_info.op");
        }

        if (!write_info.short_value().empty()) {
          kv.set_value(write_info.short_value());
          break;
        }

        auto ret1 = txn_reader.GetDataValue(mvcc::Codec::EncodeKey(key, write_info.start_ts()), *kv.mutable_value());
        if (!ret1.ok() && ret1.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "[txn]BatchGet read data failed, key: " << Helper::StringToHex(key)
                           << ", status: " << ret1.error_str();
        } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(ERROR) << "[txn]BatchGet read data failed, data is illegally not found, key: "
                           << Helper::StringToHex(key) << ", status: " << ret1.error_str()
                           << ", ts: " << write_info.start_ts();
        }
        break;
      } else {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "[txn]BatchGet is_valid = false, go to next line, write_ts: " << write_ts << " >= start_ts: " << start_ts
            << ", key: " << Helper::StringToHex(key) << ", write_key: " << Helper::StringToHex(write_iter->Key());
      }

      write_iter->Next();
    }

    kvs.emplace_back(kv);
    response_memory_size += kv.ByteSizeLong();

    if (response_memory_size >= FLAGS_max_batch_get_memory_size) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "[txn]BatchGet kvs.size: " << kvs.size() << ", response_memory_size: " << response_memory_size
          << ", max_batch_get_count: " << FLAGS_max_batch_get_count
          << ", max_batch_get_memory_size: " << FLAGS_max_batch_get_memory_size;
      break;
    }
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_scan_latency("dingo_txn_scan");

class TxnScanStreamState;
using TxnScanStreamStatePtr = std::shared_ptr<TxnScanStreamState>;

class TxnScanStreamState : public StreamState {
 public:
  TxnScanStreamState(TxnIteratorPtr iter) : iter(iter) {}
  ~TxnScanStreamState() override = default;

  static TxnScanStreamStatePtr New(TxnIteratorPtr iter) { return std::make_shared<TxnScanStreamState>(iter); }

  TxnIteratorPtr iter;
};

butil::Status TxnEngineHelper::Scan(StreamPtr stream, RawEnginePtr raw_engine,
                                    const pb::store::IsolationLevel &isolation_level, int64_t start_ts,
                                    const pb::common::Range &range, int64_t limit, bool key_only, bool is_reverse,
                                    const std::set<int64_t> &resolved_locks, bool disable_coprocessor,
                                    const pb::common::CoprocessorV2 &coprocessor,
                                    pb::store::TxnResultInfo &txn_result_info, std::vector<pb::common::KeyValue> &kvs,
                                    bool &has_more, std::string &end_scan_key) {
  BvarLatencyGuard bvar_guard(&g_txn_scan_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << fmt::format(
      "[txn][{}] Scan start_ts: {} range: {} isolation_level: {} start_ts: {} limit: {} key_only: {} is_reverse: {} "
      "resolved_locks size: {} disable_coprocessor: {} coprocessor: {} txn_result_info: {}.",
      stream->StreamId(), start_ts, Helper::RangeToString(range), pb::store::IsolationLevel_Name(isolation_level),
      start_ts, limit, key_only, is_reverse, resolved_locks.size(), disable_coprocessor, coprocessor.ShortDebugString(),
      txn_result_info.ShortDebugString());

  if (isolation_level != pb::store::SnapshotIsolation && isolation_level != pb::store::ReadCommitted) {
    DINGO_LOG(ERROR) << fmt::format("[txn][{}] TxnScan invalid isolation_level: {}.", stream->StreamId(),
                                    pb::store::IsolationLevel_Name(isolation_level));
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid isolation_level");
  }

  if (!kvs.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "kvs is not empty");
  }

  // get or new TxnIterator.
  auto stream_state =
      std::dynamic_pointer_cast<TxnScanStreamState>(stream->GetOrNewStreamState([&]() -> StreamStatePtr {
        auto iter = std::make_shared<TxnIterator>(raw_engine, range, start_ts, isolation_level, resolved_locks);
        auto ret = iter->Init();
        CHECK(ret.ok()) << fmt::format("[txn][{}] Scan init txn_iter failed, start_ts: {} range: {}  status: {}.",
                                       stream->StreamId(), start_ts, Helper::RangeToString(range), ret.error_str());
        iter->Seek(range.start_key());
        return TxnScanStreamState::New(iter);
      }));
  TxnIteratorPtr iter = stream_state->iter;
  CHECK(iter != nullptr) << fmt::format("[txn][{}] Scan stream_state->iter is nullptr.", stream->StreamId());

  RawCoprocessor::StopChecker stop_checker = [&stream](size_t size, size_t bytes) -> bool {
    return stream->Check(size, bytes);
  };

  if (!disable_coprocessor) {
    RawCoprocessorPtr txn_coprocessor = CoprocessorV2::New(Helper::GetKeyPrefix(range.start_key()));
    butil::Status status = txn_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][{}] Scan coprocessor::Open failed, error: {}.", stream->StreamId(),
                                      status.error_cstr());
      return status;
    }

    status = txn_coprocessor->Execute(iter, key_only, is_reverse, stop_checker, txn_result_info, kvs, has_more);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][{}] Scan coprocessor::Execute failed, error: {}.", stream->StreamId(),
                                      status.error_cstr());
      return status;
    }

    txn_coprocessor->Close();
    txn_coprocessor.reset();

  } else {
    size_t bytes = 0;
    while (iter->Valid(txn_result_info)) {
      auto key = iter->Key();
      auto value = iter->Value();

      pb::common::KeyValue kv;
      kv.set_key(key);
      if (!key_only) kv.set_value(value);
      bytes += kv.ByteSizeLong();
      kvs.push_back(std::move(kv));

      if (stop_checker(kvs.size(), bytes)) {
        has_more = true;

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << fmt::format(
            "[txn][{}] ScanLockInfo has_more: {} kv size: {} limit: {} max_scan_lock_limit: {} response_memory_size: "
            "{} max_scan_memory_size: {}.",
            stream->StreamId(), has_more, kvs.size(), limit, FLAGS_stream_message_max_limit_size, bytes,
            FLAGS_stream_message_max_bytes);

        break;
      }

      iter->Next();
    }
  }

  if (iter->Valid(txn_result_info)) {
    end_scan_key = iter->Key();
    iter->Next();
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_pessimistic_lock_latency("dingo_txn_pessimistic_lock");

butil::Status TxnEngineHelper::PessimisticLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                               std::shared_ptr<Context> ctx,
                                               const std::vector<pb::store::Mutation> &mutations,
                                               const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                               int64_t for_update_ts, bool return_values,
                                               std::vector<pb::common::KeyValue> &kvs) {
  BvarLatencyGuard bvar_guard(&g_txn_pessimistic_lock_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutations.size()
      << ", primary_lock: " << Helper::StringToHex(primary_lock) << ", lock_ttl: " << lock_ttl
      << ", for_update_ts: " << for_update_ts << ", return_values: " << return_values;

  if (BAIDU_UNLIKELY(mutations.size() > FLAGS_max_pessimistic_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                     << ", mutations_size: " << mutations.size()
                     << ", primary_lock: " << Helper::StringToHex(primary_lock) << ", lock_ttl: " << lock_ttl
                     << ", for_update_ts: " << for_update_ts << ", mutations.size() > FLAGS_max_pessimistic_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "pessimistic lock mutations.size() > FLAGS_max_pessimistic_count");
  }

  std::vector<pb::common::KeyValue> kv_puts_lock;
  auto *response = dynamic_cast<pb::store::TxnPessimisticLockResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", for_update_ts: " << for_update_ts << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }

  auto *error = response->mutable_error();
  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  // for every mutation, check and do lock, if any one of the mutation is failed, the whole lock is failed
  // 1. check if a lock is exists:
  for (const auto &mutation : mutations) {
    if (mutation.op() != pb::store::Op::Lock) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
                       << ", invalid mutation op, op: " << mutation.op();
      error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      error->set_errmsg("invalid mutation op");
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "invalid mutation op");
    }

    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    pb::store::LockInfo lock_info;
    auto ret = txn_reader.GetLockInfo(mutation.key(), lock_info);
    if (!ret.ok()) {
      // Now we need to fatal exit to prevent data inconsistency between raft peers
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
                       << ", get lock info failed, key: " << Helper::StringToHex(mutation.key())
                       << ", status: " << ret.error_str();

      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());

      // need response to client
      return ret;
    }

    if (!lock_info.primary_lock().empty()) {
      if (lock_info.for_update_ts() == 0) {
        // this is a optimistic lock, return lock_info
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
                         << ", key: " << Helper::StringToHex(mutation.key())
                         << " is locked by optimistic lock, lock_info: " << lock_info.ShortDebugString();
        // return lock_info
        *response->add_txn_result()->mutable_locked() = lock_info;
        continue;
      } else if (lock_info.lock_ts() == start_ts) {
        if (lock_info.for_update_ts() == for_update_ts) {
          // this is same pessimistic lock request, just do nothing.
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
              << ", key: " << Helper::StringToHex(mutation.key())
              << " is locked by self, lock_info: " << lock_info.ShortDebugString();

          if (return_values) {
            pb::store::WriteInfo write_info;
            auto ret2 = txn_reader.GetOldValue(mutation.key(), start_ts, false, write_info, kvs);
            if (!ret2.ok()) {
              // Now we need to fatal exit to prevent data inconsistency between raft peers
              DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(),
                                              start_ts)
                               << ", get old value failed, key: " << Helper::StringToHex(mutation.key())
                               << ", status: " << ret2.error_str();

              error->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
              error->set_errmsg(ret2.error_str());

              // need response to client
              return ret2;
            }
          }

          continue;
        } else if (lock_info.for_update_ts() < for_update_ts) {
          // this is a same pessimistic lock with a new for_update_ts, we need to update the lock
          pb::common::KeyValue kv;
          kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));

          lock_info.set_primary_lock(primary_lock);
          lock_info.set_lock_ts(start_ts);
          lock_info.set_for_update_ts(for_update_ts);
          lock_info.set_key(mutation.key());
          lock_info.set_lock_ttl(lock_ttl);
          lock_info.set_lock_type(pb::store::Op::Lock);
          lock_info.set_extra_data(mutation.value());
          kv.set_value(lock_info.SerializeAsString());
          kv_puts_lock.push_back(kv);

          if (return_values) {
            pb::store::WriteInfo write_info;
            auto ret3 = txn_reader.GetOldValue(mutation.key(), start_ts, false, write_info, kvs);
            if (!ret3.ok()) {
              // Now we need to fatal exit to prevent data inconsistency between raft peers
              DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(),
                                              start_ts)
                               << ", get old value failed, key: " << Helper::StringToHex(mutation.key())
                               << ", status: " << ret3.error_str();

              error->set_errcode(static_cast<pb::error::Errno>(ret3.error_code()));
              error->set_errmsg(ret3.error_str());

              // need response to client
              return ret3;
            }
          }
        } else {
          // lock_info.for_update_ts() > for_update_ts, this is a illegal request, we return lock_info
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
                           << ", key: " << Helper::StringToHex(mutation.key())
                           << " is locked by pessimistic with larger for_update_ts, lock_info: "
                           << lock_info.ShortDebugString();

          // return lock_info
          *response->add_txn_result()->mutable_locked() = lock_info;
          continue;
        }
      } else {
        // this is a lock conflict, return lock_info
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
            << ", key: " << Helper::StringToHex(mutation.key())
            << " is locked conflict, lock_info: " << lock_info.ShortDebugString();

        // add txn_result for response
        // setup lock_info
        *response->add_txn_result()->mutable_locked() = lock_info;

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
            << ", lock_conflict, key: " << Helper::StringToHex(mutation.key())
            << ", lock_info: " << lock_info.ShortDebugString();

        // need response to client
        continue;
      }
    } else {
      // there is not lock exists, we need to check if for_update_ts will confict with commit_ts
      pb::store::WriteInfo write_info;
      int64_t commit_ts = 0;
      int64_t min_commit_ts = start_ts;
      if (return_values) {
        min_commit_ts = 0;
      }
      auto ret4 = txn_reader.GetWriteInfo(min_commit_ts, Constant::kMaxVer, 0, mutation.key(), false, true, true,
                                          write_info, commit_ts);
      if (!ret4.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
                         << ", get write info failed, key: " << Helper::StringToHex(mutation.key())
                         << ", min_commit_ts: " << min_commit_ts << ", status: " << ret4.error_str();
        error->set_errcode(static_cast<pb::error::Errno>(ret4.error_code()));
        error->set_errmsg(ret4.error_str());
        return ret4;
      }
      if (commit_ts >= for_update_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "find this transaction is committed,return  WriteConflict with for_update_ts: " << for_update_ts
            << ", start_ts: " << start_ts << ", commit_ts: " << commit_ts
            << ", write_info: " << write_info.ShortDebugString();

        // pessimistic lock meet write_conflict here
        // add txn_result for response
        // setup write_conflict ( this may not be necessary, when lock_info is set)
        auto *txn_result = response->add_txn_result();
        auto *write_conflict = txn_result->mutable_write_conflict();
        write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_PessimisticRetry);
        write_conflict->set_start_ts(start_ts);
        write_conflict->set_conflict_ts(commit_ts);
        write_conflict->set_key(mutation.key());
        write_conflict->set_primary_key(lock_info.primary_lock());

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
            << ", write_conflict, start_ts: " << start_ts << ", commit_ts: " << commit_ts
            << ", write_info: " << write_info.ShortDebugString();

        continue;
      } else {
        // there is no lock and no write_confict, we can do lock
        pb::common::KeyValue kv;
        kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));

        pb::store::LockInfo lock_info;
        lock_info.set_primary_lock(primary_lock);
        lock_info.set_lock_ts(start_ts);
        lock_info.set_for_update_ts(for_update_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_lock_type(pb::store::Op::Lock);
        lock_info.set_extra_data(mutation.value());
        kv.set_value(lock_info.SerializeAsString());

        kv_puts_lock.push_back(kv);
        if (return_values) {
          auto ret5 = txn_reader.GetOldValue(mutation.key(), start_ts, true, write_info, kvs);
          if (!ret5.ok()) {
            // Now we need to fatal exit to prevent data inconsistency between raft peers
            DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(),
                                            start_ts)
                             << ", get old value failed, key: " << Helper::StringToHex(mutation.key())
                             << ", status: " << ret5.error_str();

            error->set_errcode(static_cast<pb::error::Errno>(ret5.error_code()));
            error->set_errmsg(ret5.error_str());

            // need response to client
            return ret5;
          }
        }
      }
    }
  }

  if (response->txn_result_size() > 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] PessimisticLock return txn_result,", ctx->RegionId())
        << ", txn_result_size: " << response->txn_result_size() << ", start_ts: " << start_ts
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutations.size();
    return butil::Status::OK();
  }

  if (kv_puts_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] PessimisticLock return empty kv_puts_lock,", ctx->RegionId())
        << ", kv_puts_lock_size: " << kv_puts_lock.size() << ", start_ts: " << start_ts
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutations.size();
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *lock_puts = cf_put_delete->add_puts_with_cf();
  lock_puts->set_cf_name(Constant::kTxnLockCF);
  for (auto &kv_put : kv_puts_lock) {
    auto *kv = lock_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
        << ", add lock kv, key: " << Helper::StringToHex(kv_put.key())
        << ", value: " << Helper::StringToHex(kv_put.value());
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock,", ctx->RegionId())
                     << ", write raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_do_update_lock_latency("dingo_txn_do_update_lock");

butil::Status TxnEngineHelper::DoUpdateLock(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                            const pb::store::LockInfo &lock_info) {
  BvarLatencyGuard bvar_guard(&g_txn_do_update_lock_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] UpdateLock, start_ts: {}", ctx->RegionId(), lock_info.lock_ts())
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", lock_info: " << lock_info.ShortDebugString();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << fmt::format("[txn][region({})] UpdateLock", region->Id())
                                                          << ", region is not found, region_id: " << ctx->RegionId();

    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  std::vector<pb::common::KeyValue> kv_puts_lock;

  // update lock_info
  pb::common::KeyValue kv;
  kv.set_key(mvcc::Codec::EncodeKey(lock_info.key(), Constant::kLockVer));
  kv.set_value(lock_info.SerializeAsString());

  kv_puts_lock.push_back(kv);

  if (kv_puts_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] UpdateLock return empty kv_puts_lock,", region->Id())
        << ", kv_puts_lock_size: " << kv_puts_lock.size() << ", start_ts: " << lock_info.lock_ts()
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
        << ", lock_info: " << lock_info.ShortDebugString();

    return butil::Status::OK();
  }

  // write lock_info into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *lock_puts = cf_put_delete->add_puts_with_cf();
  lock_puts->set_cf_name(Constant::kTxnLockCF);
  for (auto &kv_put : kv_puts_lock) {
    auto *kv = lock_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << fmt::format("[txn][region({})] UpdateLock,", region->Id())
                                                          << ", add lock kv, key: " << Helper::StringToHex(kv_put.key())
                                                          << ", value: " << Helper::StringToHex(kv_put.value());
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] UpdateLock,", region->Id())
                     << ", write raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_pessimistic_rollback_latency("dingo_txn_pessimistic_rollback");

butil::Status TxnEngineHelper::PessimisticRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                   std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                   int64_t start_ts, int64_t for_update_ts,
                                                   const std::vector<std::string> &keys) {
  BvarLatencyGuard bvar_guard(&g_txn_pessimistic_rollback_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] PessimisticRollback, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", for_update_ts: " << for_update_ts
      << ", keys_size: " << keys.size();

  if (BAIDU_UNLIKELY(keys.size() > FLAGS_max_pessimistic_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticRollback, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                     << ", for_update_ts: " << for_update_ts << ", keys_size: " << keys.size()
                     << ", keys.size() > FLAGS_max_pessimistic_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "pessimistic rollback keys.size() > FLAGS_max_pessimistic_count");
  }

  std::vector<std::string> kv_dels_lock;

  auto *response = dynamic_cast<pb::store::TxnPessimisticRollbackResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", region->Id(), start_ts)
                     << ", for_update_ts: " << for_update_ts << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }

  auto *error = response->mutable_error();

  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticRollback, start_ts: {}", region->Id(), start_ts)
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  // for every key, check and do rollback lock, if any one of the rollback is failed, the whole lock is failed
  // 1. check if a lock is exists:
  //    if a lock exists: a)  if start_ts
  for (const auto &key : keys) {
    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    pb::store::LockInfo lock_info;
    auto ret = txn_reader.GetLockInfo(key, lock_info);
    if (!ret.ok()) {
      // Now we need to fatal exit to prevent data inconsistency between raft peers
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticRollback, start_ts: {}", region->Id(), start_ts)
                       << ", get lock info failed, key: " << Helper::StringToHex(key)
                       << ", status: " << ret.error_str();

      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());

      // need response to client
      return ret;
    }

    if (!lock_info.primary_lock().empty()) {
      if (lock_info.lock_type() != pb::store::Op::Lock) {
        // this is a optimistic lock or pessimistic lock in prewrite stage, return lock_info
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
                           << ", key: " << Helper::StringToHex(key)
                           << " is locked by optimistic lock or pessimistic lock in prewrite stage, lock_info: "
                           << lock_info.ShortDebugString();
        // return lock_info
        *response->add_txn_result()->mutable_locked() = lock_info;
        continue;
      } else if (lock_info.lock_ts() == start_ts) {
        if (lock_info.for_update_ts() <= for_update_ts) {
          // this is same pessimistic lock request, just do rollback.
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
              << ", key: " << Helper::StringToHex(key)
              << " is locked by self, can do rollback, lock_info: " << lock_info.ShortDebugString();
          kv_dels_lock.push_back(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
          continue;
        } else {
          // this is a same pessimistic lock with a not equal for_update_ts, there may be some error
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
                           << ", key: " << Helper::StringToHex(key)
                           << " is locked by pessimistic with not equal for_update_ts, lock_info: "
                           << lock_info.ShortDebugString() << ", for_update_ts: " << for_update_ts;
          // return lock_info
          *response->add_txn_result()->mutable_locked() = lock_info;
          continue;
        }
      } else {
        // this is a lock conflict, return lock_info
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
            << ", key: " << Helper::StringToHex(key)
            << " is locked conflict, lock_info: " << lock_info.ShortDebugString();

        // add txn_result for response
        // setup lock_info
        *response->add_txn_result()->mutable_locked() = lock_info;
        continue;
      }
    } else {
      // there is not lock exists, rollback need to do nothing, just return ok.
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
          << ", key: " << Helper::StringToHex(key)
          << " is not locked, can do rollback, lock_info: " << lock_info.ShortDebugString();
      continue;
    }
  }

  if (response->txn_result_size() > 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] PessimisticRollback return txn_result,", region->Id())
        << ", txn_result_size: " << response->txn_result_size() << ", start_ts: " << start_ts
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", keys_size: " << keys.size();
    return butil::Status::OK();
  }

  if (kv_dels_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
        << ", kv_dels_lock is empty, start_ts: " << start_ts
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", keys_size: " << keys.size();
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *lock_dels = cf_put_delete->add_deletes_with_cf();
  lock_dels->set_cf_name(Constant::kTxnLockCF);
  for (auto &kv_del : kv_dels_lock) {
    lock_dels->add_keys(kv_del);
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] PessimisticRollback,", region->Id())
                     << ", write raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_prewrite_latency("dingo_txn_prewrite");

void TxnEngineHelper::GenFinalMinCommitTs(int64_t region_id, std::string key, int64_t start_ts, int64_t for_update_ts,
                                          int64_t lock_min_commit_ts, int64_t max_commit_ts,
                                          int64_t &final_min_commit_ts) {
  int64_t min_commit_ts = std::max(start_ts, for_update_ts) + 1;
  final_min_commit_ts = std::max(min_commit_ts, lock_min_commit_ts);
  if (max_commit_ts != 0 && final_min_commit_ts > max_commit_ts) {
    DINGO_LOG(WARNING) << fmt::format(
                              "[txn][region({})] Prewrite 1pc commit_ts({}) is too large, fallback to normal 2PC",
                              region_id, final_min_commit_ts)
                       << ", key: " << key << ", start_ts: " << start_ts << " , for_update_ts: " << for_update_ts
                       << " , lock_min_commit_ts: " << lock_min_commit_ts << " , max_commit_ts: " << max_commit_ts;
    final_min_commit_ts = 0;
  }
}

butil::Status TxnEngineHelper::GenPrewriteDataAndLock(
    int64_t region_id, const pb::store::Mutation &mutation, const pb::store::LockInfo &prev_lock_info,
    const pb::store::WriteInfo &write_info, const std::string &primary_lock, int64_t start_ts, int64_t for_update_ts,
    int64_t lock_ttl, int64_t txn_size, const std::string &lock_extra_data, int64_t max_commit_ts,
    bool need_check_pessimistic_lock, bool &try_one_pc, std::vector<pb::common::KeyValue> &kv_puts_data,
    std::vector<pb::common::KeyValue> &kv_puts_lock,
    std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc,
    int64_t &final_min_commit_ts) {
  // do Put/Delete/PutIfAbsent
  if (mutation.op() == pb::store::Op::Put) {
    // put data
    if (mutation.value().length() > FLAGS_max_short_value_in_write_cf) {
      pb::common::KeyValue kv;
      std::string data_key = mvcc::Codec::EncodeKey(mutation.key(), start_ts);
      kv.set_key(data_key);
      kv.set_value(mutation.value());

      kv_puts_data.push_back(kv);
    }

    // put lock
    {
      pb::store::LockInfo lock_info = prev_lock_info;
      lock_info.set_primary_lock(primary_lock);
      lock_info.set_lock_ts(start_ts);
      lock_info.set_key(mutation.key());
      lock_info.set_lock_ttl(lock_ttl);
      lock_info.set_txn_size(txn_size);
      lock_info.set_lock_type(pb::store::Op::Put);
      if (BAIDU_UNLIKELY(mutation.value().empty())) {
        return butil::Status(pb::error::Errno::EVALUE_EMPTY, "value is empty");
      }
      if (mutation.value().length() <= FLAGS_max_short_value_in_write_cf) {
        lock_info.set_short_value(mutation.value());
      }
      if (!lock_extra_data.empty()) {
        lock_info.set_extra_data(lock_extra_data);
      }

      if (try_one_pc) {
        GenFinalMinCommitTs(region_id, mutation.key(), start_ts, for_update_ts, prev_lock_info.min_commit_ts(),
                            max_commit_ts, final_min_commit_ts);
        if (final_min_commit_ts == 0) {
          // fallback_to_2PC
          try_one_pc = false;
        }
      }

      if (try_one_pc) {
        locks_for_1pc.push_back(
            std::make_tuple(mutation.key(), mutation.value(), lock_info, need_check_pessimistic_lock));
      } else {
        pb::common::KeyValue kv;
        kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));
        kv.set_value(lock_info.SerializeAsString());
        kv_puts_lock.push_back(kv);
      }
    }
  } else if (mutation.op() == pb::store::PutIfAbsent) {
    if (write_info.op() == pb::store::Op::Put) {
      // this mutation is success with key_exist, go to next mutation
      // put lock with op=PutIfAbsent
      // CAUTION: we do nothing in commit if lock.lock_type is PutIfAbsent, this is just a placeholder of a lock
      // with nothing to do
      {
        pb::store::LockInfo lock_info = prev_lock_info;
        lock_info.set_primary_lock(primary_lock);
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::PutIfAbsent);
        if (!lock_extra_data.empty()) {
          lock_info.set_extra_data(lock_extra_data);
        }

        if (try_one_pc) {
          GenFinalMinCommitTs(region_id, mutation.key(), start_ts, for_update_ts, prev_lock_info.min_commit_ts(),
                              max_commit_ts, final_min_commit_ts);
          if (final_min_commit_ts == 0) {
            // fallback_to_2PC
            try_one_pc = false;
          }
        }

        if (try_one_pc) {
          locks_for_1pc.push_back(
              std::make_tuple(mutation.key(), mutation.value(), lock_info, need_check_pessimistic_lock));
        } else {
          pb::common::KeyValue kv;
          kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));
          kv.set_value(lock_info.SerializeAsString());
          kv_puts_lock.push_back(kv);
        }
      }
    } else {
      // put data
      if (mutation.value().length() > FLAGS_max_short_value_in_write_cf) {
        pb::common::KeyValue kv;
        std::string data_key = mvcc::Codec::EncodeKey(mutation.key(), start_ts);
        kv.set_key(data_key);
        kv.set_value(mutation.value());

        kv_puts_data.push_back(kv);
      }

      // put lock
      {
        pb::store::LockInfo lock_info = prev_lock_info;
        lock_info.set_primary_lock(primary_lock);
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::Put);
        if (BAIDU_UNLIKELY(mutation.value().empty())) {
          return butil::Status(pb::error::Errno::EVALUE_EMPTY, "value is empty");
        }
        if (mutation.value().length() <= FLAGS_max_short_value_in_write_cf) {
          lock_info.set_short_value(mutation.value());
        }
        if (!lock_extra_data.empty()) {
          lock_info.set_extra_data(lock_extra_data);
        }
        if (try_one_pc) {
          GenFinalMinCommitTs(region_id, mutation.key(), start_ts, for_update_ts, prev_lock_info.min_commit_ts(),
                              max_commit_ts, final_min_commit_ts);
          if (final_min_commit_ts == 0) {
            // fallback_to_2PC
            try_one_pc = false;
          }
        }
        if (try_one_pc) {
          locks_for_1pc.push_back(
              std::make_tuple(mutation.key(), mutation.value(), lock_info, need_check_pessimistic_lock));
        } else {
          pb::common::KeyValue kv;
          kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));
          kv.set_value(lock_info.SerializeAsString());
          kv_puts_lock.push_back(kv);
        }
      }
    }
  } else if (mutation.op() == pb::store::CheckNotExists) {
    // For CheckNotExists, this op is equal to PutIfAbsent, but we do not need to write anything, just check if key is
    // exist. So it is more likely to be a get op in prewrite.
    // do nothing;
    if (try_one_pc) {
      final_min_commit_ts = start_ts + 1;
    }
  } else if (mutation.op() == pb::store::Op::Delete) {
    // put data
    // for delete, we don't write anything to kTxnDataCf.
    // when doing commit, we read op from lock_info, and write op to kTxnWriteCf with write_info.

    // put lock
    {
      pb::store::LockInfo lock_info = prev_lock_info;
      lock_info.set_primary_lock(primary_lock);
      lock_info.set_lock_ts(start_ts);
      lock_info.set_key(mutation.key());
      lock_info.set_lock_ttl(lock_ttl);
      lock_info.set_txn_size(txn_size);
      lock_info.set_lock_type(pb::store::Op::Delete);
      if (!lock_extra_data.empty()) {
        lock_info.set_extra_data(lock_extra_data);
      }
      if (try_one_pc) {
        GenFinalMinCommitTs(region_id, mutation.key(), start_ts, for_update_ts, prev_lock_info.min_commit_ts(),
                            max_commit_ts, final_min_commit_ts);
        if (final_min_commit_ts == 0) {
          // fallback_to_2PC
          try_one_pc = false;
        }
      }

      if (try_one_pc) {
        locks_for_1pc.push_back(
            std::make_tuple(mutation.key(), mutation.value(), lock_info, need_check_pessimistic_lock));
      } else {
        pb::common::KeyValue kv;
        kv.set_key(mvcc::Codec::EncodeKey(mutation.key(), Constant::kLockVer));
        kv.set_value(lock_info.SerializeAsString());
        kv_puts_lock.push_back(kv);
      }
    }
  } else {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite,", region_id)
                     << ", invalid op, key: " << Helper::StringToHex(mutation.key()) << ", start_ts: " << start_ts
                     << ", op: " << mutation.op();
    return butil::Status(pb::error::Errno::EINTERNAL, "invalid op");
  }
  return butil::Status::OK();
}

void TxnEngineHelper::FallbackTo1PCLocks(
    std::vector<pb::common::KeyValue> &kv_puts_lock,
    std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc) {
  for (auto const &[key, data_value, lock_info, is_pessimistic_lock] : locks_for_1pc) {
    // pessimistic and optimistic lock all need to record
    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
    kv.set_value(lock_info.SerializeAsString());
    kv_puts_lock.push_back(kv);
  }
}

// Commit and delete all 1pc locks in txn.
butil::Status TxnEngineHelper::OnePCommit(
    std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
    int64_t final_commit_ts,
    std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc,
    std::vector<pb::common::KeyValue> &kv_puts_data) {
  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  // for vector index region, commit to vector index
  auto *vector_add = cf_put_delete->mutable_vector_add();
  auto *vector_del = cf_put_delete->mutable_vector_del();

  // for document index region, commit to document index
  auto *document_add = cf_put_delete->mutable_document_add();
  auto *document_del = cf_put_delete->mutable_document_del();

  for (auto const &[key, data_value, lock_info, is_pessimistic_lock] : locks_for_1pc) {
    if (is_pessimistic_lock) {
      kv_deletes_lock.push_back(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
    }
    if (lock_info.lock_type() == pb::store::Op::PutIfAbsent) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(), start_ts,
                         final_commit_ts)
          << ", lock_type is PutIfAbsent, skip it, lock_info: " << lock_info.ShortDebugString();
      continue;
    }

    pb::common::KeyValue kv;
    std::string write_key = mvcc::Codec::EncodeKey(key, final_commit_ts);
    kv.set_key(write_key);

    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(lock_info.lock_type());
    if (!lock_info.short_value().empty()) {
      write_info.set_short_value(data_value);
    }
    kv.set_value(write_info.SerializeAsString());

    kv_puts_write.push_back(kv);

    if (region->Type() == pb::common::INDEX_REGION &&
        region->Definition().index_parameter().has_vector_index_parameter()) {
      if (lock_info.lock_type() == pb::store::Op::Put) {
        pb::common::VectorWithId vector_with_id;
        auto ret = vector_with_id.ParseFromString(data_value);
        if (!ret) {
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, final_commit_ts)
                           << ", parse vector_with_id failed, key: " << Helper::StringToHex(key)
                           << ", data_value: " << Helper::StringToHex(data_value)
                           << ", lock_info: " << lock_info.ShortDebugString();
          return butil::Status(pb::error::Errno::EINTERNAL, "parse vector_with_id failed");
        }

        *(vector_add->add_vectors()) = vector_with_id;
      } else if (lock_info.lock_type() == pb::store::Op::Delete) {
        auto vector_id = VectorCodec::UnPackageVectorId(lock_info.key());
        if (vector_id == 0) {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, final_commit_ts)
                           << ", decode vector_id failed, key: " << Helper::StringToHex(lock_info.key())
                           << ", lock_info: " << lock_info.ShortDebugString();
        }

        vector_del->add_ids(vector_id);
      } else {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                        start_ts, final_commit_ts)
                         << ", invalid lock_type, key: " << Helper::StringToHex(lock_info.key())
                         << ", lock_info: " << lock_info.ShortDebugString();
      }
    }

    if (region->Type() == pb::common::DOCUMENT_REGION &&
        region->Definition().index_parameter().has_document_index_parameter()) {
      if (lock_info.lock_type() == pb::store::Op::Put) {
        pb::common::DocumentWithId document_with_id;
        auto ret = document_with_id.ParseFromString(data_value);
        if (!ret) {
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, final_commit_ts)
                           << ", parse document_with_id failed, key: " << Helper::StringToHex(lock_info.key())
                           << ", data_value: " << Helper::StringToHex(data_value)
                           << ", lock_info: " << lock_info.ShortDebugString();
          return butil::Status(pb::error::Errno::EINTERNAL, "parse document_with_id failed");
        }

        *(document_add->add_documents()) = document_with_id;
      } else if (lock_info.lock_type() == pb::store::Op::Delete) {
        auto document_id = DocumentCodec::UnPackageDocumentId(lock_info.key());
        if (document_id == 0) {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, final_commit_ts)
                           << ", decode document_id failed, key: " << Helper::StringToHex(lock_info.key())
                           << ", lock_info: " << lock_info.ShortDebugString();
        }

        document_del->add_ids(document_id);
      } else {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(),
                                        start_ts, final_commit_ts)
                         << ", invalid lock_type, key: " << Helper::StringToHex(lock_info.key())
                         << ", lock_info: " << lock_info.ShortDebugString();
      }
    }
  }
  if (kv_puts_data.empty() && kv_puts_write.empty() && kv_deletes_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] OnePCCommit, start_ts: {} commit_ts: {}", region->Id(), start_ts,
                       final_commit_ts)
        << ", kv_puts_data is empty and kv_puts_write is empty and kv_deletes_lock is empty";
    return butil::Status::OK();
  }

  if (!kv_puts_data.empty()) {
    auto *data_puts = cf_put_delete->add_puts_with_cf();
    data_puts->set_cf_name(Constant::kTxnDataCF);
    for (auto &kv_put : kv_puts_data) {
      auto *kv = data_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  if (!kv_puts_write.empty()) {
    auto *write_puts = cf_put_delete->add_puts_with_cf();
    write_puts->set_cf_name(Constant::kTxnWriteCF);
    for (auto &kv_put : kv_puts_write) {
      auto *kv = write_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  if (!kv_deletes_lock.empty()) {
    auto *lock_dels = cf_put_delete->add_deletes_with_cf();
    lock_dels->set_cf_name(Constant::kTxnLockCF);
    for (auto &kv_del : kv_deletes_lock) {
      lock_dels->add_keys(kv_del);
    }
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] OnePCCommit", region->Id()) << ", kv_puts_data_size: " << kv_puts_data.size()
      << ", kv_puts_write_size: " << kv_puts_write.size() << ", kv_deletes_lock_size: " << kv_deletes_lock.size()
      << ", start_ts: " << start_ts << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString();

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] OnePCCommit", region->Id())
                     << ", write raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }
  return ret;
}

butil::Status TxnEngineHelper::DoPreWrite(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                          int64_t region_id, int64_t start_ts, int64_t mutation_size,
                                          std::vector<pb::common::KeyValue> &kv_puts_data,
                                          std::vector<pb::common::KeyValue> &kv_puts_lock) {
  if (kv_puts_data.empty() && kv_puts_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] Prewrite return empty kv_puts_data and kv_puts_lock,", region_id)
        << ", kv_puts_data_size: " << kv_puts_data.size() << ", kv_puts_lock_size: " << kv_puts_lock.size()
        << ", start_ts: " << start_ts << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
        << ", mutations_size: " << mutation_size;
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  if (!kv_puts_data.empty()) {
    auto *data_puts = cf_put_delete->add_puts_with_cf();
    data_puts->set_cf_name(Constant::kTxnDataCF);
    for (auto &kv_put : kv_puts_data) {
      auto *kv = data_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  if (!kv_puts_lock.empty()) {
    auto *lock_puts = cf_put_delete->add_puts_with_cf();
    lock_puts->set_cf_name(Constant::kTxnLockCF);
    for (auto &kv_put : kv_puts_lock) {
      auto *kv = lock_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] Prewrite", region_id) << ", kv_puts_data_size: " << kv_puts_data.size()
      << ", kv_puts_lock_size: " << kv_puts_lock.size() << ", start_ts: " << start_ts
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutation_size;

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite", region_id)
                     << ", write raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

butil::Status TxnEngineHelper::Prewrite(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                        std::shared_ptr<Context> ctx, store::RegionPtr region,
                                        const std::vector<pb::store::Mutation> &mutations,
                                        const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                        int64_t txn_size, bool try_one_pc, int64_t max_commit_ts,
                                        const std::vector<int64_t> &pessimistic_checks,
                                        const std::map<int64_t, int64_t> &for_update_ts_checks,
                                        const std::map<int64_t, std::string> &lock_extra_datas) {
  BvarLatencyGuard bvar_guard(&g_txn_prewrite_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] Prewrite, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutations.size()
      << ", primary_lock: " << Helper::StringToHex(primary_lock) << ", lock_ttl: " << lock_ttl
      << ", txn_size: " << txn_size << ", try_one_pc: " << try_one_pc << ", max_commit_ts: " << max_commit_ts
      << ", pessimistic_checks_size: " << pessimistic_checks.size()
      << ", for_update_ts_checks_size: " << for_update_ts_checks.size()
      << ", lock_extra_datas_size: " << lock_extra_datas.size();

  if (BAIDU_UNLIKELY(mutations.size() > FLAGS_max_prewrite_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                     << ", mutations_size: " << mutations.size()
                     << ", primary_lock: " << Helper::StringToHex(primary_lock) << ", lock_ttl: " << lock_ttl
                     << ", txn_size: " << txn_size << ", try_one_pc: " << try_one_pc
                     << ", max_commit_ts: " << max_commit_ts
                     << ", pessimistic_checks_size: " << pessimistic_checks.size()
                     << ", for_update_ts_checks_size: " << for_update_ts_checks.size()
                     << ", lock_extra_datas_size: " << lock_extra_datas.size()
                     << ", mutations.size() > FLAGS_max_prewrite_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "prewrite mutations.size() > FLAGS_max_prewrite_count");
  }

  if (!pessimistic_checks.empty()) {
    if (mutations.size() != pessimistic_checks.size()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", ctx->RegionId(), start_ts)
                       << ", mutations_size: " << mutations.size()
                       << ", pessimistic_checks_size: " << pessimistic_checks.size()
                       << ", mutations_size != pessimistic_checks_size";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "mutations_size != pessimistic_checks_size");
    }
  }

  std::vector<pb::common::KeyValue> kv_puts_data;
  std::vector<pb::common::KeyValue> kv_puts_lock;
  std::vector<std::string> kv_dels_lock;  // for PutIfAbsent on pessimistic lock, if key is exists, no put will be
                                          // done, need to delete the lock in prewrite
  int64_t final_min_commit_ts = 0;
  // When 1PC is enabled, locks will be collected here and put into
  // `writes`, so it can be further processed. The elements are map representing
  // ((key, value, lock_info, is_pessimistic_lock))
  std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> locks_for_1pc;

  auto *response = dynamic_cast<pb::store::TxnPrewriteResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();

  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  // for every mutation, check and do prewrite, if any one of the mutation is failed, the whole prewrite is failed
  for (int64_t i = 0; i < mutations.size(); i++) {
    const auto &mutation = mutations[i];

    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    pb::store::LockInfo prev_lock_info;
    auto ret = txn_reader.GetLockInfo(mutation.key(), prev_lock_info);
    if (!ret.ok()) {
      // TODO: do read before write to raft state machine
      // Now we need to fatal exit to prevent data inconsistency between raft peers
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                       << ", get lock info failed, key: " << Helper::StringToHex(mutation.key())
                       << ", status: " << ret.error_str();

      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());

      // need response to client
      return ret;
    }

    // if the mutation request is a repeated (already prewrited beforce, maybe just a retry from executor), will to do
    // all check and setup response, but do not apply to raft to save time and I/O
    bool is_repeated_prewrite = false;

    // if need to check pessimistic lock
    bool need_check_pessimistic_lock = false;
    if (!pessimistic_checks.empty() && pessimistic_checks[i] == 1) {
      need_check_pessimistic_lock = true;
    }

    // for optimistic prewrite
    if (!need_check_pessimistic_lock) {
      if (prev_lock_info.lock_type() == pb::store::Op::Lock) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
            << ", optimistic prewrite meet pessimistic lock, key: " << Helper::StringToHex(mutation.key())
            << ", lock_info: " << prev_lock_info.ShortDebugString();
        *response->add_txn_result()->mutable_locked() = prev_lock_info;
        continue;
      }

      if (!prev_lock_info.primary_lock().empty()) {
        // for optimistic prewrite, if the key is locked by same start_ts, this is a repeated prewrite, will skip
        // write to raft state machine
        if (prev_lock_info.lock_ts() == start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", key: " << Helper::StringToHex(mutation.key())
              << " is locked by same start_ts, this is a repeated prewrite, skip it, lock_info: "
              << prev_lock_info.ShortDebugString();

          // this is a repeated prewrite, will skip write to raft state machine
          is_repeated_prewrite = true;
        } else {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", key: " << Helper::StringToHex(mutation.key())
              << " is locked conflict, lock_info: " << prev_lock_info.ShortDebugString();

          // add txn_result for response
          // setup lock_info
          *response->add_txn_result()->mutable_locked() = prev_lock_info;

          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", lock_conflict, key: " << Helper::StringToHex(mutation.key())
              << ", lock_info: " << prev_lock_info.ShortDebugString();

          // need response to client
          continue;
        }
      }
    }
    // for pessimistic prewrite
    else {
      if (!prev_lock_info.primary_lock().empty()) {
        if (prev_lock_info.lock_ts() == start_ts) {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", key: " << Helper::StringToHex(mutation.key())
              << " is locked by same start_ts, lock_info: " << prev_lock_info.ShortDebugString();

          // if this is not a PessimisticLock, it is a repeated prewrite, will skip write to raft state machine
          if (prev_lock_info.lock_type() != pb::store::Op::Lock) {
            DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
                << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                << ", pessimistic prewrite meet optimistic lock, this is a repeated prewrite, skip it, key: "
                << Helper::StringToHex(mutation.key()) << ", lock_info: " << prev_lock_info.ShortDebugString();

            // this is a repeated prewrite, will skip write to raft state machine
            is_repeated_prewrite = true;
          }
          // do pessimistic check
          else if (for_update_ts_checks.find(i) != for_update_ts_checks.end()) {
            if (prev_lock_info.for_update_ts() != for_update_ts_checks.at(i)) {
              DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
                  << fmt::format("[txn][region({})] Prewrite,", region->Id())
                  << ", key: " << Helper::StringToHex(mutation.key())
                  << " is locked by same start_ts, but for_update_ts is not equal, lock_info: "
                  << prev_lock_info.ShortDebugString() << ", for_update_ts: " << for_update_ts_checks.at(i);

              // add txn_result for response
              // setup lock_info
              *response->add_txn_result()->mutable_locked() = prev_lock_info;

              // need response to client
              continue;
            }
          }
        } else {
          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", key: " << Helper::StringToHex(mutation.key())
              << " is locked conflict, lock_info: " << prev_lock_info.ShortDebugString();

          // add txn_result for response
          // setup lock_info
          *response->add_txn_result()->mutable_locked() = prev_lock_info;

          DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
              << fmt::format("[txn][region({})] Prewrite,", region->Id())
              << ", lock_conflict, key: " << Helper::StringToHex(mutation.key())
              << ", lock_info: " << prev_lock_info.ShortDebugString();

          // need response to client
          continue;
        }
      }
    }

    // 2. check if the key is committed or rollbacked after start_ts
    //    if the key is committed or rollbacked after start_ts, return WriteConflict
    // 2.1 check rollback
    // if there is a rollback, there will be a key | start_ts : WriteInfo| in write_cf
    pb::store::WriteInfo write_info;
    auto ret1 = txn_reader.GetRollbackInfo(start_ts, mutation.key(), write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                       << ", get rollback info failed, key: " << Helper::StringToHex(mutation.key())
                       << ", start_ts: " << start_ts << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == start_ts) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << "find this transaction is rollbacked,return  SelfRolledBack , start_ts: " << start_ts
          << ", write_info: " << write_info.ShortDebugString();

      // prewrite meet write_conflict here
      // add txn_result for response
      // setup write_conflict ( this may not be necessary, when lock_info is set)
      auto *txn_result = response->add_txn_result();
      auto *write_conflict = txn_result->mutable_write_conflict();
      write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_SelfRolledBack);
      write_conflict->set_start_ts(start_ts);
      write_conflict->set_conflict_ts(start_ts);
      write_conflict->set_key(mutation.key());
      // write_conflict->set_primary_key(lock_info.primary_lock());

      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] Prewrite,", region->Id()) << ", write_conflict, start_ts: " << start_ts
          << ", write_info: " << write_info.ShortDebugString();

      break;
    }

    // 2.2 check commit
    // if there is a commit, there will be a key | commit_ts : WriteInfo| in write_cf
    // for optimistic prewrite, we need to check if commit_ts >= start_ts
    // for pessimistic prewrite, we need to check if commit_ts >= for_update_ts, but this check is done in lock
    // phase, so we do not need to check here
    int64_t commit_ts = 0;
    auto ret2 =
        txn_reader.GetWriteInfo(0, Constant::kMaxVer, 0, mutation.key(), false, true, true, write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Prewrite", region->Id())
                       << ", get write info failed, key: " << Helper::StringToHex(mutation.key())
                       << ", start_ts: " << start_ts << ", status: " << ret2.error_str();
    }

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] Prewrite", region->Id()) << ", key: " << Helper::StringToHex(mutation.key())
        << ", start_ts: " << start_ts << ", commit_ts: " << commit_ts
        << ", write_info: " << write_info.ShortDebugString();

    if (commit_ts >= start_ts) {
      if (!need_check_pessimistic_lock) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "Optimistic Prewrite find this transaction is committed after start_ts,return "
               "WriteConflict start_ts: "
            << start_ts << ", commit_ts: " << commit_ts << ", write_info: " << write_info.ShortDebugString();

        // prewrite meet write_conflict here
        // add txn_result for response
        // setup write_conflict ( this may not be necessary, when lock_info is set)
        auto *txn_result = response->add_txn_result();
        auto *write_conflict = txn_result->mutable_write_conflict();
        write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        write_conflict->set_start_ts(start_ts);
        write_conflict->set_conflict_ts(commit_ts);
        write_conflict->set_key(mutation.key());
        // write_conflict->set_primary_key(lock_info.primary_lock());

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] Prewrite", region->Id()) << ", write_conflict, start_ts: " << start_ts
            << ", commit_ts: " << commit_ts << ", write_info: " << write_info.ShortDebugString();
        break;
      } else {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "Pessimistic Prewrite find this transaction is committed after start_ts, it's ok. start_ts: " << start_ts
            << ", commit_ts: " << commit_ts;
      }
    }

    // 3.check special mutation op.
    if (mutation.op() == pb::store::Op::PutIfAbsent) {
      // check if key is exist
      if (write_info.op() == pb::store::Op::Put) {
        response->add_keys_already_exist()->set_key(mutation.key());
      }
    } else if (mutation.op() == pb::store::Op::CheckNotExists) {
      // For CheckNotExists, this op is equal to PutIfAbsent, but we do not need to write anything, just check if key
      // is exist. So it is more likely to be a get op in prewrite.

      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] Prewrite", region->Id()) << ", key: " << Helper::StringToHex(mutation.key())
          << " is CheckNotExists, start_ts: " << start_ts << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
          << ", mutations_size: " << mutations.size() << ", prev_write_info is: " << write_info.ShortDebugString();
      // CheckNotExists should not appear in a pessimistic prewrite, so check this here
      if (need_check_pessimistic_lock) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite", region->Id())
                         << ", CheckNotExists should not appear in a pessimistic prewrite, key: "
                         << Helper::StringToHex(mutation.key()) << ", start_ts: " << start_ts
                         << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                         << ", mutations_size: " << mutations.size()
                         << ", prev_write_info is: " << write_info.ShortDebugString();

        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "CheckNotExists should not appear in a pessimistic prewrite");
      }

      // check if key is exist
      if (write_info.op() == pb::store::Op::Put) {
        response->add_keys_already_exist()->set_key(mutation.key());
      }
    }

    if (BAIDU_UNLIKELY(is_repeated_prewrite)) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] Prewrite,", region->Id())
          << ", key: " << Helper::StringToHex(mutation.key())
          << " is locked by same start_ts, this is a repeated prewrite, skip it, lock_info: "
          << prev_lock_info.ShortDebugString() << ", mutation: " << mutation.ShortDebugString();
      // if enable 1pc need to fallback to 2pc
      try_one_pc = false;
      continue;
    }

    std::string lock_extra_data = lock_extra_datas.find(i) != lock_extra_datas.end() ? lock_extra_datas.at(i) : "";
    int64_t for_update_ts = for_update_ts_checks.find(i) != for_update_ts_checks.end() ? for_update_ts_checks.at(i) : 0;
    int64_t min_commit_ts;
    // 3.1 write data and lock
    auto ret3 = GenPrewriteDataAndLock(region->Id(), mutation, prev_lock_info, write_info, primary_lock, start_ts,
                                       for_update_ts, lock_ttl, txn_size, lock_extra_data, max_commit_ts,
                                       need_check_pessimistic_lock, try_one_pc, kv_puts_data, kv_puts_lock,
                                       locks_for_1pc, min_commit_ts);
    if (!ret3.ok()) {
      if (ret3.error_code() == pb::error::Errno::EVALUE_EMPTY) {
        error->set_errcode(pb::error::Errno::EVALUE_EMPTY);
        error->set_errmsg("value is empty");
        return ret3;
      }
      return ret3;
    }
    if (try_one_pc) {
      if (final_min_commit_ts < min_commit_ts) {
        final_min_commit_ts = min_commit_ts;
      }
    } else {
      final_min_commit_ts = 0;
    }
  }
  if (response->txn_result_size() > 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] Prewrite return txn_result,", region->Id())
        << ", txn_result_size: " << response->txn_result_size() << ", start_ts: " << start_ts
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", mutations_size: " << mutations.size();
    return butil::Status::OK();
  }
  if (try_one_pc) {
    auto ret4 = OnePCommit(raft_engine, ctx, region, start_ts, final_min_commit_ts, locks_for_1pc, kv_puts_data);
    if (!ret4.ok()) {
      return ret4;
    }
    response->set_one_pc_commit_ts(final_min_commit_ts);
    return ret4;
  }
  FallbackTo1PCLocks(kv_puts_lock, locks_for_1pc);
  return DoPreWrite(raft_engine, ctx, region->Id(), start_ts, mutations.size(), kv_puts_data, kv_puts_lock);
}

bvar::LatencyRecorder g_txn_commit_latency("dingo_txn_commit");

butil::Status TxnEngineHelper::Commit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                      std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                      int64_t commit_ts, const std::vector<std::string> &keys) {
  BvarLatencyGuard bvar_guard(&g_txn_commit_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] Commit, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
      << ", keys_size: " << keys.size() << ", keys[0]: " << Helper::StringToHex(keys[0]);

  // print keys and index
  for (int i = 0; i < keys.size(); i++) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] Commit, start_ts: {}", ctx->RegionId(), start_ts)
        << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts << ", keys[" << i
        << "]: " << Helper::StringToHex(keys[i]);
  }

  if (BAIDU_UNLIKELY(keys.size() > FLAGS_max_commit_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
                     << ", keys_size: " << keys.size() << ", keys.size() > FLAGS_max_commit_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "commit keys.size() > FLAGS_max_commit_count");
  }

  if (commit_ts <= start_ts) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit", region->Id())
                     << ", commit_ts is less than start_ts, region_id: " << ctx->RegionId()
                     << ", start_ts: " << start_ts << ", commit_ts: " << commit_ts;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "commit_ts is less than start_ts");
  }

  // create reader and writer
  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit", region->Id())
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  auto *response = dynamic_cast<pb::store::TxnCommitResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_ts: {}, commit_ts: {}", region->Id(), start_ts,
                                    commit_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  std::vector<pb::store::LockInfo> lock_infos;
  for (const auto &key : keys) {
    pb::store::LockInfo lock_info;
    auto ret = txn_reader.GetLockInfo(key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts,
                                      commit_ts)
                       << ", get lock info failed, status: " << ret.error_str();
      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());
      return ret;
    }

    // // if lock is not exist, return TxnNotFound
    // if (lock_info.primary_lock().empty()) {
    //   DINGO_LOG(WARNING) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(),
    //   start_ts,
    //                                     commit_ts)
    //                      << ", txn_not_found with lock_info empty, key: " << Helper::StringToHex(key) << ",
    //                      start_ts: " << start_ts;

    //   auto *txn_not_found = txn_result->mutable_txn_not_found();
    //   txn_not_found->set_start_ts(start_ts);

    //   return butil::Status::OK();
    // }

    // if lock is exists, check if lock_ts is equal to start_ts
    // if not equal, means this key is locked by other txn, return LockInfo
    if (lock_info.lock_ts() != 0) {
      // if lock is exists but start_ts is not equal to lock_ts, return locked
      if (lock_info.lock_ts() != start_ts) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(),
                                          start_ts, commit_ts)
                           << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: "
                           << Helper::StringToHex(key) << ", lock_info: " << lock_info.ShortDebugString();

        *txn_result->mutable_locked() = lock_info;
        return butil::Status::OK();
      } else {
        // lock_ts match start_ts, check if lock_type is Put/Delete/PutIfAbsent
        // If this is a pessimistic lock in Lock phase, return LockInfo
        // If this is not a Put/Delete/PutIfAbsent, return LockInfo
        if (lock_info.lock_type() == pb::store::Op::Lock) {
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(),
                                          start_ts, commit_ts)
                           << ", meet a pessimistic lock, there must BUG in executor, key: " << Helper::StringToHex(key)
                           << ", lock_info: " << lock_info.ShortDebugString();
          *txn_result->mutable_locked() = lock_info;
          return butil::Status::OK();
        } else if (lock_info.lock_type() != pb::store::Op::Put && lock_info.lock_type() != pb::store::Op::Delete &&
                   lock_info.lock_type() != pb::store::Op::PutIfAbsent) {
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(),
                                          start_ts, commit_ts)
                           << ", meet a invalid lock_type, there must BUG in executor, key: "
                           << Helper::StringToHex(key) << ", lock_info: " << lock_info.ShortDebugString();
          *txn_result->mutable_locked() = lock_info;
          return butil::Status::OK();
        }

        // check if the commit_ts is bigger than min_commit_ts, if not, return CommitTsExpired, the executor should
        // get a new tso from coordinator, then commit again.
        if (lock_info.min_commit_ts() > 0 && commit_ts <= lock_info.min_commit_ts()) {
          // the min_commit_ts is already setup and commit_ts is less than min_commit_ts, return CommitTsExpired
          auto *commit_ts_expired = txn_result->mutable_commit_ts_expired();
          commit_ts_expired->set_start_ts(start_ts);
          commit_ts_expired->set_attempted_commit_ts(commit_ts);
          commit_ts_expired->set_key(key);
          commit_ts_expired->set_min_commit_ts(lock_info.min_commit_ts());

          return butil::Status::OK();
        }
      }
    } else {
      // check if the key is already committed, if it is committed can skip it
      pb::store::WriteInfo write_info;
      int64_t prev_commit_ts = 0;
      auto ret2 =
          txn_reader.GetWriteInfo(start_ts, commit_ts, start_ts, key, false, true, true, write_info, prev_commit_ts);
      if (!ret2.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts,
                                        commit_ts)
                         << ", get write info failed, key: " << Helper::StringToHex(key)
                         << ", status: " << ret2.error_str();
        error->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
        error->set_errmsg(ret2.error_str());
        return ret2;
      }

      // if prev_commit_ts == commit_ts, means this key of start_ts is already committed, can skip it
      if (prev_commit_ts == commit_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts, commit_ts)
            << ", key: " << Helper::StringToHex(key) << " is already committed, prev_commit_ts: " << prev_commit_ts;
        continue;
      }

      // check if the key is already rollbacked, if it is rollbacked, return WriteConflict
      // if there is a rollback, there will be a key | start_ts : WriteInfo| in write_cf
      auto ret1 = txn_reader.GetRollbackInfo(start_ts, key, write_info);
      if (!ret1.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts,
                                        commit_ts)
                         << ", get rollback info failed, key: " << Helper::StringToHex(key)
                         << ", start_ts: " << start_ts << ", status: " << ret1.error_str();
      }

      if (write_info.start_ts() == start_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << "find this transaction is rollbacked,return  SelfRolledBack , start_ts: " << start_ts
            << ", write_info: " << write_info.ShortDebugString();

        // prewrite meet write_conflict here
        // add txn_result for response
        // setup write_conflict ( this may not be necessary, when lock_info is set)
        auto *write_conflict = txn_result->mutable_write_conflict();
        write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_SelfRolledBack);
        write_conflict->set_start_ts(start_ts);
        write_conflict->set_conflict_ts(start_ts);
        write_conflict->set_key(key);
        // write_conflict->set_primary_key(lock_info.primary_lock());

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] Commit,", region->Id())
            << ", meet key is rollbacked, return write_conflict, start_ts: " << start_ts
            << ", write_info: " << write_info.ShortDebugString();

        return butil::Status::OK();
      }

      // no committed and no rollbacked, there may be BUG
      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_start_ts(start_ts);
      txn_not_found->set_key(key);

      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts,
                                      commit_ts)
                       << ", no committed and no rollbacked, there may be BUG, key: " << Helper::StringToHex(key)
                       << ", start_ts: " << start_ts << ", write_info: " << write_info.ShortDebugString();

      return butil::Status::OK();
    }

    // now txn is match, prepare to commit
    lock_infos.push_back(lock_info);
  }

  auto ret = DoTxnCommit(raw_engine, raft_engine, ctx, region, lock_infos, start_ts, commit_ts);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit, commit_ts: {}, start_ts: {}", region->Id(), commit_ts,
                                    start_ts)
                     << ", do txn commit failed, status: " << ret.error_str();
    error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    error->set_errmsg(ret.error_str());
    return ret;
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_do_commit_latency("dingo_txn_do_commit");

butil::Status TxnEngineHelper::DoTxnCommit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, store::RegionPtr region,
                                           const std::vector<pb::store::LockInfo> &lock_infos, int64_t start_ts,
                                           int64_t commit_ts) {
  BvarLatencyGuard bvar_guard(&g_txn_do_commit_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {}", region->Id(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
      << ", lock_infos_size: " << lock_infos.size();

  // create reader and writer
  auto reader = raw_engine->Reader();

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  // for vector index region, commit to vector index
  auto *vector_add = cf_put_delete->mutable_vector_add();
  auto *vector_del = cf_put_delete->mutable_vector_del();

  // for document index region, commit to document index
  auto *document_add = cf_put_delete->mutable_document_add();
  auto *document_del = cf_put_delete->mutable_document_del();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  for (const auto &lock_info : lock_infos) {
    // 1.delete lock from lock_cf
    { kv_deletes_lock.push_back(mvcc::Codec::EncodeKey(lock_info.key(), Constant::kLockVer)); }

    if (lock_info.lock_type() == pb::store::Op::PutIfAbsent) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(), start_ts, commit_ts)
          << ", lock_type is PutIfAbsent, skip it, lock_info: " << lock_info.ShortDebugString();
      continue;
    }

    // 2.put data to write_cf
    std::string data_value;
    if (!lock_info.short_value().empty()) {
      data_value = lock_info.short_value();
    } else if (lock_info.lock_type() == pb::store::Put) {
      auto ret = reader->KvGet(Constant::kTxnDataCF, mvcc::Codec::EncodeKey(lock_info.key(), start_ts), data_value);
      if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                        start_ts, commit_ts)
                         << ", get data failed, key: " << lock_info.key() << ", start_ts: " << start_ts
                         << ", status: " << ret.error_str() << ", lock_info: " << lock_info.ShortDebugString();
      }
    }

    {
      pb::common::KeyValue kv;
      std::string write_key = mvcc::Codec::EncodeKey(lock_info.key(), commit_ts);
      kv.set_key(write_key);

      pb::store::WriteInfo write_info;
      write_info.set_start_ts(start_ts);
      write_info.set_op(lock_info.lock_type());
      if (!lock_info.short_value().empty()) {
        write_info.set_short_value(data_value);
      }
      kv.set_value(write_info.SerializeAsString());

      kv_puts_write.push_back(kv);

      if (region->Type() == pb::common::INDEX_REGION &&
          region->Definition().index_parameter().has_vector_index_parameter()) {
        if (lock_info.lock_type() == pb::store::Op::Put) {
          pb::common::VectorWithId vector_with_id;
          auto ret = vector_with_id.ParseFromString(data_value);
          if (!ret) {
            DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                            start_ts, commit_ts)
                             << ", parse vector_with_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", data_value: " << Helper::StringToHex(data_value)
                             << ", lock_info: " << lock_info.ShortDebugString();
            return butil::Status(pb::error::Errno::EINTERNAL, "parse vector_with_id failed");
          }

          *(vector_add->add_vectors()) = vector_with_id;
        } else if (lock_info.lock_type() == pb::store::Op::Delete) {
          auto vector_id = VectorCodec::UnPackageVectorId(lock_info.key());
          if (vector_id == 0) {
            DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                            start_ts, commit_ts)
                             << ", decode vector_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", lock_info: " << lock_info.ShortDebugString();
          }

          vector_del->add_ids(vector_id);
        } else {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, commit_ts)
                           << ", invalid lock_type, key: " << Helper::StringToHex(lock_info.key())
                           << ", lock_info: " << lock_info.ShortDebugString();
        }
      }

      if (region->Type() == pb::common::DOCUMENT_REGION &&
          region->Definition().index_parameter().has_document_index_parameter()) {
        if (lock_info.lock_type() == pb::store::Op::Put) {
          pb::common::DocumentWithId document_with_id;
          auto ret = document_with_id.ParseFromString(data_value);
          if (!ret) {
            DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                            start_ts, commit_ts)
                             << ", parse document_with_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", data_value: " << Helper::StringToHex(data_value)
                             << ", lock_info: " << lock_info.ShortDebugString();
            return butil::Status(pb::error::Errno::EINTERNAL, "parse document_with_id failed");
          }

          *(document_add->add_documents()) = document_with_id;
        } else if (lock_info.lock_type() == pb::store::Op::Delete) {
          auto document_id = DocumentCodec::UnPackageDocumentId(lock_info.key());
          if (document_id == 0) {
            DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                            start_ts, commit_ts)
                             << ", decode document_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", lock_info: " << lock_info.ShortDebugString();
          }

          document_del->add_ids(document_id);
        } else {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                          start_ts, commit_ts)
                           << ", invalid lock_type, key: " << Helper::StringToHex(lock_info.key())
                           << ", lock_info: " << lock_info.ShortDebugString();
        }
      }
    }
  }

  if (kv_puts_write.empty() && kv_deletes_lock.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(), start_ts, commit_ts)
        << ", kv_puts_write is empty and kv_deletes_lock is empty";
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  if (!kv_puts_write.empty()) {
    auto *write_puts = cf_put_delete->add_puts_with_cf();
    write_puts->set_cf_name(Constant::kTxnWriteCF);
    for (auto &kv_put : kv_puts_write) {
      auto *kv = write_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  if (!kv_deletes_lock.empty()) {
    auto *lock_dels = cf_put_delete->add_deletes_with_cf();
    lock_dels->set_cf_name(Constant::kTxnLockCF);
    for (auto &kv_del : kv_deletes_lock) {
      lock_dels->add_keys(kv_del);
    }
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(), start_ts,
                                    commit_ts)
                     << ", write to raft engine failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_check_txn_status_latency("dingo_txn_check_txn_status");

butil::Status TxnEngineHelper::CheckTxnStatus(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                              std::shared_ptr<Context> ctx, const std::string &primary_key,
                                              int64_t lock_ts, int64_t caller_start_ts, int64_t current_ts) {
  BvarLatencyGuard bvar_guard(&g_txn_check_txn_status_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] CheckTxnStatus, primary_key: {}", ctx->RegionId(),
                     Helper::StringToHex(primary_key))
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", lock_ts: " << lock_ts
      << ", caller_start_ts: " << caller_start_ts << ", current_ts: " << current_ts;

  // we need to do if primay_key is in this region'range in service before apply to raft state machine
  // use reader to get if the lock is exists, if lock is exists, check if the lock is expired its ttl, if expired do
  // rollback and return if not expired, return conflict if the lock is not exists, return commited the the lock's
  // ts is matched, but it is not a primary_key, return PrimaryMismatch
  // In the read-write conflict scenario, we may need to update the min_commit_ts of the primary_lock to reduce the
  // read-write confict. The executor will receive MinCommitTSPushed in CheckTxnStatus response, and will add the
  // lock_ts to Get and Scan requests's context, after this, the read-write conflict will be resolved. And the store
  // will bypass the lock conflict for the lock_ts. And if the executor commit lock in a commit_ts smaller than the
  // new min_commit_ts, it will received CommitTsExpired in TxnResultInfo. Executor need to get a new tso from
  // coordinator, then retry the commit.

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  // create reader and writer
  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus", region->Id()) << ", init txn_reader failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  auto *response = dynamic_cast<pb::store::TxnCheckTxnStatusResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus, primary_key: {}", region->Id(), primary_key)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // get lock info
  pb::store::LockInfo lock_info;
  auto ret = txn_reader.GetLockInfo(primary_key, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus, ", region->Id())
                     << ", get lock info failed, primary_key: " << Helper::StringToHex(primary_key)
                     << ", lock_ts: " << lock_ts << ", status: " << ret.error_str();
  }

  // if the lock_ts is matched, we will check if we can update the min_commit_ts. The new commit_ts will be the
  // caller_start_ts.
  if (lock_info.lock_ts() == lock_ts) {
    // the lock is exists, check if it is expired, if not expired, return conflict, if expired, do rollback
    // check if this is a primary key
    if (lock_info.key() != lock_info.primary_lock()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                         << ", primary mismatch, primary_key: " << Helper::StringToHex(primary_key)
                         << ", lock_ts: " << lock_ts << ", lock_info: " << lock_info.ShortDebugString();

      auto *primary_mismatch = txn_result->mutable_primary_mismatch();
      *(primary_mismatch->mutable_lock_info()) = lock_info;
      return butil::Status::OK();
    }

    // for pessimistic lock, we just return lock_info, let executor decide to backoff or unlock
    if (lock_info.lock_type() == pb::store::Op::Lock) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                         << ", pessimistic lock, found locked for executor, primary_key: " << primary_key
                         << ", lock_info: " << lock_info.ShortDebugString() << ", lock_ts: " << lock_ts
                         << ", caller_start_ts: " << caller_start_ts << ", current_ts: " << current_ts;
      ;
      *txn_result->mutable_locked() = lock_info;

      // if the lock is not expired, try to update the min_commit_ts in the next code block, so we cannot return
      // now.

    } else if (lock_info.lock_type() != pb::store::Op::Put && lock_info.lock_type() != pb::store::Op::Delete &&
               lock_info.lock_type() != pb::store::Op::PutIfAbsent) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                       << ", invalid lock_type, key: " << lock_info.key() << ", caller_start_ts: " << caller_start_ts
                       << ", current_ts: " << current_ts << ", lock_info: " << lock_info.ShortDebugString();

      *txn_result->mutable_locked() = lock_info;
      response->set_action(pb::store::Action::NoAction);
      response->set_lock_ttl(lock_info.lock_ttl());

      return butil::Status::OK();
    }

    int64_t current_ms = current_ts >> 18;

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
        << "lock is exists, check ttl, lock_info: " << lock_info.ShortDebugString() << ", current_ms: " << current_ms;

    if (lock_info.lock_ttl() >= current_ms) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
          << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
          << "lock is not expired, return conflict, lock_info: " << lock_info.ShortDebugString()
          << ", current_ms: " << current_ms;

      response->set_lock_ttl(lock_info.lock_ttl());
      response->set_commit_ts(0);

      // check if we need to update the min_commit_ts
      // for valid lock, we can update min_commit_ts to reduce read-write conflict.
      if (lock_info.min_commit_ts() < caller_start_ts) {
        lock_info.set_min_commit_ts(caller_start_ts);

        auto ret = DoUpdateLock(raft_engine, ctx, lock_info);
        if (!ret.ok()) {
          DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                           << ", update lock failed, primary_key: " << Helper::StringToHex(primary_key)
                           << ", lock_ts: " << lock_ts << ", status: " << ret.error_str();
          error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
          error->set_errmsg(ret.error_str());
          return ret;
        }

        response->set_action(pb::store::Action::MinCommitTSPushed);

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
            << ", update min_commit_ts, primary_key: " << Helper::StringToHex(primary_key)
            << ", Action: " << pb::store::Action_Name(pb::store::Action::MinCommitTSPushed);
      } else {
        response->set_action(pb::store::Action::NoAction);

        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
            << ", no need to update min_commit_ts, primary_key: " << Helper::StringToHex(primary_key)
            << ", Action: " << pb::store::Action_Name(pb::store::Action::NoAction);
      }

      return butil::Status::OK();
    }

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
        << "lock is expired, do rollback, lock_info: " << lock_info.ShortDebugString()
        << ", current_ms: " << current_ms;

    // lock is expired, do rollback
    std::vector<std::string> keys_to_rollback_with_data;
    std::vector<std::string> keys_to_rollback_without_data;
    if (lock_info.short_value().empty()) {
      keys_to_rollback_with_data.push_back(primary_key);
    } else {
      keys_to_rollback_without_data.push_back(primary_key);
    }
    DINGO_LOG(INFO) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                    << ", do rollback, primary_key: " << Helper::StringToHex(primary_key) << ", lock_ts: " << lock_ts
                    << ", lock_info: " << lock_info.ShortDebugString();
    auto ret =
        DoRollback(raw_engine, raft_engine, ctx, keys_to_rollback_with_data, keys_to_rollback_without_data, lock_ts);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                       << ", rollback failed, primary_key: " << Helper::StringToHex(primary_key)
                       << ", lock_ts: " << lock_ts << ", status: " << ret.error_str();
      return ret;
    }

    response->set_lock_ttl(0);
    response->set_commit_ts(0);
    response->set_action(::dingodb::pb::store::Action::TTLExpireRollback);
    return butil::Status::OK();
  } else {
    // the lock is not exists, check if it is rollbacked or committed
    // try to get if there is a rollback to lock_ts
    pb::store::WriteInfo write_info;
    auto ret1 = txn_reader.GetRollbackInfo(lock_ts, primary_key, write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus, ", region->Id())
                       << ", get rollback info failed, primary_key: " << Helper::StringToHex(primary_key)
                       << ", lock_ts: " << lock_ts << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == lock_ts) {
      // rollback, return rollback
      response->set_lock_ttl(0);
      response->set_commit_ts(0);
      response->set_action(pb::store::Action::LockNotExistRollback);
      return butil::Status::OK();
    }

    // if there is not a rollback to lock_ts, try to get the commit_ts
    int64_t commit_ts = 0;
    auto ret2 = txn_reader.GetWriteInfo(lock_ts, Constant::kMaxVer, lock_ts, primary_key, false, true, true, write_info,
                                        commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                       << ", get write info failed, primary_key: " << Helper::StringToHex(primary_key)
                       << ", lock_ts: " << lock_ts << ", status: " << ret2.error_str();
    }

    if (commit_ts == 0) {
      // it seems there is a lock previously exists, but it is not committed, and there is no rollback, there must
      // be some error, return TxnNotFound
      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_primary_key(primary_key);
      txn_not_found->set_start_ts(lock_ts);
      DINGO_LOG(ERROR)
          << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
          << ", cannot found the transaction, maybe some error ocurred, return txn_not_found, primary_key: "
          << Helper::StringToHex(primary_key) << ", lock_ts: " << lock_ts
          << ", lock_info: " << lock_info.ShortDebugString();
      return butil::Status::OK();
    }

    // commit, return committed
    response->set_lock_ttl(0);
    response->set_commit_ts(commit_ts);
    response->set_action(pb::store::Action::LockNotExistDoNothing);
    return butil::Status::OK();
  }
}

bvar::LatencyRecorder g_txn_batch_rollback_latency("dingo_txn_batch_rollback");

butil::Status TxnEngineHelper::BatchRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                             std::shared_ptr<Context> ctx, int64_t start_ts,
                                             const std::vector<std::string> &keys) {
  BvarLatencyGuard bvar_guard(&g_txn_batch_rollback_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] BatchRollback, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", keys_size: " << keys.size();

  if (BAIDU_UNLIKELY(keys.size() > FLAGS_max_rollback_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", keys_size: " << keys.size()
                     << ", keys.size() > FLAGS_max_rollback_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "rollback keys.size() > FLAGS_max_rollback_count");
  }

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  // create reader and writer
  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback", region->Id())
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  auto *response = dynamic_cast<pb::store::TxnBatchRollbackResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback, start_ts: {}", region->Id(), start_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  std::vector<std::string> keys_to_rollback;

  // if keys is not empty, we only do resolve lock for these keys
  if (keys.empty()) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] BatchRollback, ", region->Id()) << ", nothing to do";
    return butil::Status::OK();
  }

  std::vector<std::string> keys_to_rollback_with_data;
  std::vector<std::string> keys_to_rollback_without_data;
  for (const auto &key : keys) {
    pb::store::LockInfo lock_info;
    auto ret = txn_reader.GetLockInfo(key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                       << ", get lock info failed, key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts
                       << ", status: " << ret.error_str();
    }

    // if lock is not exist, nothing to do
    if (lock_info.primary_lock().empty()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] BatchRollback", region->Id())
                         << ", txn_not_found with lock_info empty, key: " << Helper::StringToHex(key)
                         << ", start_ts: " << start_ts;

      // auto *txn_not_found = txn_result->mutable_txn_not_found();
      // txn_not_found->set_start_ts(start_ts);
      continue;
    }

    if (lock_info.lock_ts() != start_ts) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                         << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: "
                         << Helper::StringToHex(key) << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();

      // it's not a legal rollback, return lock_info
      auto *locked = txn_result->mutable_locked();
      *locked = lock_info;
      return butil::Status::OK();
    }

    // if the lock is a pessimistic lock, can't do rollback
    if (lock_info.lock_type() == pb::store::Op::Lock) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                       << ", pessimistic lock, can't do rollback, key: " << Helper::StringToHex(key)
                       << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();

      // it's not a legal rollback, return lock_info
      auto *locked = txn_result->mutable_locked();
      *locked = lock_info;
      return butil::Status::OK();
    } else if (lock_info.lock_type() != pb::store::Op::Put && lock_info.lock_type() != pb::store::Op::Delete &&
               lock_info.lock_type() != pb::store::Op::PutIfAbsent) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                       << ", invalid lock_type, key: " << lock_info.key() << ", start_ts: " << start_ts
                       << ", lock_info: " << lock_info.ShortDebugString();
      *txn_result->mutable_locked() = lock_info;
      return butil::Status::OK();
    }

    if (lock_info.short_value().empty()) {
      keys_to_rollback_with_data.push_back(key);
    } else {
      keys_to_rollback_without_data.push_back(key);
    }
  }

  // do rollback
  auto ret =
      DoRollback(raw_engine, raft_engine, ctx, keys_to_rollback_with_data, keys_to_rollback_without_data, start_ts);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                     << ", rollback failed, status: " << ret.error_str();
    return ret;
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_do_rollback_latency("dingo_txn_do_rollback");

// DoRollback
butil::Status TxnEngineHelper::DoRollback(RawEnginePtr /*raw_engine*/, std::shared_ptr<Engine> raft_engine,
                                          std::shared_ptr<Context> ctx,
                                          std::vector<std::string> &keys_to_rollback_with_data,
                                          std::vector<std::string> &keys_to_rollback_without_data, int64_t start_ts) {
  BvarLatencyGuard bvar_guard(&g_txn_do_rollback_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << "[txn]Rollback start_ts: " << start_ts << ", keys_count_with_data: " << keys_to_rollback_with_data.size()
      << ", keys_count_without_data: " << keys_to_rollback_without_data.size();

  if (keys_to_rollback_without_data.empty() && keys_to_rollback_with_data.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << "[txn]Rollback nothing to do, start_ts: " << start_ts;
    return butil::Status::OK();
  }

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;
  std::vector<std::string> kv_deletes_data;

  for (const auto &key : keys_to_rollback_without_data) {
    // delete lock
    kv_deletes_lock.emplace_back(mvcc::Codec::EncodeKey(key, Constant::kLockVer));

    // add write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  for (const auto &key : keys_to_rollback_with_data) {
    // delete lock
    kv_deletes_lock.emplace_back(mvcc::Codec::EncodeKey(key, Constant::kLockVer));

    // delete data
    kv_deletes_data.emplace_back(mvcc::Codec::EncodeKey(key, start_ts));

    // add write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  if (kv_puts_write.empty() && kv_deletes_lock.empty() && kv_deletes_data.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail) << "[txn]Rollback nothing to do, start_ts: " << start_ts;
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  if (!kv_puts_write.empty()) {
    auto *write_puts = cf_put_delete->add_puts_with_cf();
    write_puts->set_cf_name(Constant::kTxnWriteCF);
    for (auto &kv_put : kv_puts_write) {
      auto *kv = write_puts->add_kvs();
      kv->set_key(kv_put.key());
      kv->set_value(kv_put.value());
    }
  }

  if (!kv_deletes_lock.empty()) {
    auto *lock_dels = cf_put_delete->add_deletes_with_cf();
    lock_dels->set_cf_name(Constant::kTxnLockCF);
    for (auto &key_del : kv_deletes_lock) {
      lock_dels->add_keys(key_del);
    }
  }

  if (!kv_deletes_data.empty()) {
    auto *data_dels = cf_put_delete->add_deletes_with_cf();
    data_dels->set_cf_name(Constant::kTxnDataCF);
    for (auto &key_del : kv_deletes_data) {
      data_dels->add_keys(key_del);
    }
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << "[txn]Rollback failed, start_ts: " << start_ts << ", error_code: " << ret.error_code()
                     << ", error_msg: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_resolve_lock_latency("dingo_txn_resolve_lock");

butil::Status TxnEngineHelper::ResolveLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                           const std::vector<std::string> &keys) {
  BvarLatencyGuard bvar_guard(&g_txn_resolve_lock_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] ResolveLock, start_ts: {}", ctx->RegionId(), start_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
      << ", keys_size: " << keys.size();

  if (BAIDU_UNLIKELY(keys.size() > FLAGS_max_resolve_count)) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock, start_ts: {}", ctx->RegionId(), start_ts)
                     << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
                     << ", keys_size: " << keys.size() << ", keys.size() > FLAGS_max_resolve_count";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "resolve keys.size() > FLAGS_max_resolve_count");
  }

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  if (commit_ts < 0) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                     << ", commit_ts < 0, region_id: " << ctx->RegionId() << ", start_ts: " << start_ts
                     << ", commit_ts: " << commit_ts;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "commit_ts < 0");
  }

  if (commit_ts > 0 && commit_ts <= start_ts) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                     << ", commit_ts <= start_ts, region_id: " << ctx->RegionId() << ", start_ts: " << start_ts
                     << ", commit_ts: " << commit_ts;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "commit_ts <= start_ts");
  }

  // if commit_ts = 0, do rollback else do commit
  // scan lock_cf to search if transaction with start_ts is exists, if exists, do rollback or commit
  // if not exists, do nothing
  // create reader and writer
  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  auto *response = dynamic_cast<pb::store::TxnResolveLockResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock, start_ts: {}", region->Id(), start_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  std::vector<std::string> keys_to_rollback;

  std::vector<pb::store::LockInfo> lock_infos_to_commit;
  std::vector<std::string> keys_to_rollback_with_data;
  std::vector<std::string> keys_to_rollback_without_data;

  // if keys is not empty, we only do resolve lock for these keys
  if (!keys.empty()) {
    for (const auto &key : keys) {
      pb::store::LockInfo lock_info;
      auto ret = txn_reader.GetLockInfo(key, lock_info);
      if (!ret.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                         << ", get lock info failed, key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts
                         << ", status: " << ret.error_str();
      }

      // if lock is not exist, nothing to do
      if (lock_info.primary_lock().empty()) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                           << ", txn_not_found with lock_info empty, key: " << Helper::StringToHex(key)
                           << ", start_ts: " << start_ts;
        // txn_result->mutable_txn_not_found()->set_start_ts(start_ts);
        continue;
      }

      if (lock_info.lock_ts() != start_ts) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                           << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: "
                           << Helper::StringToHex(key) << ", start_ts: " << start_ts
                           << ", lock_info: " << lock_info.ShortDebugString();
        // txn_result->mutable_txn_not_found()->set_start_ts(start_ts);
        continue;
      }

      // if the lock is a pessimistic lock, can't do resolvelock
      if (lock_info.lock_type() == pb::store::Op::Lock) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                         << ", pessimistic lock, can't do resolvelock, key: " << Helper::StringToHex(key)
                         << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();
        *txn_result->mutable_locked() = lock_info;
        return butil::Status::OK();
      } else if (lock_info.lock_type() != pb::store::Op::Put && lock_info.lock_type() != pb::store::Op::Delete &&
                 lock_info.lock_type() != pb::store::Op::PutIfAbsent) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                         << ", invalid lock_type, key: " << lock_info.key() << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();
        *txn_result->mutable_locked() = lock_info;
        return butil::Status::OK();
      }

      // prepare to do rollback or commit
      if (commit_ts > 0) {
        // do commit
        lock_infos_to_commit.push_back(lock_info);
      } else {
        // do rollback
        if (lock_info.short_value().empty()) {
          keys_to_rollback_with_data.push_back(key);
        } else {
          keys_to_rollback_without_data.push_back(key);
        }
      }
    }
  }
  // scan for keys to rollback
  else {
    auto stream = Stream::New(FLAGS_stream_message_max_limit_size);
    std::vector<pb::store::LockInfo> tmp_lock_infos;
    bool has_more = false;
    std::string end_key{};
    auto ret = ScanLockInfo(stream, raw_engine, start_ts, start_ts + 1, region->Range(false), 0, tmp_lock_infos,
                            has_more, end_key);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                       << ", get lock info failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    }

    for (const auto &lock_info : tmp_lock_infos) {
      // if the lock is a pessimistic lock, can't do resolvelock
      if (lock_info.lock_type() == pb::store::Op::Lock) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                         << ", pessimistic lock, can't do resolvelock, key: " << lock_info.key()
                         << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();
        *txn_result->mutable_locked() = lock_info;
        return butil::Status::OK();
      } else if (lock_info.lock_type() != pb::store::Op::Put && lock_info.lock_type() != pb::store::Op::Delete &&
                 lock_info.lock_type() != pb::store::Op::PutIfAbsent) {
        DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                         << ", invalid lock_type, key: " << lock_info.key() << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();
        *txn_result->mutable_locked() = lock_info;
        return butil::Status::OK();
      }

      // prepare to do rollback or commit
      const std::string &key = lock_info.key();
      if (commit_ts > 0) {
        // do commit
        lock_infos_to_commit.push_back(lock_info);
      } else {
        if (lock_info.short_value().empty()) {
          keys_to_rollback_with_data.push_back(key);
        } else {
          keys_to_rollback_without_data.push_back(key);
        }
      }
    }  // end while iter
  }    // end scan lock

  if (!lock_infos_to_commit.empty()) {
    auto ret = DoTxnCommit(raw_engine, raft_engine, ctx, region, lock_infos_to_commit, start_ts, commit_ts);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                       << ", do txn commit failed, start_ts: " << start_ts << ", status: " << ret.error_str();
      return ret;
    }
  }

  if (!keys_to_rollback_with_data.empty() || !keys_to_rollback_without_data.empty()) {
    for (auto const &key : keys_to_rollback_with_data) {
      DINGO_LOG(INFO) << fmt::format("[txn][region({})] ResolveLock, ", region->Id()) << "primary key:" << key
                      << ", do rollback with data, start_ts: " << start_ts;
    }
    for (auto const &key : keys_to_rollback_without_data) {
      DINGO_LOG(INFO) << fmt::format("[txn][region({})] ResolveLock, ", region->Id()) << "primary key:" << key
                      << ", do rollback without data, start_ts: " << start_ts;
    }
    auto ret =
        DoRollback(raw_engine, raft_engine, ctx, keys_to_rollback_with_data, keys_to_rollback_without_data, start_ts);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                       << ", rollback failed, start_ts: " << start_ts << ", status: " << ret.error_str();
      return ret;
    }
  }

  return butil::Status::OK();
}

bvar::LatencyRecorder g_txn_hearbeat_latency("dingo_txn_heartbeat");

butil::Status TxnEngineHelper::HeartBeat(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                         std::shared_ptr<Context> ctx, const std::string &primary_lock,
                                         int64_t start_ts, int64_t advise_lock_ttl) {
  BvarLatencyGuard bvar_guard(&g_txn_hearbeat_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", ctx->RegionId(),
                     Helper::StringToHex(primary_lock))
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", start_ts: " << start_ts
      << ", advise_lock_ttl: " << advise_lock_ttl;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  auto *response = dynamic_cast<pb::store::TxnHeartBeatResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(),
                                    Helper::StringToHex(primary_lock))
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  TxnReader txn_reader(raw_engine);
  auto ret_init = txn_reader.Init();
  if (!ret_init.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat", region->Id())
                     << ", init txn_reader failed, status: " << ret_init.error_str();
    return butil::Status(pb::error::Errno::EINTERNAL, "init txn_reader failed");
  }

  pb::store::LockInfo lock_info;
  auto ret = txn_reader.GetLockInfo(primary_lock, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(),
                                    Helper::StringToHex(primary_lock))
                     << ", get lock info failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    return ret;
  }

  if (lock_info.primary_lock().empty()) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(),
                                      Helper::StringToHex(primary_lock))
                       << ", txn_not_found with lock_info empty, primary_lock: " << Helper::StringToHex(primary_lock)
                       << ", start_ts: " << start_ts;

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
    return butil::Status::OK();
  }

  if (lock_info.lock_ts() != start_ts) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(),
                                      Helper::StringToHex(primary_lock))
                       << ", txn_not_found with lock_info.lock_ts not equal to start_ts, primary_lock: "
                       << Helper::StringToHex(primary_lock) << ", start_ts: " << start_ts
                       << ", lock_info: " << lock_info.ShortDebugString();

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
    return butil::Status::OK();
  }

  if (lock_info.lock_ttl() >= advise_lock_ttl) {
    DINGO_LOG(WARNING)
        << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(), Helper::StringToHex(primary_lock))
        << ", lock_info.lock_ttl greater than advise_lock_ttl, no need to update lock_ttl, lock_info.lock_ttl: "
        << lock_info.lock_ttl() << ", advise_lock_ttl: " << advise_lock_ttl
        << ", lock_info: " << lock_info.ShortDebugString();
    return butil::Status::OK();
  }

  // update lock_info
  lock_info.set_lock_ttl(advise_lock_ttl);

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *lock_puts = cf_put_delete->add_puts_with_cf();
  lock_puts->set_cf_name(Constant::kTxnLockCF);
  auto *kv_lock = lock_puts->add_kvs();
  kv_lock->set_key(mvcc::Codec::EncodeKey(primary_lock, Constant::kLockVer));
  kv_lock->set_value(lock_info.SerializeAsString());

  if (txn_raft_request.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }

  ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(),
                                    Helper::StringToHex(primary_lock))
                     << ", write failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

bvar::LatencyRecorder g_txn_delete_range_latency("dingo_txn_delete_range");

butil::Status TxnEngineHelper::DeleteRange(RawEnginePtr /*raw_engine*/, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, const std::string &start_key,
                                           const std::string &end_key) {
  BvarLatencyGuard bvar_guard(&g_txn_delete_range_latency);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] DeleteRange, start_key: {}", ctx->RegionId(), Helper::StringToHex(start_key))
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", end_key: " << end_key;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DeleteRange", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  auto *response = dynamic_cast<pb::store::TxnDeleteRangeResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DeleteRange, start_key: {}", region->Id(),
                                    Helper::StringToHex(start_key))
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *delete_range_request = txn_raft_request.mutable_mvcc_delete_range();
  delete_range_request->set_start_key(start_key);
  delete_range_request->set_end_key(end_key);

  if (txn_raft_request.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }

  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DeleteRange, start_key: {}", region->Id(),
                                    Helper::StringToHex(start_key))
                     << ", write failed, status: " << ret.error_str();
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, ret.error_str());
  }

  return ret;
}

butil::Status TxnEngineHelper::Gc(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                  std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
      << fmt::format("[txn_gc][region({})] Gc, safe_point_ts: {}", ctx->RegionId(), safe_point_ts)
      << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString();

  store::RegionPtr region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn_gc][region({})] Gc", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region is not found");
  }

  auto *response = dynamic_cast<pb::store::TxnGcResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn_gc][region({})] Gc, safe_point_ts: {}", region->Id(), safe_point_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }

  [[maybe_unused]] auto *error = response->mutable_error();

  std::shared_ptr<StoreMetaManager> store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  std::shared_ptr<GCSafePointManager> gc_safe_point_manager = store_meta_manager->GetGCSafePointManager();

  auto range = region->Range(false);

  auto definition = region->Definition();
  int64_t tenant_id = definition.tenant_id();

  auto gc_safe_point = gc_safe_point_manager->FindSafePoint(tenant_id);

  return DoGc(raw_engine, raft_engine, ctx, safe_point_ts, gc_safe_point, range.start_key(), range.end_key());
}

butil::Status TxnEngineHelper::DoGc(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                    std::shared_ptr<Context> ctx, int64_t safe_point_ts,
                                    std::shared_ptr<GCSafePoint> gc_safe_point, const std::string &region_start_key,
                                    const std::string &region_end_key) {
  store::RegionPtr region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    std::string s = fmt::format("[txn_gc][tenant({})][region({})]  region not found.", gc_safe_point->GetTenantId(),
                                ctx->RegionId());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, s);
  }

  pb::common::RegionType type = region->Type();

  bool is_txn = region->IsTxn();

  if (type == pb::common::RegionType::STORE_REGION && is_txn) {
    return DoGcForStoreTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                           region_end_key);
  } else if (type == pb::common::RegionType::STORE_REGION && !is_txn) {
    return DoGcForStoreNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                              region_end_key);
  } else if (type == pb::common::RegionType::INDEX_REGION && is_txn) {
    return DoGcForIndexTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                           region_end_key);
  } else if (type == pb::common::RegionType::INDEX_REGION && !is_txn) {
    return DoGcForIndexNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                              region_end_key);
  } else if (type == pb::common::RegionType::DOCUMENT_REGION && is_txn) {
    return DoGcForDocumentTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                              region_end_key);
  } else if (type == pb::common::RegionType::DOCUMENT_REGION && !is_txn) {
    return DoGcForDocumentNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                                 region_end_key);
  }

  std::string s = fmt::format(
      "[txn_gc][tenant({})][region({})][type({})] Gc invalid region type and txn,  safe_point_ts: {} txn : {}",
      gc_safe_point->GetTenantId(), region->Id(), pb::common::RegionType_Name(type), safe_point_ts,
      (is_txn ? "true" : "false"));
  DINGO_LOG(ERROR) << s;
  return butil::Status(pb::error::Errno::ENOT_SUPPORT, s);
}

butil::Status TxnEngineHelper::DoGcCoreTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                           int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                           const std::string &region_start_key, const std::string &region_end_key) {
  int64_t start_time_ms = Helper::TimestampMs();
  int64_t end_time_ms = 0;
  int64_t total_delete_count = 0;
  int64_t total_iter_count = 0;
  int64_t region_id = ctx->RegionId();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
      "[txn_gc][statics][tenant({})][region({})][type({})][txn] start region start_key: {} end_key: {} "
      "safe_point_ts : {}",
      gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), Helper::StringToHex(region_start_key),
      Helper::StringToHex(region_end_key), safe_point_ts);

  std::vector<std::string> kv_deletes_lock;
  std::vector<std::string> kv_deletes_data;
  std::vector<std::string> kv_deletes_write;

  std::string lock_start_key;
  std::string lock_end_key;

  std::string last_lock_start_key;
  std::string last_lock_end_key;

  RawEngine::ReaderPtr reader = raw_engine->Reader();
  std::shared_ptr<Snapshot> snapshot = raw_engine->GetSnapshot();

  IteratorOptions write_iter_options;
  write_iter_options.lower_bound = mvcc::Codec::EncodeKey(region_start_key, Constant::kMaxVer);
  write_iter_options.upper_bound = mvcc::Codec::EncodeKey(region_end_key, Constant::kMaxVer);

  auto write_iter = reader->NewIterator(Constant::kTxnWriteCF, snapshot, write_iter_options);
  if (nullptr == write_iter) {
    std::string s = fmt::format(
        "[txn_gc][write][tenant({})][region({})][type({})][txn] NewIterator failed.  start_key: {} end_key: {} ",
        gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
        Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  write_iter->Seek(write_iter_options.lower_bound);

  // this var is too long. this is important var. do not cut down it.
  bool is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts = false;
  // this var is too long. this is important var. do not cut down it.
  bool is_first_put_key_if_write_ts_le_safe_point_ts = true;

  std::string write_key;
  std::string last_write_key;

  while (write_iter->Valid()) {
    total_iter_count++;
    std::string_view write_iter_key = write_iter->Key();
    std::string_view write_iter_value = write_iter->Value();

    int64_t write_ts = 0;

    auto ret = mvcc::Codec::DecodeKey(write_iter_key, write_key, write_ts);
    if (!ret) {
      std::string s = fmt::format(
          "[txn_gc][write][tenant({})][region({})][type({})][txn] decode txn key failed, write_iter->key : {} ",
          gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
          Helper::StringToHex(write_iter_key));
      DINGO_LOG(FATAL) << s;
    }

    pb::store::WriteInfo write_info;
    bool parse_success = write_info.ParseFromArray(write_iter_value.data(), write_iter_value.size());
    if (!parse_success) {
      std::string s = fmt::format(
          "[txn_gc][write][tenant({})][region({})][type({})][txn] ParseFromArray failed, write_iter->key : {}  "
          "write_key : {}  write_ts : {}, write_iter->value : {} ",
          gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
          Helper::StringToHex(write_iter_key), Helper::StringToHex(write_key), write_ts,
          Helper::StringToHex(write_iter_value));
      DINGO_LOG(FATAL) << s;
    }

    // reset for first put
    if (last_write_key != write_key) {
      is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts = false;
      last_write_key = write_key;
      is_first_put_key_if_write_ts_le_safe_point_ts = true;
    }

    // update lock_start_key
    if (lock_start_key != write_key) {
      if (lock_start_key == last_lock_start_key || lock_start_key.empty()) {
        lock_start_key = write_key;
      }
    }

    pb::store::Op op = write_info.op();

    // write_ts > safe_point_ts key value
    if (write_ts > safe_point_ts) {
      if (!is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts) {
        if (pb::store::Op::Put == op || pb::store::Op::Delete == op) {
          is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts = true;
        }
      }
      write_iter->Next();
      continue;
    }

    // handle write_ts <= safe_point_ts key value
    switch (op) {
      case pb::store::Put: {
        // caution!!!
        // if write_ts > safe_point_ts. not exist delete or put key.
        // if write_ts <= safe_point_ts. first is put key. this first put key can not be delete !!!
        if (!is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts) {
          if (is_first_put_key_if_write_ts_le_safe_point_ts) {
            // others put key can be delete. clear this flag.
            is_first_put_key_if_write_ts_le_safe_point_ts = false;
            write_iter->Next();
            continue;
          }
        }
        kv_deletes_write.emplace_back(write_iter_key);
        if (write_info.short_value().empty()) {
          // try get key from data column family. if not exist , do not delete.
          std::string data_key = mvcc::Codec::EncodeKey(write_key, write_info.start_ts());
          std::string data_value;
          auto status = reader->KvGet(Constant::kTxnDataCF, snapshot, data_key, data_value);
          if (status.ok()) {
            kv_deletes_data.emplace_back(data_key);
          } else {
            if (pb::error::Errno::EKEY_NOT_FOUND == status.error_code()) {
              std::string s = fmt::format(
                  "[txn_gc][data][tenant({})][region({})][type({})][txn] key not found.  maybe key already deleted "
                  "or txn error. ignore. key: {} raw_key : {}  start_ts : {} status : {} write_info: {}",
                  gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
                  Helper::StringToHex(data_key), Helper::StringToHex(write_key), write_info.start_ts(),
                  status.error_cstr(), write_info.ShortDebugString());
              DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << s;
            } else {  // other error
              std::string s = fmt::format(
                  "[txn_gc][data][tenant({})][region({})][type({})][txn] get key failed. key: {} raw_key : {}  "
                  "start_ts : {} status : {} write_info: {}",
                  gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
                  Helper::StringToHex(data_key), Helper::StringToHex(write_key), write_info.start_ts(),
                  status.error_cstr(), write_info.ShortDebugString());
              DINGO_LOG(ERROR) << s;
            }
          }
        }
        break;
      }
      case pb::store::Delete: {
        // caution!!!
        // if write_ts > safe_point_ts. not exist delete or put key.
        // if write_ts <= safe_point_ts. first is delete key. this first put key actually can be delete.
        if (!is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts) {
          if (is_first_put_key_if_write_ts_le_safe_point_ts) {
            is_first_put_key_if_write_ts_le_safe_point_ts = false;
          }
        }
        kv_deletes_write.emplace_back(write_iter_key);
        break;
      }

      case pb::store::Rollback: {
        kv_deletes_write.emplace_back(write_iter_key);
        break;
      }
      case pb::store::Lock:
        [[fallthrough]];
      case pb::store::PutIfAbsent:
        [[fallthrough]];
      case pb::store::None:
        [[fallthrough]];
      case pb::store::Op_INT_MIN_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      case pb::store::Op_INT_MAX_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      default: {
        std::string s = fmt::format(
            "[txn_gc][write][tenant({})][region({})][type({})][txn] invalid pb::store::Op type : {} type string : "
            "{} , write_iter->key : {}  write_key : {}  write_ts : {} write_iter->value : {} ignore. ",
            gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), static_cast<int>(op),
            pb::store::Op_Name(op), Helper::StringToHex(write_iter_key), Helper::StringToHex(write_key), write_ts,
            write_info.ShortDebugString());
        DINGO_LOG(ERROR) << s;
        break;
      }
    }

    if ((kv_deletes_lock.size() + kv_deletes_write.size() + kv_deletes_data.size()) >= FLAGS_gc_delete_batch_count) {
      auto [internal_gc_stop, internal_safe_point_ts] = gc_safe_point->GetGcFlagAndSafePointTs();
      if (internal_gc_stop) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[txn_gc][tenant({})][region({})][type({})][txn] gc_stop set stop.  start_key : {} end_key : {}. return",
            gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type),
            Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
        goto _interrupt1;
      }

      if (safe_point_ts < internal_safe_point_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[txn_gc][tenant({})][region({})][type({})][txn] current safe_point_ts : {}. newest safe_point_ts : {}. "
            "Don't worry, we'll deal with it next time. ignore.  start_key : {} end_key : {}",
            gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type), safe_point_ts,
            internal_safe_point_ts, Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
      }

      total_delete_count += (kv_deletes_lock.size() + kv_deletes_write.size() + kv_deletes_data.size());
      DoFinalWorkForTxnGc(raft_engine, ctx, reader, snapshot, write_key, gc_safe_point->GetTenantId(), type,
                          safe_point_ts, kv_deletes_lock, kv_deletes_data, kv_deletes_write, lock_start_key,
                          lock_end_key, last_lock_start_key, last_lock_end_key);
    }
    write_iter->Next();
  }

_interrupt1:

  auto [internal_gc_stop, internal_safe_point_ts] = gc_safe_point->GetGcFlagAndSafePointTs();
  if (internal_gc_stop) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
        << fmt::format("[txn_gc][tenant({})][region({})][type({})][txn] set internal_gc_stop stop, return",
                       gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type));
    goto _interrupt2;
  }

  if (safe_point_ts < internal_safe_point_ts) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
        "[txn_gc][tenant({})][region({})][type({})][txn] current safe_point_ts : {}. newest safe_point_ts : {}. "
        "Don't worry, we'll deal with it next time. ignore.",
        gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type), safe_point_ts,
        internal_safe_point_ts);
  }

  total_delete_count += (kv_deletes_lock.size() + kv_deletes_write.size() + kv_deletes_data.size());
  DoFinalWorkForTxnGc(raft_engine, ctx, reader, snapshot, write_key, gc_safe_point->GetTenantId(), type, safe_point_ts,
                      kv_deletes_lock, kv_deletes_data, kv_deletes_write, lock_start_key, lock_end_key,
                      last_lock_start_key, last_lock_end_key);

_interrupt2:
  end_time_ms = Helper::TimestampMs();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
      "[txn_gc][statics][tenant({})][region({})][type({})][txn] end region start_key: {} end_key: {} safe_point_ts "
      ": {} time consuming : {} ms total_delete_count : {} total_iter_count : {} ",
      gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), Helper::StringToHex(region_start_key),
      Helper::StringToHex(region_end_key), safe_point_ts, (end_time_ms - start_time_ms), total_delete_count,
      total_iter_count);

  return butil::Status();
}

butil::Status TxnEngineHelper::DoGcCoreNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                              std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                              int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                              const std::string &region_start_key, const std::string &region_end_key) {
  store::RegionPtr region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    std::string s = fmt::format("[txn_gc][tenant({})][region({})][nontxn]  region not found.",
                                gc_safe_point->GetTenantId(), ctx->RegionId());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, s);
  }

  int64_t start_time_ms = Helper::TimestampMs();
  int64_t end_time_ms = 0;
  int64_t total_delete_count = 0;
  int64_t total_iter_count = 0;
  int64_t region_id = ctx->RegionId();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
      "[txn_gc][statics][tenant({})][region({})][type({})][nontxn] start region start_key: {} end_key: {} "
      "safe_point_ts : {}",
      gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), Helper::StringToHex(region_start_key),
      Helper::StringToHex(region_end_key), safe_point_ts);

  std::vector<std::string> kv_deletes_default;
  std::vector<std::string> kv_deletes_scalar;
  std::vector<std::string> kv_deletes_table;
  std::vector<std::string> kv_deletes_scalar_speedup;

  RawEngine::ReaderPtr reader = raw_engine->Reader();
  std::shared_ptr<Snapshot> snapshot = raw_engine->GetSnapshot();

  IteratorOptions default_iter_options;
  default_iter_options.lower_bound = mvcc::Codec::EncodeKey(region_start_key, Constant::kMaxVer);
  default_iter_options.upper_bound = mvcc::Codec::EncodeKey(region_end_key, Constant::kMaxVer);

  const std::string &cf_name =
      (type == pb::common::RegionType::INDEX_REGION ? Constant::kVectorDataCF : Constant::kStoreDataCF);

  auto default_iter = reader->NewIterator(cf_name, snapshot, default_iter_options);
  if (nullptr == default_iter) {
    std::string s = fmt::format(
        "[txn_gc][write][tenant({})][region({})][type({})][nontxn] NewIterator failed.  start_key: {} end_key: {} ",
        gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
        Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  default_iter->Seek(default_iter_options.lower_bound);

  // this var is too long. this is important var. do not cut down it.
  bool is_exist_default_key_if_ts_gt_safe_point_ts = false;
  // this var is too long. this is important var. do not cut down it.
  bool is_first_put_key_if_ts_le_safe_point_ts = true;

  std::string default_key;
  std::string last_default_key;

  char prefix = '\0';
  int64_t region_part_id = 0;
  if (type == pb::common::RegionType::INDEX_REGION) {
    prefix = region->GetKeyPrefix();
    region_part_id = region->PartitionId();
  }

  auto lambda_emplace_back_function = [&region, &prefix, &region_part_id, &kv_deletes_default, &kv_deletes_scalar,
                                       &kv_deletes_table, &kv_deletes_scalar_speedup](
                                          pb::common::RegionType type, std::string_view default_iter_key,
                                          int64_t vector_id, int64_t default_ts) {
    kv_deletes_default.emplace_back(std::string(default_iter_key));
    if (type == pb::common::RegionType::INDEX_REGION) {
      kv_deletes_scalar.emplace_back(std::string(default_iter_key));
      kv_deletes_table.emplace_back(std::string(default_iter_key));
      pb::common::ScalarSchema scalar_schema = region->ScalarSchema();
      for (const auto &fields : scalar_schema.fields()) {
        if (fields.enable_speed_up()) {
          std::string encode_key_with_ts =
              VectorCodec::EncodeVectorKey(prefix, region_part_id, vector_id, fields.key(), default_ts);
          kv_deletes_scalar_speedup.emplace_back(std::move(encode_key_with_ts));
        }
      }
    }
  };

  while (default_iter->Valid()) {
    total_iter_count++;
    std::string_view default_iter_key = default_iter->Key();
    std::string_view default_iter_value = default_iter->Value();
    int64_t default_ts = 0;

    auto ret = mvcc::Codec::DecodeKey(default_iter_key, default_key, default_ts);
    if (!ret) {
      std::string s =
          fmt::format("[txn_gc][decode][tenant({})][region({})][type({})][nontxn] decode key failed, key : {} ",
                      gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
                      Helper::StringToHex(default_iter_key));
      DINGO_LOG(FATAL) << s;
    }

    int64_t vector_id = 0;
    int64_t partition_id = 0;

    if (type == pb::common::RegionType::INDEX_REGION) {
      VectorCodec::DecodeFromEncodeKeyWithTs(std::string(default_iter_key), partition_id, vector_id);
      if (region_part_id != partition_id) {
        std::string s = fmt::format(
            "[txn_gc][decode][tenant({})][region({})][type({})][nontxn] decode  region_part_id({}) != "
            "partition_id({}), "
            "key : {} ",
            gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), region_part_id, partition_id,
            Helper::StringToHex(default_iter_key));
        DINGO_LOG(FATAL) << s;
      }
    }

    if (last_default_key != default_key) {
      last_default_key = default_key;
      is_exist_default_key_if_ts_gt_safe_point_ts = false;
      is_first_put_key_if_ts_le_safe_point_ts = true;

      if (default_ts > safe_point_ts) {
        is_exist_default_key_if_ts_gt_safe_point_ts = true;
        is_first_put_key_if_ts_le_safe_point_ts = false;
      }
    }

    if (default_ts > safe_point_ts) {
      default_iter->Next();
      continue;
    }

    mvcc::ValueFlag value_flag = mvcc::Codec::GetValueFlag(default_iter_value);
    switch (value_flag) {
      case mvcc::ValueFlag::kPut: {
        if (!is_exist_default_key_if_ts_gt_safe_point_ts) {
          if (is_first_put_key_if_ts_le_safe_point_ts) {
            is_first_put_key_if_ts_le_safe_point_ts = false;
            is_exist_default_key_if_ts_gt_safe_point_ts = true;
            default_iter->Next();
            continue;
          }
        }

        lambda_emplace_back_function(type, default_iter_key, vector_id, default_ts);
        break;
      }
      case mvcc::ValueFlag::kPutTTL: {
        // note: The first occurrence of the expired key has been deleted.
        if (!is_exist_default_key_if_ts_gt_safe_point_ts) {
          if (is_first_put_key_if_ts_le_safe_point_ts) {
            is_first_put_key_if_ts_le_safe_point_ts = false;
            is_exist_default_key_if_ts_gt_safe_point_ts = true;
            int64_t ttl = mvcc::Codec::GetValueTTL(default_iter_value);
            if (start_time_ms <= ttl) {
              // ttl not expired
              default_iter->Next();
              continue;
            }
          }
        }
        lambda_emplace_back_function(type, default_iter_key, vector_id, default_ts);
        break;
      }

      case mvcc::ValueFlag::kDelete: {
        if (!is_exist_default_key_if_ts_gt_safe_point_ts) {
          if (is_first_put_key_if_ts_le_safe_point_ts) {
            is_first_put_key_if_ts_le_safe_point_ts = false;
            is_exist_default_key_if_ts_gt_safe_point_ts = true;
          }
        }
        lambda_emplace_back_function(type, default_iter_key, vector_id, default_ts);
        break;
      }

      default: {
        std::string s =
            fmt::format("[txn_gc][tenant({})][region({})][type({})][nontxn] invalid mvcc value flag : {}, value : {} ",
                        gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type),
                        static_cast<int>(value_flag), Helper::StringToHex(default_iter_value));
        DINGO_LOG(FATAL) << s;
      }
    }

    if ((kv_deletes_default.size() + kv_deletes_scalar.size() + kv_deletes_table.size() +
         kv_deletes_scalar_speedup.size()) >= FLAGS_gc_delete_batch_count) {
      auto [internal_gc_stop, internal_safe_point_ts] = gc_safe_point->GetGcFlagAndSafePointTs();
      if (internal_gc_stop) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[txn_gc][tenant({})][region({})][type({})][nontxn] gc_stop set stop.  start_key : {} end_key : {}. return",
            gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type),
            Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
        goto _interrupt1;
      }

      if (safe_point_ts < internal_safe_point_ts) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[txn_gc][tenant({})][region({})][type({})][nontxn] current safe_point_ts : {}. newest safe_point_ts : {}. "
            "Don't worry, we'll deal with it next time. ignore.  start_key : {} end_key : {}",
            gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type), safe_point_ts,
            internal_safe_point_ts, Helper::StringToHex(region_start_key), Helper::StringToHex(region_end_key));
      }

      total_delete_count += kv_deletes_default.size() + kv_deletes_scalar.size() + kv_deletes_table.size() +
                            kv_deletes_scalar_speedup.size();
      DoFinalWorkForNonTxnGc(raft_engine, ctx, gc_safe_point->GetTenantId(), type, kv_deletes_default,
                             kv_deletes_scalar, kv_deletes_table, kv_deletes_scalar_speedup);
    }

    default_iter->Next();
  }

_interrupt1:

  auto [internal_gc_stop, internal_safe_point_ts] = gc_safe_point->GetGcFlagAndSafePointTs();
  if (internal_gc_stop) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
        << fmt::format("[txn_gc][tenant({})][region({})][type({})][nontxn] set internal_gc_stop stop, return",
                       gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type));

    goto _interrupt2;
  }

  if (safe_point_ts < internal_safe_point_ts) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
        "[txn_gc][tenant({})][region({})][type({})][nontxn] current safe_point_ts : {}. newest safe_point_ts : {}. "
        "Don't worry, we'll deal with it next time. ignore.",
        gc_safe_point->GetTenantId(), ctx->RegionId(), pb::common::RegionType_Name(type), safe_point_ts,
        internal_safe_point_ts);
  }

  total_delete_count +=
      kv_deletes_default.size() + kv_deletes_scalar.size() + kv_deletes_table.size() + kv_deletes_scalar_speedup.size();
  DoFinalWorkForNonTxnGc(raft_engine, ctx, gc_safe_point->GetTenantId(), type, kv_deletes_default, kv_deletes_scalar,
                         kv_deletes_table, kv_deletes_scalar_speedup);

_interrupt2:
  end_time_ms = Helper::TimestampMs();

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
      "[txn_gc][statics][tenant({})][region({})][type({})][nontxn] end region start_key: {} end_key: {} "
      "safe_point_ts "
      ": {} time  consuming : {} ms total_delete_count : {} total_iter_count : {}",
      gc_safe_point->GetTenantId(), region_id, pb::common::RegionType_Name(type), Helper::StringToHex(region_start_key),
      Helper::StringToHex(region_end_key), safe_point_ts, (end_time_ms - start_time_ms), total_delete_count,
      total_iter_count);

  return butil::Status();
}

butil::Status TxnEngineHelper::DoGcForStoreTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                               std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                               int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                               const std::string &region_start_key, const std::string &region_end_key) {
  return DoGcCoreTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                     region_end_key);
}

butil::Status TxnEngineHelper::DoGcForStoreNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                  std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                                  int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                                  const std::string &region_start_key,
                                                  const std::string &region_end_key) {
  return DoGcCoreNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                        region_end_key);
}

butil::Status TxnEngineHelper::DoGcForIndexTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                               std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                               int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                               const std::string &region_start_key, const std::string &region_end_key) {
  return DoGcCoreTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                     region_end_key);
}
butil::Status TxnEngineHelper::DoGcForIndexNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                  std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                                  int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                                  const std::string &region_start_key,
                                                  const std::string &region_end_key) {
  return DoGcCoreNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                        region_end_key);
}

butil::Status TxnEngineHelper::DoGcForDocumentTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                  std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                                  int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                                  const std::string &region_start_key,
                                                  const std::string &region_end_key) {
  return DoGcCoreTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                     region_end_key);
}
butil::Status TxnEngineHelper::DoGcForDocumentNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                     std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                                     int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                                     const std::string &region_start_key,
                                                     const std::string &region_end_key) {
  return DoGcCoreNonTxn(raw_engine, raft_engine, ctx, type, safe_point_ts, gc_safe_point, region_start_key,
                        region_end_key);
}

bvar::LatencyRecorder g_txn_check_lock_for_gc_latency("dingo_txn_check_lock_for_gc");

butil::Status TxnEngineHelper::CheckLockForTxnGc(RawEngine::ReaderPtr reader, std::shared_ptr<Snapshot> snapshot,
                                                 const std::string &start_key, const std::string &end_key,
                                                 int64_t safe_point_ts, int64_t region_id, int64_t tenant_id,
                                                 pb::common::RegionType type) {
  BvarLatencyGuard bvar_guard(&g_txn_check_lock_for_gc_latency);

  auto lambda_get_raw_key_function = [](std::string_view lock_iter_key) {
    std::string lock_key;
    int64_t ts = 0;
    mvcc::Codec::DecodeKey(lock_iter_key, lock_key, ts);
    return lock_key;
  };

  int64_t total_count = 0;
  int64_t failed_count = 0;

  auto lambda_add_error_statics_function = [&total_count, &failed_count]() {
    failed_count++;
    total_count++;
  };

  IteratorOptions lock_iter_options;
  lock_iter_options.lower_bound = mvcc::Codec::EncodeKey(start_key, Constant::kLockVer);
  lock_iter_options.upper_bound = mvcc::Codec::EncodeKey(end_key, 0);

  std::shared_ptr<dingodb::Iterator> lock_iter = reader->NewIterator(Constant::kTxnLockCF, snapshot, lock_iter_options);
  if (nullptr == lock_iter) {
    std::string s = fmt::format(
        "[txn_gc][lock][tenant({})][region({})][type({})][txn] NewIterator failed, start_key: {} end_key : {} "
        "safe_point_ts : {}",
        tenant_id, region_id, pb::common::RegionType_Name(type), Helper::StringToHex(start_key),
        Helper::StringToHex(end_key), safe_point_ts);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  lock_iter->Seek(lock_iter_options.lower_bound);

  while (lock_iter->Valid()) {
    std::string_view lock_iter_key = lock_iter->Key();
    std::string_view lock_iter_value = lock_iter->Value();

    if (lock_iter_value.empty()) {
      std::string s = fmt::format(
          "[txn_gc][lock][tenant({})][region({})][type({})][txn] lock info empty. lock_iter_key: {} lock_key : {} "
          "safe_point_ts : {}",
          tenant_id, region_id, pb::common::RegionType_Name(type), Helper::StringToHex(lock_iter_key),
          Helper::StringToHex(lambda_get_raw_key_function(lock_iter_key)), safe_point_ts);
      DINGO_LOG(ERROR) << s;
      lambda_add_error_statics_function();
      lock_iter->Next();
      continue;
    }

    pb::store::LockInfo lock_info;
    bool parse_success = lock_info.ParseFromString(std::string(lock_iter_value));
    if (!parse_success) {
      std::string s = fmt::format(
          "[txn_gc][lock][tenant({})][region({})][type({})][txn] parse lock info failed, lock_iter_key : {} lock_key: "
          "{} , lock_value : {} safe_point_ts : {}",
          tenant_id, region_id, pb::common::RegionType_Name(type), Helper::StringToHex(lock_iter_key),
          Helper::StringToHex(lambda_get_raw_key_function(lock_iter_key)), Helper::StringToHex(lock_iter_value),
          safe_point_ts);
      DINGO_LOG(ERROR) << s;
      lambda_add_error_statics_function();
      lock_iter->Next();
      continue;
    }

    int64_t lock_ts = lock_info.lock_ts();
    if (lock_ts <= safe_point_ts) {
      std::string s = fmt::format(
          "[txn_gc][lock][tenant({})][region({})][type({})][txn] find lock error. exist lock_ts : {} <= safe_point_ts "
          ": {}, lock_iter_key : {} lock_key: {}  , safe_point_ts : {} lock_value : {}",
          tenant_id, region_id, pb::common::RegionType_Name(type), lock_ts, safe_point_ts,
          Helper::StringToHex(lock_iter_key), Helper::StringToHex(lambda_get_raw_key_function(lock_iter_key)),
          safe_point_ts, lock_info.ShortDebugString());
      DINGO_LOG(ERROR) << s;
      lambda_add_error_statics_function();
      lock_iter->Next();
      continue;
    }

    lock_iter->Next();
    total_count++;
  }

  std::string s = fmt::format(
      "[txn_gc][lock][tenant({})][region({})][type({})][txn] scan lock column family. start_key : {} end_key : {} "
      "safe_point_ts : {} total : {} success : {} failed : {}",
      tenant_id, region_id, pb::common::RegionType_Name(type), Helper::StringToHex(start_key),
      Helper::StringToHex(end_key), safe_point_ts, total_count, (total_count - failed_count), failed_count);
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << s;

  return butil::Status();
}

bvar::LatencyRecorder g_txn_raft_engine_write_for_gc_latency("dingo_txn_raft_engine_write_for_gc");

butil::Status TxnEngineHelper::RaftEngineWriteForTxnGc(std::shared_ptr<Engine> raft_engine,
                                                       std::shared_ptr<Context> ctx,
                                                       const std::vector<std::string> &kv_deletes_lock,
                                                       const std::vector<std::string> &kv_deletes_data,
                                                       const std::vector<std::string> &kv_deletes_write,
                                                       int64_t tenant_id, pb::common::RegionType type) {
  BvarLatencyGuard bvar_guard(&g_txn_raft_engine_write_for_gc_latency);

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  pb::raft::DeletesWithCf *write_dels = nullptr;
  if (!kv_deletes_write.empty()) {
    write_dels = cf_put_delete->add_deletes_with_cf();
    write_dels->set_cf_name(Constant::kTxnWriteCF);
    for (const auto &key_del : kv_deletes_write) {
      write_dels->add_keys(key_del);
    }
  }

  pb::raft::DeletesWithCf *lock_dels = nullptr;
  if (!kv_deletes_lock.empty()) {
    lock_dels = cf_put_delete->add_deletes_with_cf();
    lock_dels->set_cf_name(Constant::kTxnLockCF);
    for (const auto &key_del : kv_deletes_lock) {
      lock_dels->add_keys(key_del);
    }
  }

  pb::raft::DeletesWithCf *data_dels = nullptr;
  if (!kv_deletes_data.empty()) {
    data_dels = cf_put_delete->add_deletes_with_cf();
    data_dels->set_cf_name(Constant::kTxnDataCF);
    for (const auto &key_del : kv_deletes_data) {
      data_dels->add_keys(key_del);
    }
  }

  if (nullptr == write_dels && nullptr == lock_dels && nullptr == data_dels) {
    return butil::Status::OK();
  }

  if (txn_raft_request.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }
#if defined(ENABLE_GC_MOCK)
  if (!kv_deletes_write.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kTxnWriteCF, {}, kv_deletes_write);
  if (!kv_deletes_lock.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kTxnLockCF, {}, kv_deletes_lock);
  if (!kv_deletes_data.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kTxnDataCF, {}, kv_deletes_data);
  return butil::Status::OK();
#else
  if (nullptr == raft_engine) {
    return butil::Status::OK();
  }

  return RaftEngineWrite(raft_engine, ctx, txn_raft_request, tenant_id, type, "RaftEngineWriteForTxnGc");
#endif
}

butil::Status TxnEngineHelper::RaftEngineWriteForNonTxnStoreAndDocumentGc(
    std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
    const std::vector<std::string> &kv_deletes_default, int64_t tenant_id, pb::common::RegionType type) {
  BvarLatencyGuard bvar_guard(&g_txn_raft_engine_write_for_gc_latency);

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  pb::raft::DeletesWithCf *default_dels = nullptr;
  if (!kv_deletes_default.empty()) {
    default_dels = cf_put_delete->add_deletes_with_cf();
    default_dels->set_cf_name(Constant::kStoreDataCF);
    for (const auto &key_del : kv_deletes_default) {
      default_dels->add_keys(key_del);
    }
  }

  if (nullptr == default_dels) {
    return butil::Status::OK();
  }

  if (txn_raft_request.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }
#if defined(ENABLE_GC_MOCK)
  if (!kv_deletes_default.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kStoreDataCF, {}, kv_deletes_default);

  return butil::Status::OK();
#else

  if (nullptr == raft_engine) {
    return butil::Status::OK();
  }

  return RaftEngineWrite(raft_engine, ctx, txn_raft_request, tenant_id, type,
                         "RaftEngineWriteForNonTxnStoreAndDocumentGc");
#endif
}

butil::Status TxnEngineHelper::RaftEngineWriteForNonTxnIndexGc(
    std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
    const std::vector<std::string> &kv_deletes_default, const std::vector<std::string> &kv_deletes_scalar,
    const std::vector<std::string> &kv_deletes_table, const std::vector<std::string> &kv_deletes_scalar_speedup,
    int64_t tenant_id, pb::common::RegionType type) {
  BvarLatencyGuard bvar_guard(&g_txn_raft_engine_write_for_gc_latency);

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  pb::raft::DeletesWithCf *default_dels = nullptr;
  if (!kv_deletes_default.empty()) {
    default_dels = cf_put_delete->add_deletes_with_cf();
    default_dels->set_cf_name(Constant::kVectorDataCF);
    for (const auto &key_del : kv_deletes_default) {
      default_dels->add_keys(key_del);
    }
  }

  pb::raft::DeletesWithCf *scalar_dels = nullptr;
  if (!kv_deletes_scalar.empty()) {
    scalar_dels = cf_put_delete->add_deletes_with_cf();
    scalar_dels->set_cf_name(Constant::kVectorScalarCF);
    for (const auto &key_del : kv_deletes_scalar) {
      scalar_dels->add_keys(key_del);
    }
  }

  pb::raft::DeletesWithCf *table_dels = nullptr;
  if (!kv_deletes_table.empty()) {
    table_dels = cf_put_delete->add_deletes_with_cf();
    table_dels->set_cf_name(Constant::kVectorTableCF);
    for (const auto &key_del : kv_deletes_table) {
      table_dels->add_keys(key_del);
    }
  }

  pb::raft::DeletesWithCf *scalar_speedup_dels = nullptr;
  if (!kv_deletes_scalar_speedup.empty()) {
    scalar_speedup_dels = cf_put_delete->add_deletes_with_cf();
    scalar_speedup_dels->set_cf_name(Constant::kVectorScalarKeySpeedUpCF);
    for (const auto &key_del : kv_deletes_scalar_speedup) {
      scalar_speedup_dels->add_keys(key_del);
    }
  }

  if (nullptr == default_dels && nullptr == scalar_dels && nullptr == table_dels && nullptr == scalar_speedup_dels) {
    return butil::Status::OK();
  }

  if (txn_raft_request.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }

#if defined(ENABLE_GC_MOCK)
  if (!kv_deletes_default.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kVectorDataCF, {}, kv_deletes_default);
  if (!kv_deletes_scalar.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kVectorScalarCF, {}, kv_deletes_scalar);
  if (!kv_deletes_table.empty()) ctx->Writer()->KvBatchPutAndDelete(Constant::kVectorTableCF, {}, kv_deletes_table);
  if (!kv_deletes_scalar_speedup.empty())
    ctx->Writer()->KvBatchPutAndDelete(Constant::kVectorScalarKeySpeedUpCF, {}, kv_deletes_scalar_speedup);
  return butil::Status::OK();
#else
  if (nullptr == raft_engine) {
    return butil::Status::OK();
  }

  return RaftEngineWrite(raft_engine, ctx, txn_raft_request, tenant_id, type, "RaftEngineWriteForNonTxnIndexGc");
#endif
}

butil::Status TxnEngineHelper::RaftEngineWrite(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                               pb::raft::TxnRaftRequest &txn_raft_request, int64_t tenant_id,
                                               pb::common::RegionType type, const std::string &name) {
  auto ret = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (ret.error_code() == EPERM) {
    std::string s = fmt::format("[txn_gc][tenant({})][region({})][type({})][nontxn] {}, write failed, status: {}",
                                tenant_id, ctx->RegionId(), pb::common::RegionType_Name(type), name, ret.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::ERAFT_NOTLEADER, s);
  }

  return ret;
}

bvar::LatencyRecorder g_txn_do_final_work_for_gc_latency("dingo_txn_do_final_work_for_gc");

butil::Status TxnEngineHelper::DoFinalWorkForTxnGc(
    std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx, RawEngine::ReaderPtr reader,
    std::shared_ptr<Snapshot> snapshot, const std::string &write_key, int64_t tenant_id, pb::common::RegionType type,
    int64_t safe_point_ts, std::vector<std::string> &kv_deletes_lock, std::vector<std::string> &kv_deletes_data,
    std::vector<std::string> &kv_deletes_write, std::string &lock_start_key, std::string &lock_end_key,
    std::string &last_lock_start_key, std::string &last_lock_end_key) {
  BvarLatencyGuard bvar_guard(&g_txn_do_final_work_for_gc_latency);

  butil::Status status;
  lock_end_key = write_key;
  // optimization . already checked Ignore.
  if (lock_start_key != last_lock_start_key && lock_end_key != last_lock_end_key) {
    status = CheckLockForTxnGc(reader, snapshot, lock_start_key, lock_end_key, safe_point_ts, ctx->RegionId(),
                               tenant_id, type);
    if (!status.ok()) {
      std::string s = fmt::format(
          "[txn_gc][lock][tenant({})][region({})][type({})][txn] CheckLockForTxnGc failed. lock_start_key : {} "
          "lock_end_key : {} safe_point_ts : {}. ignore.",
          tenant_id, ctx->RegionId(), pb::common::RegionType_Name(type), Helper::StringToHex(lock_start_key),
          Helper::StringToHex(lock_end_key), safe_point_ts);
      DINGO_LOG(ERROR) << s + status.error_str();
    }
  }

  status =
      RaftEngineWriteForTxnGc(raft_engine, ctx, kv_deletes_lock, kv_deletes_data, kv_deletes_write, tenant_id, type);
  if (!status.ok()) {
    std::string s = fmt::format(
        "[txn_gc][write][tenant({})][region({})][type({})][txn] RaftEngineWriteForTxnGc failed. kv_deletes_lock size : "
        "{} kv_deletes_data size : {} kv_deletes_write : {} safe_point_ts : {}. ignore.",
        tenant_id, ctx->RegionId(), pb::common::RegionType_Name(type), kv_deletes_lock.size(), kv_deletes_data.size(),
        kv_deletes_write.size(), safe_point_ts);
    DINGO_LOG(ERROR) << s + status.error_str();
  }

  last_lock_start_key = lock_start_key;
  last_lock_end_key = lock_end_key;
  lock_start_key = lock_end_key;
  lock_end_key = "";
  kv_deletes_lock.resize(0);
  kv_deletes_data.resize(0);
  kv_deletes_write.resize(0);

  return butil::Status();
}

butil::Status TxnEngineHelper::DoFinalWorkForNonTxnGc(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                                      int64_t tenant_id, pb::common::RegionType type,
                                                      std::vector<std::string> &kv_deletes_default,
                                                      std::vector<std::string> &kv_deletes_scalar,
                                                      std::vector<std::string> &kv_deletes_table,
                                                      std::vector<std::string> &kv_deletes_scalar_speedup) {
  BvarLatencyGuard bvar_guard(&g_txn_do_final_work_for_gc_latency);

  butil::Status status;

  if (type == pb::common::RegionType::INDEX_REGION) {
    status = RaftEngineWriteForNonTxnIndexGc(raft_engine, ctx, kv_deletes_default, kv_deletes_scalar, kv_deletes_table,
                                             kv_deletes_scalar_speedup, tenant_id, type);
    if (!status.ok()) {
      std::string s = fmt::format(
          "[txn_gc][write][tenant({})][region({})][type({})][nontxn] RaftEngineWriteForNonTxnIndexGc failed. "
          "kv_deletes_default size : {}  kv_deletes_scalar size : {}  kv_deletes_table size : {} "
          "kv_deletes_scalar_speedup size : {}. "
          "ignore.",
          tenant_id, ctx->RegionId(), pb::common::RegionType_Name(type), kv_deletes_default.size(),
          kv_deletes_scalar.size(), kv_deletes_table.size(), kv_deletes_scalar_speedup.size());
      DINGO_LOG(ERROR) << s + status.error_str();
    }
  } else {
    status = RaftEngineWriteForNonTxnStoreAndDocumentGc(raft_engine, ctx, kv_deletes_default, tenant_id, type);
    if (!status.ok()) {
      std::string s = fmt::format(
          "[txn_gc][write][tenant({})][region({})][type({})][nontxn] RaftEngineWriteForNonTxnStoreAndDocumentGc "
          "failed. kv_deletes_default size : {}. ignore.",
          tenant_id, ctx->RegionId(), pb::common::RegionType_Name(type), kv_deletes_default.size());
      DINGO_LOG(ERROR) << s + status.error_str();
    }
  }

  kv_deletes_default.resize(0);
  kv_deletes_scalar.resize(0);
  kv_deletes_table.resize(0);
  kv_deletes_scalar_speedup.resize(0);

  return butil::Status();
}

void TxnEngineHelper::RegularUpdateSafePointTsHandler(void * /*arg*/) {
  static std::atomic<bool> g_regular_update_safe_point_ts_handler_running(false);

  if (g_regular_update_safe_point_ts_handler_running.load(std::memory_order_relaxed)) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
        << "RegularUpdateSafePointTsHandler... g_regular_update_safe_point_ts_handler_running is true, return";
    return;
  }

  AtomicGuard guard(g_regular_update_safe_point_ts_handler_running);

  int64_t start_time = 0;

  std::shared_ptr<CoordinatorInteraction> coordinator_interaction = Server::GetInstance().GetCoordinatorInteraction();
  std::shared_ptr<StoreMetaManager> store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  pb::coordinator::GetGCSafePointRequest request;

  request.mutable_request_info()->set_request_id(Server::GetInstance().Id());
  request.set_get_all_tenant(true);

  start_time = Helper::TimestampMs();
  pb::coordinator::GetGCSafePointResponse response;
  butil::Status status = coordinator_interaction->SendRequest("GetGCSafePoint", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[GetGCSafePoint]  failed, error: {} {}",
                                      pb::error::Errno_Name(status.error_code()), status.error_str());
    return;
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
      << fmt::format("[GetGCSafePoint] response size({}) elapsed time({} ms) response : {}", response.ByteSizeLong(),
                     Helper::TimestampMs() - start_time, response.ShortDebugString());

  std::shared_ptr<GCSafePointManager> gc_safe_point_manager = store_meta_manager->GetGCSafePointManager();

  bool gc_stop = response.gc_stop();
  int64_t safe_point_ts = response.safe_point();

  std::map<int64_t, int64_t> safe_point_ts_group;

  safe_point_ts_group.emplace(Constant::kDefaultTenantId, safe_point_ts);

  for (auto [tenant_id, internal_safe_point_ts] : response.tenant_safe_points()) {
    safe_point_ts_group.emplace(tenant_id, internal_safe_point_ts);
  }

  gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, gc_stop);
}

// readme .
// This macro handles the last safe_point_ts state.
// If it is started, there is no need to repeat the process.
// But the problem is that some regions may not be processed for a long time.
// This region cannot be processed due to leader switching or region state changes.
// In addition, the node's out-of-synchronization of data will also result in the inability to process it in time.
// Scenario 1: If we don’t care about the data processing of a certain region, we can enable it.
// Scenario 2: If the system is always busy and you don’t care that the data can be cleared in time. we can disable it.
// To be conservative, let's close him first. The consequence is a waste of system resources.
#ifndef ENABLE_TXN_GC_REMEMBER_LAST_ACCOMPLISHED_SAFE_POINT_TS
#define ENABLE_TXN_GC_REMEMBER_LAST_ACCOMPLISHED_SAFE_POINT_TS
#endif
#undef ENABLE_TXN_GC_REMEMBER_LAST_ACCOMPLISHED_SAFE_POINT_TS

void TxnEngineHelper::RegularDoGcHandler(void * /*arg*/) {
  static std::atomic<bool> g_regular_do_gc_handler_running(false);

  if (g_regular_do_gc_handler_running.load(std::memory_order_relaxed)) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
        << "RegularUpdateSafePointTsHandler... g_regular_do_gc_handler_running is true, return";
    return;
  }

  AtomicGuard guard(g_regular_do_gc_handler_running);

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format("[txn_gc] gc task start.");

  std::shared_ptr<StoreMetaManager> store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  std::shared_ptr<GCSafePointManager> gc_safe_point_manager = store_meta_manager->GetGCSafePointManager();

  std::map<int64_t, std::pair<bool, int64_t>> safe_point_ts_group;
  safe_point_ts_group = gc_safe_point_manager->GetAllGcFlagAndSafePointTs();

  bool all_gc_stop = true;
  for (auto [tenant_id, safe_point_ts_pair] : safe_point_ts_group) {
    if (!safe_point_ts_pair.first) {
      all_gc_stop = false;
    }
  }

  if (all_gc_stop) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format("[txn_gc] set gc_flag stop, return.");
    for (auto [tenant_id, safe_point_ts_pair] : safe_point_ts_group) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
          "[txn_gc][tenant({})][safe_point_ts({})] set gc_flag stop, return.", tenant_id, safe_point_ts_pair.second);
    }
    return;
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
      << fmt::format("[txn_gc] tenant size : {}", safe_point_ts_group.size());

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
      << fmt::format("[txn_gc] =====================================");

  for (auto iter = safe_point_ts_group.begin(); iter != safe_point_ts_group.end();) {
    if (iter->second.second <= 0) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
          "[txn_gc][tenant({})][safe_point_ts({})] <= 0, ignore. return.", iter->first, iter->second.second);
      gc_safe_point_manager->RemoveSafePoint(iter->first);
      iter = safe_point_ts_group.erase(iter);
    } else {
      ++iter;
    }
  }

  for (auto [tenant_id, safe_point_ts_pair] : safe_point_ts_group) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
        << fmt::format("[txn_gc][tenant({})][safe_point_ts({})] gc task start.", tenant_id, safe_point_ts_pair.second);
    gc_safe_point_manager->SetGcStop(tenant_id, false);
  }
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
      << fmt::format("[txn_gc] =====================================");

#if defined(ENABLE_TXN_GC_REMEMBER_LAST_ACCOMPLISHED_SAFE_POINT_TS)
  for (auto iter = safe_point_ts_group.begin(); iter != safe_point_ts_group.end();) {
    auto gc_safe_point = gc_safe_point_manager->FindSafePoint(iter->first);
    int64_t last_accomplished_safe_point_ts = gc_safe_point->GetLastAccomplishedSafePointTs();
    if (last_accomplished_safe_point_ts >= safe_point_ts_pair.second) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
          << fmt::format("[txn_gc][tenant({})][safe_point_ts({})] already accomplished. ignore", iter->first,
                         safe_point_ts_pair.second);
      gc_safe_point_manager->RemoveSafePoint(iter->first);
      iter = safe_point_ts_group.erase(iter);
    } else {
      ++iter;
    }
  }
#endif

  if (safe_point_ts_group.empty()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format("[txn_gc] tenant_ids empty, return.");
    return;
  }

  std::vector<store::RegionPtr> region_ptrs = Server::GetInstance().GetAllAliveRegion();

  std::vector<store::RegionPtr> leader_region_ptrs;
  leader_region_ptrs.reserve(region_ptrs.size());

  std::shared_ptr<Storage> storage = Server::GetInstance().GetStorage();

  for (auto &region_ptr : region_ptrs) {
    butil::Status status;
    status = storage->ValidateLeader(region_ptr);

    if (status.ok()) {
      // if (region_ptr->LeaderId() == self_id) {
      if (pb::common::StoreRegionState::NORMAL == region_ptr->State()) {
        auto definition = region_ptr->Definition();
        int64_t tenant_id = definition.tenant_id();
        if (safe_point_ts_group.find(tenant_id) != safe_point_ts_group.end()) {
          leader_region_ptrs.push_back(region_ptr);
        }
      }
    }
  }

  sort(leader_region_ptrs.begin(), leader_region_ptrs.end(),
       [](const store::RegionPtr &lhs, const store::RegionPtr &rhs) {
         return mvcc::Codec::EncodeBytes(lhs->Range().start_key()) < mvcc::Codec::EncodeBytes(rhs->Range().start_key());
       });

  for (auto &region_ptr : leader_region_ptrs) {
    if (FLAGS_dingo_log_switch_txn_gc_detail) {
      auto definition = region_ptr->Definition();
      int64_t tenant_id = definition.tenant_id();
      DINGO_LOG(INFO) << fmt::format("[txn_gc][tenant({})][region({})]  start_key : {} end_key : {}", tenant_id,
                                     region_ptr->Id(), Helper::StringToHex(region_ptr->Range().start_key()),
                                     Helper::StringToHex(region_ptr->Range().end_key()));
    }
  }

  // Caution !!!
  // We will not use a snapshot globally because it will affect other region compaction.
  for (const auto &region_ptr : leader_region_ptrs) {
    butil::Status status;

    auto definition = region_ptr->Definition();
    int64_t tenant_id = definition.tenant_id();

    status = storage->ValidateLeader(region_ptr);
    if (!status.ok()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
          << fmt::format("[txn_gc][tenant({})][region({})]  is not leader yet. start_key : {} end_key : {}. ignore.",
                         tenant_id, region_ptr->Id(), Helper::StringToHex(region_ptr->Range().start_key()),
                         Helper::StringToHex(region_ptr->Range().end_key()));
      continue;
    } else {
      if (pb::common::StoreRegionState::NORMAL != region_ptr->State()) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[txn_gc][tenant({})][region({})] is leader. but state is not normal : {}.  start_key : {} end_key : {}.  "
            "ignore.",
            tenant_id, region_ptr->Id(), static_cast<int>(region_ptr->State()),
            Helper::StringToHex(region_ptr->Range().start_key()), Helper::StringToHex(region_ptr->Range().end_key()));
        continue;
      }
    }

    auto [internal_gc_stop, internal_safe_point_ts] = gc_safe_point_manager->GetGcFlagAndSafePointTs(tenant_id);
    auto gc_safe_point = gc_safe_point_manager->FindSafePoint(tenant_id);

    if (internal_gc_stop) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
          "[txn_gc][tenant({})][region({})] set internal_gc_stop stop,  start_key : {} end_key : {} ignore.", tenant_id,
          region_ptr->Id(), Helper::StringToHex(region_ptr->Range().start_key()),
          Helper::StringToHex(region_ptr->Range().end_key()));
      gc_safe_point->SetGcStop(true);
      continue;
    }

    auto safe_point_ts = safe_point_ts_group[tenant_id].second;

    if (safe_point_ts < internal_safe_point_ts) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
          "[txn_gc][tenant({})][region({})] current safe_point_ts : {}. newest safe_point_ts : {}. Don't worry, we'll "
          "deal with it next time. ignore.",
          tenant_id, region_ptr->Id(), safe_point_ts, internal_safe_point_ts);
    }

    dingodb::pb::store::TxnGcRequest request;
    dingodb::pb::store::TxnGcResponse response;

    std::shared_ptr<Context> ctx = std::make_shared<Context>(nullptr, nullptr, &request, &response);
    ctx->SetRegionId(region_ptr->Id());
    ctx->SetCfName(Constant::kStoreDataCF);
    ctx->SetRegionEpoch(region_ptr->Epoch());
    ctx->SetIsolationLevel(::dingodb::pb::store::IsolationLevel::ReadCommitted);
    ctx->SetRawEngineType(region_ptr->GetRawEngineType());
    ctx->SetStoreEngineType(region_ptr->GetStoreEngineType());

    auto writer = storage->GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

    status = writer->TxnGc(ctx, safe_point_ts);

    if (gc_safe_point->GetGcStop()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
          << fmt::format("[txn_gc][tenant({})][region({})]  gc_stop stopped,  start_key : {} end_key : {}. ignore.",
                         tenant_id, ctx->RegionId(), Helper::StringToHex(region_ptr->Range().start_key()),
                         Helper::StringToHex(region_ptr->Range().end_key()));
      continue;
    }

#if defined(ENABLE_TXN_GC_REMEMBER_LAST_ACCOMPLISHED_SAFE_POINT_TS)
    gc_safe_point->SetLastAccomplishedSafePointTs(safe_point_ts);
#endif
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format("[txn_gc] gc task end.");
}

}  // namespace dingodb
