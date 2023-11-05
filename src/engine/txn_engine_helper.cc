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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"

namespace dingodb {

DECLARE_uint32(max_short_value_in_write_cf);
DEFINE_uint32(max_scan_line_count, 10000, "max scan line count");
DEFINE_uint32(max_scan_memory_size, 32 * 1024 * 1024, "max scan memory size");

butil::Status TxnEngineHelper::GetLockInfo(RawEngine::ReaderPtr reader, const std::string &key,
                                           pb::store::LockInfo &lock_info) {
  std::string lock_value;
  auto status = reader->KvGet(Constant::kTxnLockCF, Helper::EncodeTxnKey(key, Constant::kLockVer), lock_value);
  // if lock_value is not found or it is empty, then the key is not locked
  // else the key is locked, return WriteConflict
  if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // key is not exists, the key is not locked
    DINGO_LOG(INFO) << "[txn]GetLockInfo key: " << key << " is not locked, lock_key is not exist";
    return butil::Status::OK();
  }

  if (!status.ok()) {
    // other error, return error
    DINGO_LOG(ERROR) << "[txn]GetLockInfo read lock_key failed, lock_key: " << key
                     << ", status: " << status.error_str();
    return butil::Status(status.error_code(), status.error_str());
  }

  if (lock_value.empty()) {
    // lock_value is empty, the key is not locked
    DINGO_LOG(INFO) << "[txn]GetLockInfo key: " << key << " is not locked, lock_value is null";
    return butil::Status::OK();
  }

  auto ret = lock_info.ParseFromString(lock_value);
  if (!ret) {
    DINGO_LOG(FATAL) << "[txn]GetLockInfo parse lock info failed, lock_key: " << key
                     << ", lock_value(hex): " << Helper::StringToHex(lock_value);
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::ScanLockInfo(RawEnginePtr engine, int64_t min_lock_ts, int64_t max_lock_ts,
                                            const std::string &start_key, const std::string &end_key, uint32_t limit,
                                            std::vector<pb::store::LockInfo> &lock_infos) {
  IteratorOptions iter_options;
  iter_options.lower_bound = Helper::EncodeTxnKey(start_key, Constant::kLockVer);
  iter_options.upper_bound = Helper::EncodeTxnKey(end_key, Constant::kLockVer);

  auto iter = engine->Reader()->NewIterator(Constant::kTxnLockCF, iter_options);
  if (iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]GetLockInfo NewIterator failed, start_key: " << start_key << ", end_key: " << end_key;
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    auto lock_value = iter->Value();
    if (lock_value.length() <= 8) {
      DINGO_LOG(FATAL) << "invalid lock_value, key: " << Helper::StringToHex(iter->Key())
                       << ", min_lock_ts: " << min_lock_ts
                       << ", lock_value is less than 8 bytes: " << Helper::StringToHex(lock_value);
    }

    pb::store::LockInfo lock_info;
    auto ret = lock_info.ParseFromArray(lock_value.data(), lock_value.size());
    if (!ret) {
      DINGO_LOG(FATAL)
      "parse lock info failed, key: " << Helper::StringToHex(iter->Key())
                                      << ", lock_value(hex): " << Helper::StringToHex(lock_value);
    }

    DINGO_LOG(INFO) << "get lock_info lock_ts: " << lock_info.lock_ts()
                    << ", lock_info: " << lock_info.ShortDebugString()
                    << ", iter->key: " << Helper::StringToHex(iter->Key())
                    << ", lock_key: " << Helper::StringToHex(lock_info.key());

    // if lock is not exist, nothing to do
    if (lock_info.lock_ts() == 0) {
      DINGO_LOG(WARNING) << "txn_not_found with lock_info empty, iter->key: " << Helper::StringToHex(iter->Key());

      // auto *txn_not_found = txn_result->mutable_txn_not_found();
      // txn_not_found->set_start_ts(start_ts);
      iter->Next();
      continue;
    }

    if (lock_info.lock_ts() < min_lock_ts || lock_info.lock_ts() >= max_lock_ts) {
      DINGO_LOG(WARNING) << "txn_not_found with lock_info.lock_ts not in range, iter->key: "
                         << Helper::StringToHex(iter->Key()) << ", min_lock_ts: " << min_lock_ts
                         << ", max_lock_ts: " << max_lock_ts << ", lock_info: " << lock_info.ShortDebugString();
      iter->Next();
      continue;
    }

    lock_infos.push_back(lock_info);

    if (limit > 0 && lock_infos.size() >= limit) {
      break;
    }

    iter->Next();
  }

  return butil::Status::OK();
}

// DoRollback
butil::Status TxnEngineHelper::DoRollback(RawEnginePtr /*raw_engine*/, std::shared_ptr<Engine> raft_engine,
                                          std::shared_ptr<Context> ctx,
                                          std::vector<std::string> &keys_to_rollback_with_data,
                                          std::vector<std::string> &keys_to_rollback_without_data, int64_t start_ts) {
  DINGO_LOG(INFO) << "[txn]Rollback start_ts: " << start_ts
                  << ", keys_count_with_data: " << keys_to_rollback_with_data.size()
                  << ", keys_count_without_data: " << keys_to_rollback_without_data.size();

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;
  std::vector<std::string> kv_deletes_data;

  for (const auto &key : keys_to_rollback_without_data) {
    // delete lock
    kv_deletes_lock.emplace_back(Helper::EncodeTxnKey(key, Constant::kLockVer));

    // add write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(Helper::EncodeTxnKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  for (const auto &key : keys_to_rollback_without_data) {
    // delete lock
    kv_deletes_lock.emplace_back(Helper::EncodeTxnKey(key, Constant::kLockVer));

    // delete data
    kv_deletes_data.emplace_back(Helper::EncodeTxnKey(key, start_ts));

    // add write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(Helper::EncodeTxnKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  auto *write_puts = cf_put_delete->add_puts_with_cf();
  write_puts->set_cf_name(Constant::kTxnWriteCF);
  for (auto &kv_put : kv_puts_write) {
    auto *kv = write_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());
  }
  auto *lock_dels = cf_put_delete->add_deletes_with_cf();
  lock_dels->set_cf_name(Constant::kTxnLockCF);
  for (auto &key_del : kv_deletes_lock) {
    lock_dels->add_keys(key_del);
  }
  auto *data_dels = cf_put_delete->add_deletes_with_cf();
  data_dels->set_cf_name(Constant::kTxnDataCF);
  for (auto &key_del : kv_deletes_data) {
    data_dels->add_keys(key_del);
  }

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

butil::Status TxnEngineHelper::BatchGet(RawEnginePtr engine, const pb::store::IsolationLevel &isolation_level,
                                        int64_t start_ts, const std::vector<std::string> &keys,
                                        std::vector<pb::common::KeyValue> &kvs,
                                        pb::store::TxnResultInfo &txn_result_info) {
  DINGO_LOG(INFO) << "[txn]BatchGet keys_count: " << keys.size() << ", isolation_level: " << isolation_level
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

  if (txn_result_info.ByteSizeLong() > 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "txn_result_info is not empty");
  }

  auto reader = engine->Reader();

  // for every key in keys, get lock info, if lock_ts < start_ts, return LockInfo
  // else find the latest write below our start_ts
  // then read data from data_cf
  for (const auto &key : keys) {
    pb::common::KeyValue kv;
    kv.set_key(key);

    pb::store::LockInfo lock_info;
    auto ret = GetLockInfo(reader, key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << "[txn]BatchGet GetLockInfo failed, key: " << key << ", status: " << ret.error_str();
    }

    // if lock_info is exists, check if lock_ts < start_ts
    if (lock_info.lock_ts() > 0 && lock_info.lock_ts() < start_ts) {
      // lock_ts < start_ts, return lock_info
      *(txn_result_info.mutable_locked()) = lock_info;
      return butil::Status::OK();
    }

    IteratorOptions iter_options;
    iter_options.lower_bound = Helper::EncodeTxnKey(key, start_ts);
    iter_options.upper_bound = Helper::EncodeTxnKey(key, 0);
    auto iter = reader->NewIterator(Constant::kTxnWriteCF, iter_options);
    if (iter == nullptr) {
      DINGO_LOG(FATAL) << "[txn]BatchGet NewIterator failed, start_ts: " << start_ts;
    }

    // if the key is committed after start_ts, return WriteConflict
    iter->SeekToFirst();
    while (iter->Valid()) {
      if (iter->Key().length() <= 8) {
        DINGO_LOG(FATAL) << ", invalid write_key, key: " << iter->Key() << ", start_ts: " << start_ts
                         << ", write_key is less than 8 bytes: " << iter->Key();
      }
      std::string write_key;
      int64_t write_ts;
      Helper::DecodeTxnKey(iter->Key(), write_key, write_ts);

      if (write_ts < start_ts) {
        // write_ts < start_ts, return write_info
        pb::store::WriteInfo write_info;
        auto ret = write_info.ParseFromArray(iter->Value().data(), iter->Value().size());
        if (!ret) {
          DINGO_LOG(FATAL) << "[txn]BatchGet parse write info failed, key: " << key << ", write_key: " << iter->Key()
                           << ", write_value(hex): " << Helper::StringToHex(iter->Value());
        }

        if (!write_info.short_value().empty()) {
          kv.set_value(write_info.short_value());
          break;
        }

        auto ret1 =
            reader->KvGet(Constant::kTxnDataCF, Helper::EncodeTxnKey(key, write_info.start_ts()), *kv.mutable_value());
        if (!ret1.ok() && ret1.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "[txn]BatchGet read data failed, key: " << key << ", status: " << ret1.error_str();
        } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(ERROR) << "[txn]BatchGet read data failed, data is illegally not found, key: " << key
                           << ", status: " << ret1.error_str()
                           << ", raw_key: " << Helper::EncodeTxnKey(key, write_info.start_ts());
        }
        break;
      } else {
        DINGO_LOG(ERROR) << "[txn]BatchGet write_ts: " << write_ts << " >= start_ts: " << start_ts << ", key: " << key
                         << ", write_key: " << iter->Key();
      }

      iter->Next();
    }

    kvs.emplace_back(kv);
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::ScanGetNextKeyValue(RawEngine::ReaderPtr reader, std::shared_ptr<Iterator> write_iter,
                                                   std::shared_ptr<Iterator> lock_iter, int64_t start_ts,
                                                   const std::string &start_iter_key, std::string &last_lock_key,
                                                   std::string &last_write_key,
                                                   pb::store::TxnResultInfo &txn_result_info, std::string &iter_key,
                                                   std::string &data_value) {
  DINGO_LOG(INFO) << "[txn]ScanGetNextKeyValue start_ts: " << start_ts
                  << ", start_iter_key: " << Helper::StringToHex(start_iter_key)
                  << ", last_lock_key: " << Helper::StringToHex(last_lock_key)
                  << ", last_write_key: " << Helper::StringToHex(last_write_key);

  int64_t write_ts = 0;
  int64_t lock_ts = 0;

  bool is_last_lock_key_update = false;
  bool is_last_write_key_update = false;

  if (lock_iter->Valid() && last_lock_key < start_iter_key) {
    lock_iter->Seek(Helper::EncodeTxnKey(start_iter_key, Constant::kLockVer));

    if (lock_iter->Valid()) {
      auto ret = Helper::DecodeTxnKey(lock_iter->Key(), last_lock_key, lock_ts);
      if (!ret.ok()) {
        DINGO_LOG(FATAL) << "[txn]Scan DecodeTxnKey failed, lock_iter->key: " << Helper::StringToHex(lock_iter->Key())
                         << ", start_ts: " << start_ts;
      }

      is_last_lock_key_update = true;
    } else {
      DINGO_LOG(INFO) << "[txn]Scan lock_iter is invalid, start_ts: " << start_ts
                      << ", last_lock_key: " << Helper::StringToHex(last_lock_key);
    }
  }

  if (write_iter->Valid() && last_write_key < start_iter_key) {
    write_iter->Seek(Helper::EncodeTxnKey(start_iter_key, start_ts));

    if (write_iter->Valid()) {
      auto ret = Helper::DecodeTxnKey(write_iter->Key(), last_write_key, write_ts);
      if (!ret.ok()) {
        DINGO_LOG(FATAL) << "[txn]Scan DecodeTxnKey failed, write_iter->key: " << Helper::StringToHex(write_iter->Key())
                         << ", start_ts: " << start_ts;
      }

      is_last_write_key_update = true;
    } else {
      DINGO_LOG(INFO) << "[txn]Scan write_iter is invalid, start_ts: " << start_ts
                      << ", last_write_key: " << Helper::StringToHex(last_write_key);
    }
  }

  if (!is_last_lock_key_update && !is_last_write_key_update) {
    DINGO_LOG(INFO) << "[txn]Scan last_lock_key and last_write_key are not updated, start_ts: " << start_ts
                    << ", last_lock_key: " << Helper::StringToHex(last_lock_key)
                    << ", last_write_key: " << Helper::StringToHex(last_write_key);
    return butil::Status::OK();
  }

  if (last_lock_key <= last_write_key) {
    iter_key = last_lock_key;

    // get lock info
    pb::store::LockInfo lock_info;
    auto lock_value = lock_iter->Value();
    auto ret1 = lock_info.ParseFromArray(lock_value.data(), lock_value.size());
    if (!ret1) {
      DINGO_LOG(FATAL) << "[txn]Scan parse lock info failed, lock_key: " << Helper::StringToHex(lock_iter->Key())
                       << ", lock_value(hex): " << Helper::StringToHex(lock_value);
    }

    if (lock_info.lock_ts() < start_ts) {
      // lock_ts < start_ts, return lock_info
      *(txn_result_info.mutable_locked()) = lock_info;
      return butil::Status::OK();
    }

    // if lock_key == write_key, then we can get data from write_cf
    if (last_lock_key == last_write_key) {
      while (write_iter->Valid()) {
        std::string tmp_key;
        int64_t tmp_ts;
        auto ret1 = Helper::DecodeTxnKey(write_iter->Key(), tmp_key, tmp_ts);
        if (!ret1.ok()) {
          DINGO_LOG(FATAL) << "[txn]Scan DecodeTxnKey failed, write_iter->key: "
                           << Helper::StringToHex(write_iter->Key()) << ", start_ts: " << start_ts;
        }

        if (tmp_key > iter_key) {
          DINGO_LOG(INFO) << "[txn]Scan write_iter is invalid, start_ts: " << start_ts
                          << ", last_write_key: " << Helper::StringToHex(last_write_key)
                          << ", tmp_key: " << Helper::StringToHex(tmp_key);
          break;
        }

        pb::store::WriteInfo write_info;
        auto ret2 = write_info.ParseFromArray(write_iter->Value().data(), write_iter->Value().size());
        if (!ret2) {
          DINGO_LOG(FATAL) << "[txn]Scan parse write info failed, write_key: " << Helper::StringToHex(write_iter->Key())
                           << ", write_value(hex): " << Helper::StringToHex(write_iter->Value());
        }

        if (write_info.op() == pb::store::Op::Delete) {
          // if op is delete, value is null
          data_value = std::string();
          return butil::Status::OK();
          write_iter->Next();
        } else if (write_info.op() == pb::store::Op::Rollback) {
          // if op is rollback, go to next write
          write_iter->Next();
        } else if (write_info.op() == pb::store::Op::Put) {
          // use write_ts to get data from data_cf
          if (!write_info.short_value().empty()) {
            data_value = write_info.short_value();
          } else {
            auto ret3 =
                reader->KvGet(Constant::kTxnDataCF, Helper::EncodeTxnKey(tmp_key, write_info.start_ts()), data_value);
            if (!ret3.ok() && ret3.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
              DINGO_LOG(FATAL) << "[txn]Scan read data failed, key: " << tmp_key << ", status: " << ret3.error_str();
            } else if (ret3.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
              DINGO_LOG(ERROR) << "[txn]Scan read data failed, data is illegally not found, key: " << tmp_key
                               << ", status: " << ret3.error_str()
                               << ", raw_key: " << Helper::EncodeTxnKey(tmp_key, write_info.start_ts());
              return butil::Status(pb::error::Errno::EINTERNAL, "data is illegally not found");
            }
          }

          break;
        } else {
          DINGO_LOG(INFO) << "[txn]Scan write_iter meet illegal op, start_ts: " << start_ts
                          << ", last_write_key: " << Helper::StringToHex(last_write_key)
                          << ", tmp_key: " << Helper::StringToHex(tmp_key) << ", write_info.op: " << write_info.op();
          write_iter->Next();
        }
      }

      return butil::Status::OK();
    } else {
      // lock_key < write_key, there is no data
      data_value = std::string();
      return butil::Status::OK();
    }
  } else {
    iter_key = last_write_key;

    while (write_iter->Valid()) {
      std::string tmp_key;
      int64_t tmp_ts;
      auto ret1 = Helper::DecodeTxnKey(write_iter->Key(), tmp_key, tmp_ts);
      if (!ret1.ok()) {
        DINGO_LOG(FATAL) << "[txn]Scan DecodeTxnKey failed, write_iter->key: " << Helper::StringToHex(write_iter->Key())
                         << ", start_ts: " << start_ts;
      }

      if (tmp_key > iter_key) {
        DINGO_LOG(INFO) << "[txn]Scan write_iter is invalid, start_ts: " << start_ts
                        << ", last_write_key: " << Helper::StringToHex(last_write_key)
                        << ", tmp_key: " << Helper::StringToHex(tmp_key);
        break;
      }

      pb::store::WriteInfo write_info;
      auto ret2 = write_info.ParseFromArray(write_iter->Value().data(), write_iter->Value().size());
      if (!ret2) {
        DINGO_LOG(FATAL) << "[txn]Scan parse write info failed, write_key: " << Helper::StringToHex(write_iter->Key())
                         << ", write_value(hex): " << Helper::StringToHex(write_iter->Value());
      }

      if (write_info.op() == pb::store::Op::Delete) {
        // if op is delete, value is null
        data_value = std::string();
        return butil::Status::OK();
      } else if (write_info.op() == pb::store::Op::Rollback) {
        // if op is rollback, go to next write
        write_iter->Next();
      } else if (write_info.op() == pb::store::Op::Put) {
        // use write_ts to get data from data_cf
        if (!write_info.short_value().empty()) {
          data_value = write_info.short_value();
        } else {
          auto ret3 =
              reader->KvGet(Constant::kTxnDataCF, Helper::EncodeTxnKey(tmp_key, write_info.start_ts()), data_value);
          if (!ret3.ok() && ret3.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
            DINGO_LOG(FATAL) << "[txn]Scan read data failed, key: " << tmp_key << ", status: " << ret3.error_str();
          } else if (ret3.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
            DINGO_LOG(ERROR) << "[txn]Scan read data failed, data is illegally not found, key: " << tmp_key
                             << ", status: " << ret3.error_str()
                             << ", raw_key: " << Helper::EncodeTxnKey(tmp_key, write_info.start_ts());
            return butil::Status(pb::error::Errno::EINTERNAL, "data is illegally not found");
          }
        }

        break;
      } else {
        DINGO_LOG(INFO) << "[txn]Scan write_iter meet illegal op, start_ts: " << start_ts
                        << ", last_write_key: " << Helper::StringToHex(last_write_key)
                        << ", tmp_key: " << Helper::StringToHex(tmp_key) << ", write_info.op: " << write_info.op();
        write_iter->Next();
      }
    }

    return butil::Status::OK();
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::Scan(RawEnginePtr raw_engine, const pb::store::IsolationLevel &isolation_level,
                                    int64_t start_ts, const pb::common::Range &range, int64_t limit, bool key_only,
                                    bool is_reverse, pb::store::TxnResultInfo &txn_result_info,
                                    std::vector<pb::common::KeyValue> &kvs, bool &has_more, std::string &end_key) {
  DINGO_LOG(INFO) << "[txn]Scan start_ts: " << start_ts << ", range: " << range.ShortDebugString()
                  << ", isolation_level: " << isolation_level << ", start_ts: " << start_ts << ", limit: " << limit
                  << ", key_only: " << key_only << ", is_reverse: " << is_reverse
                  << ", txn_result_info: " << txn_result_info.ShortDebugString();

  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan engine is null";
  }

  if (limit == 0) {
    return butil::Status::OK();
  }

  if (!kvs.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "kvs is not empty");
  }

  if (has_more || !end_key.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "has_more or end_key is not empty");
  }

  auto snapshot = raw_engine->GetSnapshot();

  auto reader = raw_engine->Reader();
  // construct lock iter
  IteratorOptions write_iter_options;
  write_iter_options.lower_bound = Helper::EncodeTxnKey(range.start_key(), start_ts);
  write_iter_options.upper_bound = Helper::EncodeTxnKey(range.end_key(), 0);

  auto write_iter = reader->NewIterator(Constant::kTxnWriteCF, snapshot, write_iter_options);
  if (write_iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewIterator write failed, start_ts: " << start_ts;
  }

  // construct lock iter
  IteratorOptions lock_iter_options;
  lock_iter_options.lower_bound = Helper::EncodeTxnKey(range.start_key(), Constant::kLockVer);
  lock_iter_options.upper_bound = Helper::EncodeTxnKey(range.end_key(), Constant::kLockVer);

  auto lock_iter = reader->NewIterator(Constant::kTxnLockCF, snapshot, lock_iter_options);
  if (lock_iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewIterator lock failed, start_ts: " << start_ts;
  }

  // iter write and lock iter, if lock_ts < start_ts, return LockInfo
  write_iter->SeekToFirst();
  lock_iter->SeekToFirst();

  if ((!write_iter->Valid()) && (!lock_iter->Valid())) {
    DINGO_LOG(ERROR) << "[txn]Scan write_iter is not valid and lock_iter is not valid, start_ts: " << start_ts
                     << ", write_iter->Valid(): " << write_iter->Valid()
                     << ", lock_iter->Valid(): " << lock_iter->Valid();
    has_more = false;
    return butil::Status::OK();
  }

  // scan keys
  std::string last_iter_key = Helper::StringSubtractRightAlign(range.start_key(), std::string(1, 0x1));
  std::string last_lock_key = last_iter_key;
  std::string last_write_key = last_iter_key;
  std::string start_iter_key = last_iter_key;
  std::string iter_key;
  std::string data_value;

  while (true) {
    data_value.clear();
    iter_key.clear();
    auto ret = ScanGetNextKeyValue(reader, write_iter, lock_iter, start_ts, start_iter_key, last_lock_key,
                                   last_write_key, txn_result_info, iter_key, data_value);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "[txn]Scan ScanGetNextKeyValue failed, start_ts: " << start_ts
                       << ", status: " << ret.error_str();
      return ret;
    }

    if (iter_key.empty()) {
      DINGO_LOG(INFO) << "[txn]Scan iter_key is empty, end scan, start_ts: " << start_ts
                      << ", range: " << range.ShortDebugString()
                      << ", last_iter_key: " << Helper::StringToHex(last_iter_key);
      break;
    }

    last_iter_key = iter_key;

    if (txn_result_info.ByteSizeLong() > 0) {
      // if txn_result_info is not empty, return
      DINGO_LOG(INFO) << "[txn]Scan txn_result_info is not empty, end scan, start_ts: " << start_ts
                      << ", range: " << range.ShortDebugString()
                      << ", last_iter_key: " << Helper::StringToHex(last_iter_key)
                      << ", txn_result_info: " << txn_result_info.ShortDebugString();
      end_key = iter_key;
      return butil::Status::OK();
    }

    if (!data_value.empty()) {
      pb::common::KeyValue kv;
      kv.set_key(iter_key);
      if (!key_only) {
        kv.set_value(data_value);
      }
      kvs.push_back(kv);
    }

    if ((limit > 0 && kvs.size() >= limit) || kvs.size() >= FLAGS_max_scan_line_count) {
      has_more = true;
      break;
    }

    // next scan start from iter_key + 1, use prefix_next to generate next start_key
    start_iter_key = Helper::PrefixNext(iter_key);
  }

  end_key = last_iter_key;
  return butil::Status::OK();
}

butil::Status TxnEngineHelper::GetWriteInfo(RawEnginePtr engine, int64_t min_commit_ts, int64_t max_commit_ts,
                                            int64_t start_ts, const std::string &key, bool include_rollback,
                                            bool include_delete, bool include_put, pb::store::WriteInfo &write_info,
                                            int64_t &commit_ts) {
  IteratorOptions iter_options;
  iter_options.lower_bound = Helper::EncodeTxnKey(key, max_commit_ts);
  iter_options.upper_bound = Helper::EncodeTxnKey(key, min_commit_ts);
  auto iter = engine->Reader()->NewIterator(Constant::kTxnWriteCF, iter_options);
  if (iter == nullptr) {
    DINGO_LOG(FATAL) << ", new iterator failed, key: " << Helper::StringToHex(key);
  }

  // if the key is committed after start_ts, return WriteConflict
  pb::store::WriteInfo tmp_write_info;
  iter->SeekToFirst();
  while (iter->Valid()) {
    if (iter->Key().length() <= 8) {
      DINGO_LOG(ERROR) << "invalid write_key, key: " << Helper::StringToHex(iter->Key())
                       << ", write_key is less than 8 bytes: " << Helper::StringToHex(iter->Key());
      return butil::Status(pb::error::Errno::EINTERNAL, "invalid write_key");
    }

    std::string write_key;
    int64_t write_ts;
    Helper::DecodeTxnKey(iter->Key(), write_key, write_ts);

    if (write_ts < min_commit_ts) {
      break;
    }

    auto ret = tmp_write_info.ParseFromArray(iter->Value().data(), iter->Value().size());
    if (!ret) {
      DINGO_LOG(ERROR) << "cannot parse tmp_write_info, key: " << Helper::StringToHex(iter->Key())
                       << ", write_ts: " << write_ts << ", write_key: " << Helper::StringToHex(iter->Key())
                       << ", write_value(hex): " << Helper::StringToHex(iter->Value());
      return butil::Status(pb::error::Errno::EINTERNAL, "cannot parse tmp_write_info");
    }

    if (start_ts > 0) {
      if (tmp_write_info.start_ts() != start_ts) {
        iter->Next();
        continue;
      }
    }

    if (tmp_write_info.op() == pb::store::Op::Rollback) {
      if (!include_rollback) {
        iter->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Delete) {
      if (!include_delete) {
        iter->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Put) {
      if (!include_put) {
        iter->Next();
        continue;
      }
    } else {
      DINGO_LOG(ERROR) << "invalid write op, key: " << Helper::StringToHex(iter->Key()) << ", write_ts: " << write_ts
                       << ", write_key: " << Helper::StringToHex(iter->Key())
                       << ", write_value(hex): " << Helper::StringToHex(iter->Value());
      iter->Next();
      continue;
    }

    write_info = tmp_write_info;
    commit_ts = write_ts;
    break;
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::GetRollbackInfo(RawEngine::ReaderPtr reader, int64_t start_ts, const std::string &key,
                                               pb::store::WriteInfo &write_info) {
  std::string write_value;
  auto ret = reader->KvGet(Constant::kTxnWriteCF, Helper::EncodeTxnKey(key, start_ts), write_value);
  if (ret.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // no rollback
    DINGO_LOG(INFO) << "not find a rollback write line, key: " << Helper::StringToHex(key)
                    << ", start_ts: " << start_ts;

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
    DINGO_LOG(ERROR) << "parse write info failed, key: " << key << ", start_ts: " << start_ts
                     << ", tmp_write_info: " << tmp_write_info.ShortDebugString()
                     << ", write_value: " << Helper::StringToHex(write_value);
    return butil::Status(pb::error::Errno::EINTERNAL, "parse write info failed");
  }

  if (tmp_write_info.op() == pb::store::Op::Rollback) {
    if (tmp_write_info.start_ts() == start_ts) {
      write_info = tmp_write_info;
      return butil::Status::OK();
    } else {
      DINGO_LOG(ERROR) << "find a rollback write line, but not the same start_ts, there my be FATAL error, write_info: "
                       << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key)
                       << ", start_ts: " << start_ts;
    }
  } else {
    DINGO_LOG(ERROR) << "find a rollback write line, but not rollback op, there may be FATAL error, write_info: "
                     << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key)
                     << ", start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "find a rollback write line, but not rollback op");
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::PessimisticLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                               std::shared_ptr<Context> ctx,
                                               const std::vector<pb::store::Mutation> &mutations,
                                               const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                               int64_t for_update_ts, std::string extra_data) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] PessimisticLock, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                  << ", mutations_size: " << mutations.size() << ", primary_lock: " << primary_lock
                  << ", lock_ttl: " << lock_ttl << ", for_update_ts: " << for_update_ts
                  << ", extra_data: " << extra_data;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::PessimisticRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                                   std::shared_ptr<Context> ctx, int64_t start_ts,
                                                   int64_t for_update_ts, const std::vector<std::string> &keys) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] PessimisticRollback, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", for_update_ts: " << for_update_ts
                  << ", keys_size: " << keys.size();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::Prewrite(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                        std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation> &mutations,
                                        const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                        int64_t txn_size, bool try_one_pc, int64_t max_commit_ts) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                  << ", mutations_size: " << mutations.size() << ", primary_lock: " << primary_lock
                  << ", lock_ttl: " << lock_ttl << ", txn_size: " << txn_size << ", try_one_pc: " << try_one_pc
                  << ", max_commit_ts: " << max_commit_ts;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  std::vector<pb::common::KeyValue> kv_puts_data;
  std::vector<pb::common::KeyValue> kv_puts_lock;

  auto *response = dynamic_cast<pb::store::TxnPrewriteResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                     << ", response is nullptr";
  }
  auto *error = response->mutable_error();
  // auto *txn_result = response->mutable_txn_result();

  auto reader = raw_engine->Reader();
  // for every mutation, check and do prewrite, if any one of the mutation is failed, the whole prewrite is failed
  for (const auto &mutation : mutations) {
    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    pb::store::LockInfo lock_info;
    auto ret = GetLockInfo(reader, mutation.key(), lock_info);
    if (!ret.ok()) {
      // TODO: do read before write to raft state machine
      // Now we need to fatal exit to prevent data inconsistency between raft peers
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite, start_ts: {}", region->Id(), start_ts)
                       << ", get lock info failed, key: " << mutation.key() << ", status: " << ret.error_str();

      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());

      // need response to client
      return ret;
    }

    if (!lock_info.primary_lock().empty()) {
      if (lock_info.lock_ts() == start_ts) {
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite,", region->Id()) << ", key: " << mutation.key()
                        << " is locked by self, lock_info: " << lock_info.ShortDebugString();
        // go to next key
        continue;
      } else {
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite,", region->Id()) << ", key: " << mutation.key()
                        << " is locked conflict, lock_info: " << lock_info.ShortDebugString();

        // add txn_result for response
        // setup lock_info
        *response->add_txn_result()->mutable_locked() = lock_info;

        DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                        << ", lock_conflict, key: " << mutation.key()
                        << ", lock_info: " << lock_info.ShortDebugString();

        // need response to client
        continue;
      }
    }

    // 2. check if the key is committed or rollbacked after start_ts
    //    if the key is committed or rollbacked after start_ts, return WriteConflict
    // 2.1 check rollback
    // if there is a rollback, there will be a key | start_ts : WriteInfo| in write_cf
    pb::store::WriteInfo write_info;
    auto ret1 = GetRollbackInfo(reader, start_ts, mutation.key(), write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                       << ", get rollback info failed, key: " << mutation.key() << ", start_ts: " << start_ts
                       << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == start_ts) {
      DINGO_LOG(INFO) << "find this transaction is rollbacked,return  SelfRolledBack , start_ts: " << start_ts
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

      DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                      << ", write_conflict, start_ts: " << start_ts
                      << ", write_info: " << write_info.ShortDebugString();

      continue;
    }

    // 2.2 check commit
    // if there is a commit, there will be a key | commit_ts : WriteInfo| in write_cf
    int64_t commit_ts = 0;
    auto ret2 = GetWriteInfo(raw_engine, start_ts, Constant::kMaxVer, 0, mutation.key(), false, true, true, write_info,
                             commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                       << ", get write info failed, key: " << mutation.key() << ", start_ts: " << start_ts
                       << ", status: " << ret2.error_str();
    }

    if (commit_ts > start_ts) {
      DINGO_LOG(INFO) << "find this transaction is committed,return  WriteConflict start_ts: " << start_ts
                      << ", commit_ts: " << commit_ts << ", write_info: " << write_info.ShortDebugString();

      // prewrite meet write_conflict here
      // add txn_result for response
      // setup write_conflict ( this may not be necessary, when lock_info is set)
      auto *txn_result = response->add_txn_result();
      auto *write_conflict = txn_result->mutable_write_conflict();
      write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_PessimisticRetry);
      write_conflict->set_start_ts(start_ts);
      write_conflict->set_conflict_ts(commit_ts);
      write_conflict->set_key(mutation.key());
      // write_conflict->set_primary_key(lock_info.primary_lock());

      DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                      << ", write_conflict, start_ts: " << start_ts << ", commit_ts: " << commit_ts
                      << ", write_info: " << write_info.ShortDebugString();

      continue;
    }

    // 3.do Put/Delete/PutIfAbsent
    if (mutation.op() == pb::store::Op::Put) {
      // put data
      if (mutation.value().length() >= FLAGS_max_short_value_in_write_cf) {
        pb::common::KeyValue kv;
        std::string data_key = Helper::EncodeTxnKey(mutation.key(), start_ts);
        kv.set_key(data_key);
        kv.set_value(mutation.value());

        kv_puts_data.push_back(kv);
      }

      // put lock
      {
        pb::common::KeyValue kv;
        kv.set_key(Helper::EncodeTxnKey(mutation.key(), Constant::kLockVer));

        pb::store::LockInfo lock_info;
        lock_info.set_primary_lock(primary_lock);
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::Put);
        if (mutation.value().length() < FLAGS_max_short_value_in_write_cf) {
          lock_info.set_short_value(mutation.value());
        }
        kv.set_value(lock_info.SerializeAsString());

        kv_puts_lock.push_back(kv);
      }
    } else if (mutation.op() == pb::store::Op::PutIfAbsent) {
      // check if key is exist
      if (write_info.op() == pb::store::Op::Put) {
        response->add_keys_already_exist()->set_key(mutation.key());
        // this mutation is success with key_exist, go to next mutation
        continue;
      } else {
        // put data
        if (mutation.value().length() >= FLAGS_max_short_value_in_write_cf) {
          pb::common::KeyValue kv;
          std::string data_key = Helper::EncodeTxnKey(mutation.key(), start_ts);
          kv.set_key(data_key);
          kv.set_value(mutation.value());

          kv_puts_data.push_back(kv);
        }

        // put lock
        {
          pb::common::KeyValue kv;
          kv.set_key(Helper::EncodeTxnKey(mutation.key(), Constant::kLockVer));

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock(primary_lock);
          lock_info.set_lock_ts(start_ts);
          lock_info.set_key(mutation.key());
          lock_info.set_lock_ttl(lock_ttl);
          lock_info.set_txn_size(txn_size);
          lock_info.set_lock_type(pb::store::Op::Put);
          if (mutation.value().length() < FLAGS_max_short_value_in_write_cf) {
            lock_info.set_short_value(mutation.value());
          }
          kv.set_value(lock_info.SerializeAsString());

          kv_puts_lock.push_back(kv);
        }
      }
    } else if (mutation.op() == pb::store::Op::Delete) {
      // put data
      // for delete, we don't write anything to kTxnDataCf.
      // when doing commit, we read op from lock_info, and write op to kTxnWriteCf with write_info.

      // put lock
      {
        pb::common::KeyValue kv;
        kv.set_key(Helper::EncodeTxnKey(mutation.key(), Constant::kLockVer));

        pb::store::LockInfo lock_info;
        lock_info.set_primary_lock(primary_lock);
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::Delete);
        kv.set_value(lock_info.SerializeAsString());

        kv_puts_lock.push_back(kv);
      }
    } else {
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Prewrite,", region->Id())
                       << ", invalid op, key: " << mutation.key() << ", start_ts: " << start_ts
                       << ", op: " << mutation.op();
      return butil::Status(pb::error::Errno::EINTERNAL, "invalid op");
    }
  }

  if (response->txn_result_size() > 0) {
    DINGO_LOG(INFO) << fmt::format("[txn][region({})] Prewrite return txn_result,", region->Id())
                    << ", txn_result_size: " << response->txn_result_size() << ", start_ts: " << start_ts
                    << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString()
                    << ", mutations_size: " << mutations.size();
    return butil::Status::OK();
  }

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *data_puts = cf_put_delete->add_puts_with_cf();
  data_puts->set_cf_name(Constant::kTxnDataCF);
  for (auto &kv_put : kv_puts_data) {
    auto *kv = data_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());
  }
  auto *lock_puts = cf_put_delete->add_puts_with_cf();
  lock_puts->set_cf_name(Constant::kTxnLockCF);
  for (auto &kv_put : kv_puts_lock) {
    auto *kv = lock_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());
  }

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

butil::Status TxnEngineHelper::Commit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                      std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                      const std::vector<std::string> &keys) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] Commit, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
                  << ", keys_size: " << keys.size();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Commit", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  // create reader and writer
  auto reader = raw_engine->Reader();

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

  // for vector index region, commit to vector index
  pb::raft::Request raft_request_for_vector_add;
  pb::raft::Request raft_request_for_vector_del;
  auto *vector_add = raft_request_for_vector_add.mutable_vector_add();
  auto *vector_del = raft_request_for_vector_del.mutable_vector_delete();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  std::vector<pb::store::LockInfo> lock_infos;
  for (const auto &key : keys) {
    pb::store::LockInfo lock_info;
    auto ret = TxnEngineHelper::GetLockInfo(reader, key, lock_info);
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
    //                      << ", txn_not_found with lock_info empty, key: " << key << ", start_ts: " << start_ts;

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
                           << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: " << key
                           << ", lock_info: " << lock_info.ShortDebugString();

        auto *locked = txn_result->mutable_locked();
        *locked = lock_info;

        return butil::Status::OK();
      }
    } else {
      // check if the key is already committed, if it is committed can skip it
      pb::store::WriteInfo write_info;
      int64_t prev_commit_ts = 0;
      auto ret2 = TxnEngineHelper::GetWriteInfo(raw_engine, start_ts, Constant::kMaxVer, start_ts, key, false, true,
                                                true, write_info, prev_commit_ts);
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
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] Commit, start_Ts: {}, commit_ts: {}", region->Id(), start_ts,
                                       commit_ts)
                        << ", key: " << key << " is already committed, prev_commit_ts: " << prev_commit_ts;
        continue;
      }
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

butil::Status TxnEngineHelper::DoTxnCommit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, store::RegionPtr region,
                                           const std::vector<pb::store::LockInfo> &lock_infos, int64_t start_ts,
                                           int64_t commit_ts) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {}", region->Id(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
                  << ", lock_infos_size: " << lock_infos.size();

  // create reader and writer
  auto reader = raw_engine->Reader();

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  // for vector index region, commit to vector index
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();
  auto *vector_add = cf_put_delete->mutable_vector_add();
  auto *vector_del = cf_put_delete->mutable_vector_del();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  for (const auto &lock_info : lock_infos) {
    // 1.put data to write_cf
    std::string data_value;
    if (lock_info.short_value().length() > 0) {
      data_value = lock_info.short_value();
    } else if (lock_info.lock_type() == pb::store::Put) {
      auto ret = reader->KvGet(Constant::kTxnDataCF, Helper::EncodeTxnKey(lock_info.key(), start_ts), data_value);
      if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, start_ts: {} commit_ts: {}", region->Id(),
                                        start_ts, commit_ts)
                         << ", get data failed, key: " << lock_info.key() << ", start_ts: " << start_ts
                         << ", status: " << ret.error_str() << ", lock_info: " << lock_info.ShortDebugString();
      }
    }

    {
      pb::common::KeyValue kv;
      std::string write_key = Helper::EncodeTxnKey(lock_info.key(), commit_ts);
      kv.set_key(write_key);

      pb::store::WriteInfo write_info;
      write_info.set_start_ts(start_ts);
      write_info.set_op(lock_info.lock_type());
      if (!data_value.empty()) {
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
          auto vector_id = Helper::DecodeVectorId(lock_info.key());
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
    }

    // 3.delete lock from lock_cf
    { kv_deletes_lock.push_back(Helper::EncodeTxnKey(lock_info.key(), Constant::kLockVer)); }
  }

  // after all mutations is processed, write into raft engine
  auto *write_puts = cf_put_delete->add_puts_with_cf();
  write_puts->set_cf_name(Constant::kTxnWriteCF);
  for (auto &kv_put : kv_puts_write) {
    auto *kv = write_puts->add_kvs();
    kv->set_key(kv_put.key());
    kv->set_value(kv_put.value());
  }
  auto *lock_dels = cf_put_delete->add_deletes_with_cf();
  lock_dels->set_cf_name(Constant::kTxnLockCF);
  for (auto &key_del : kv_deletes_lock) {
    lock_dels->add_keys(key_del);
  }

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

butil::Status TxnEngineHelper::CheckTxnStatus(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                              std::shared_ptr<Context> ctx, const std::string &primary_key,
                                              int64_t lock_ts, int64_t caller_start_ts, int64_t current_ts) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] CheckTxnStatus, primary_key: {}", ctx->RegionId(), primary_key)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", lock_ts: " << lock_ts
                  << ", caller_start_ts: " << caller_start_ts << ", current_ts: " << current_ts;

  // we need to do if primay_key is in this region'range in service before apply to raft state machine
  // use reader to get if the lock is exists, if lock is exists, check if the lock is expired its ttl, if expired do
  // rollback and return if not expired, return conflict if the lock is not exists, return commited the the lock's ts is
  // matched, but it is not a primary_key, return PrimaryMismatch

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] CheckTxnStatus", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  // create reader and writer
  auto reader = raw_engine->Reader();

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
  auto ret = TxnEngineHelper::GetLockInfo(reader, primary_key, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus, ", region->Id())
                     << ", get lock info failed, primary_key: " << primary_key << ", lock_ts: " << lock_ts
                     << ", status: " << ret.error_str();
  }

  if (lock_info.lock_ts() == lock_ts) {
    // the lock is exists, check if it is expired, if not expired, return conflict, if expired, do rollback
    // check if this is a primary key
    if (lock_info.key() != lock_info.primary_lock()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                         << ", primary mismatch, primary_key: " << primary_key << ", lock_ts: " << lock_ts
                         << ", lock_info: " << lock_info.ShortDebugString();

      auto *primary_mismatch = txn_result->mutable_primary_mismatch();
      *(primary_mismatch->mutable_lock_info()) = lock_info;
      return butil::Status::OK();
    }

    int64_t current_ms = current_ts >> 18;

    DINGO_LOG(INFO) << "lock is exists, check ttl, lock_info: " << lock_info.ShortDebugString()
                    << ", current_ms: " << current_ms;

    if (lock_info.lock_ttl() >= current_ms) {
      DINGO_LOG(INFO) << "lock is not expired, return conflict, lock_info: " << lock_info.ShortDebugString()
                      << ", current_ms: " << current_ms;

      response->set_lock_ttl(lock_info.lock_ttl());
      response->set_commit_ts(0);
      response->set_action(::dingodb::pb::store::Action::NoAction);
      return butil::Status::OK();
    }

    DINGO_LOG(INFO) << "lock is expired, do rollback, lock_info: " << lock_info.ShortDebugString()
                    << ", current_ms: " << current_ms;

    // lock is expired, do rollback
    std::vector<std::string> keys_to_rollback_with_data;
    std::vector<std::string> keys_to_rollback_without_data;
    if (lock_info.short_value().empty()) {
      keys_to_rollback_with_data.push_back(primary_key);
    } else {
      keys_to_rollback_without_data.push_back(primary_key);
    }
    auto ret =
        DoRollback(raw_engine, raft_engine, ctx, keys_to_rollback_with_data, keys_to_rollback_without_data, lock_ts);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                       << ", rollback failed, primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret.error_str();
    }

    response->set_lock_ttl(0);
    response->set_commit_ts(0);
    response->set_action(::dingodb::pb::store::Action::TTLExpireRollback);
    return butil::Status::OK();
  } else {
    // the lock is not exists, check if it is rollbacked or committed
    // try to get if there is a rollback to lock_ts
    pb::store::WriteInfo write_info;
    auto ret1 = TxnEngineHelper::GetRollbackInfo(reader, lock_ts, primary_key, write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus, ", region->Id())
                       << ", get rollback info failed, primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == lock_ts) {
      // rollback, return rollback
      response->set_lock_ttl(0);
      response->set_commit_ts(0);
      response->set_action(::dingodb::pb::store::Action::LockNotExistDoNothing);
      return butil::Status::OK();
    }

    // if there is not a rollback to lock_ts, try to get the commit_ts
    int64_t commit_ts = 0;
    auto ret2 = TxnEngineHelper::GetWriteInfo(raw_engine, lock_ts, Constant::kMaxVer, lock_ts, primary_key, false, true,
                                              true, write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] CheckTxnStatus,", region->Id())
                       << ", get write info failed, primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret2.error_str();
    }

    if (commit_ts == 0) {
      // it seems there is a lock previously exists, but it is not committed, and there is no rollback, there must be
      // some error, return TxnNotFound
      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_primary_key(primary_key);
      txn_not_found->set_start_ts(lock_ts);
      return butil::Status::OK();
    }

    // commit, return committed
    response->set_lock_ttl(0);
    response->set_commit_ts(commit_ts);
    response->set_action(::dingodb::pb::store::Action::NoAction);
    return butil::Status::OK();
  }
}

butil::Status TxnEngineHelper::BatchRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                             std::shared_ptr<Context> ctx, int64_t start_ts,
                                             const std::vector<std::string> &keys) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] BatchRollback, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", keys_size: " << keys.size();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] BatchRollback", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  // create reader and writer
  auto reader = raw_engine->Reader();
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
    auto ret = TxnEngineHelper::GetLockInfo(reader, key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                       << ", get lock info failed, key: " << key << ", start_ts: " << start_ts
                       << ", status: " << ret.error_str();
    }

    // if lock is not exist, nothing to do
    if (lock_info.primary_lock().empty()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                         << ", txn_not_found with lock_info empty, key: " << key << ", start_ts: " << start_ts;

      // auto *txn_not_found = txn_result->mutable_txn_not_found();
      // txn_not_found->set_start_ts(start_ts);
      continue;
    }

    if (lock_info.lock_ts() != start_ts) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                         << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: " << key
                         << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();

      // it's not a legal rollback, return lock_info
      auto *locked = txn_result->mutable_locked();
      *locked = lock_info;
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
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] BatchRollback, ", region->Id())
                     << ", rollback failed, status: " << ret.error_str();
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::ResolveLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                           const std::vector<std::string> &keys) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] ResolveLock, start_ts: {}", ctx->RegionId(), start_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", commit_ts: " << commit_ts
                  << ", keys_size: " << keys.size();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] ResolveLock", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  // if commit_ts = 0, do rollback else do commit
  // scan lock_cf to search if transaction with start_ts is exists, if exists, do rollback or commit
  // if not exists, do nothing
  // create reader and writer
  auto reader = raw_engine->Reader();

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

  // for vector index region, commit to vector index
  pb::raft::Request raft_request_for_vector_add;
  pb::raft::Request raft_request_for_vector_del;
  auto *vector_add = raft_request_for_vector_add.mutable_vector_add();
  auto *vector_del = raft_request_for_vector_del.mutable_vector_delete();

  std::vector<std::string> keys_to_rollback;

  std::vector<pb::store::LockInfo> lock_infos_to_commit;
  std::vector<std::string> keys_to_rollback_with_data;
  std::vector<std::string> keys_to_rollback_without_data;

  // if keys is not empty, we only do resolve lock for these keys
  if (!keys.empty()) {
    for (const auto &key : keys) {
      pb::store::LockInfo lock_info;
      auto ret = GetLockInfo(reader, key, lock_info);
      if (!ret.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                         << ", get lock info failed, key: " << key << ", start_ts: " << start_ts
                         << ", status: " << ret.error_str();
      }

      // if lock is not exist, nothing to do
      if (lock_info.primary_lock().empty()) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                           << ", txn_not_found with lock_info empty, key: " << key << ", start_ts: " << start_ts;

        // auto *txn_not_found = txn_result->mutable_txn_not_found();
        // txn_not_found->set_start_ts(start_ts);
        continue;
      }

      if (lock_info.lock_ts() != start_ts) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] ResolveLock,", region->Id())
                           << ", txn_not_found with lock_info.lock_ts not equal to start_ts, key: " << key
                           << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();
        continue;
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
    std::vector<pb::store::LockInfo> tmp_lock_infos;
    auto ret = ScanLockInfo(raw_engine, start_ts, start_ts + 1, region->Range().start_key(), region->Range().end_key(),
                            0, tmp_lock_infos);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                       << ", get lock info failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    }

    for (const auto &lock_info : tmp_lock_infos) {
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
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] ResolveLock, ", region->Id())
                       << ", do txn commit failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    }
  }

  if (!keys_to_rollback_with_data.empty() || !keys_to_rollback_without_data.empty()) {
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

butil::Status TxnEngineHelper::HeartBeat(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                         std::shared_ptr<Context> ctx, const std::string &primary_lock,
                                         int64_t start_ts, int64_t advise_lock_ttl) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", ctx->RegionId(), primary_lock)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", start_ts: " << start_ts
                  << ", advise_lock_ttl: " << advise_lock_ttl;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  auto *response = dynamic_cast<pb::store::TxnHeartBeatResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(), primary_lock)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  pb::store::LockInfo lock_info;
  auto ret = GetLockInfo(raw_engine->Reader(), primary_lock, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(), primary_lock)
                     << ", get lock info failed, start_ts: " << start_ts << ", status: " << ret.error_str();
    return ret;
  }

  if (lock_info.primary_lock().empty()) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(), primary_lock)
                       << ", txn_not_found with lock_info empty, primary_lock: " << primary_lock
                       << ", start_ts: " << start_ts;

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
    return butil::Status::OK();
  }

  if (lock_info.lock_ts() != start_ts) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HeartBeat, primary_lock: {}", region->Id(), primary_lock)
                       << ", txn_not_found with lock_info.lock_ts not equal to start_ts, primary_lock: " << primary_lock
                       << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
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
  kv_lock->set_key(Helper::EncodeTxnKey(primary_lock, Constant::kLockVer));
  kv_lock->set_value(lock_info.SerializeAsString());

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

butil::Status TxnEngineHelper::DeleteRange(RawEnginePtr /*raw_engine*/, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, const std::string &start_key,
                                           const std::string &end_key) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] DeleteRange, start_key: {}", ctx->RegionId(), start_key)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString() << ", end_key: " << end_key;

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DeleteRange", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  auto *response = dynamic_cast<pb::store::TxnDeleteRangeResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] DeleteRange, start_key: {}", region->Id(), start_key)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();

  pb::raft::TxnRaftRequest txn_raft_request;
  auto *delete_range_request = txn_raft_request.mutable_mvcc_delete_range();
  delete_range_request->set_start_key(start_key);
  delete_range_request->set_end_key(end_key);

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

butil::Status TxnEngineHelper::Gc(RawEnginePtr /*raw_engine*/, std::shared_ptr<Engine> raft_engine,
                                  std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] Gc, safe_point_ts: {}", ctx->RegionId(), safe_point_ts)
                  << ", region_epoch: " << ctx->RegionEpoch().ShortDebugString();

  auto region = Server::GetInstance().GetRegion(ctx->RegionId());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Gc", region->Id())
                     << ", region is not found, region_id: " << ctx->RegionId();
  }

  auto *response = dynamic_cast<pb::store::TxnGcResponse *>(ctx->Response());
  if (response == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[txn][region({})] Gc, safe_point_ts: {}", region->Id(), safe_point_ts)
                     << ", response is nullptr";
    return butil::Status(pb::error::Errno::EINTERNAL, "response is nullptr");
  }
  auto *error = response->mutable_error();

  std::vector<std::string> kv_deletes_lock;
  std::vector<std::string> kv_deletes_data;
  std::vector<std::string> kv_deletes_write;

  // TODO: scan data to be delete and add items to kv_deletes_*

  // after all mutations is processed, write into raft engine
  pb::raft::TxnRaftRequest txn_raft_request;
  auto *cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  auto *write_dels = cf_put_delete->add_deletes_with_cf();
  write_dels->set_cf_name(Constant::kTxnWriteCF);
  for (auto &key_del : kv_deletes_write) {
    write_dels->add_keys(key_del);
  }
  auto *lock_dels = cf_put_delete->add_deletes_with_cf();
  lock_dels->set_cf_name(Constant::kTxnLockCF);
  for (auto &key_del : kv_deletes_lock) {
    lock_dels->add_keys(key_del);
  }
  auto *data_dels = cf_put_delete->add_deletes_with_cf();
  data_dels->set_cf_name(Constant::kTxnDataCF);
  for (auto &key_del : kv_deletes_data) {
    data_dels->add_keys(key_del);
  }

  return raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
}

}  // namespace dingodb
