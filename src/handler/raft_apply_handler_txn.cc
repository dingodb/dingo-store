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

#include <sys/types.h>

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
#include "engine/iterator.h"
#include "engine/txn_engine_helper.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "handler/raft_apply_handler.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DEFINE_uint32(max_short_value_in_write_cf, 1024, "max short value in write cf");

void TxnHandler::HandleMultiCfPutAndDeleteRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                  std::shared_ptr<RawEngine> engine,
                                                  const pb::raft::MultiCfPutAndDeleteRequest &request,
                                                  [[maybe_unused]] store::RegionMetricsPtr region_metrics,
                                                  uint64_t term_id, uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  butil::Status status;

  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &puts : request.puts_with_cf()) {
      for (const auto &kv : puts.kvs()) {
        if (range.end_key().compare(kv.key()) <= 0) {
          if (ctx) {
            status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
            ctx->SetStatus(status);
          }
          return;
        }
      }
    }
    for (const auto &dels : request.deletes_with_cf()) {
      for (const auto &key : dels.keys()) {
        if (range.end_key().compare(key) <= 0) {
          if (ctx) {
            status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
            ctx->SetStatus(status);
          }
          return;
        }
      }
    }
  }

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", new multi cf writer failed, request: " << request.ShortDebugString();
    return;
  }
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<uint32_t, std::vector<std::string>> kv_deletes_with_cf;

  for (const auto &puts : request.puts_with_cf()) {
    if (!kTxnCf2Id.count(puts.cf_name())) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", cf_name: " << puts.cf_name() << " not supported, request: " << request.ShortDebugString();
      continue;
    }

    uint32_t cf_id = kTxnCf2Id.at(puts.cf_name());

    for (const auto &kv : puts.kvs()) {
      kv_puts_with_cf[cf_id].push_back(kv);
    }
  }

  for (const auto &dels : request.deletes_with_cf()) {
    if (!kTxnCf2Id.count(dels.cf_name())) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", cf_name: " << dels.cf_name() << " not supported, request: " << request.ShortDebugString();
      continue;
    }

    uint32_t cf_id = kTxnCf2Id.at(dels.cf_name());

    for (const auto &key : dels.keys()) {
      kv_deletes_with_cf[cf_id].push_back(key);
    }
  }

  status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString();
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  // if (region_metrics != nullptr) {
  //   region_metrics->UpdateMaxAndMinKey(request.kvs());
  // }
}

void TxnHandler::HandleTxnPrewriteRequest([[maybe_unused]] std::shared_ptr<Context> ctx,
                                          [[maybe_unused]] store::RegionPtr region,
                                          [[maybe_unused]] std::shared_ptr<RawEngine> engine,
                                          const pb::raft::TxnPrewriteRequest &request,
                                          [[maybe_unused]] store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                          uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }

  auto write_reader = engine->NewReader(Constant::kTxnWriteCF);
  if (write_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }

  std::vector<pb::common::KeyValue> kv_puts_data;
  std::vector<pb::common::KeyValue> kv_puts_lock;

  const uint64_t &start_ts = request.start_ts();
  const uint64_t &lock_ttl = request.lock_ttl();
  const uint64_t &txn_size = request.mutations_size();
  const bool &try_one_pc = request.try_one_pc();
  const uint64_t &max_commit_ts = request.max_commit_ts();

  auto *response = dynamic_cast<pb::store::TxnPrewriteResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // for every mutation, check and do prewrite, if any one of the mutation is failed, the whole prewrite is failed
  for (const auto &mutation : request.mutations()) {
    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    pb::store::LockInfo lock_info;
    auto ret = TxnEngineHelper::GetLockInfo(lock_reader, mutation.key(), lock_info);
    if (!ret.ok()) {
      // TODO: do read before write to raft state machine
      // Now we need to fatal exit to prevent data inconsistency between raft peers
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", get lock info failed, request: " << request.ShortDebugString()
                       << ", lock_key: " << mutation.key() << ", start_ts: " << start_ts
                       << ", status: " << ret.error_str();
      error->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error->set_errmsg(ret.error_str());

      // need response to client
      return;
    }

    if (!lock_info.primary_lock().empty()) {
      if (lock_info.lock_ts() == start_ts) {
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                       term_id, log_id)
                        << ", key: " << mutation.key()
                        << " is locked by self, lock_info: " << lock_info.ShortDebugString();

        // go to next key
        continue;
      } else {
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                       term_id, log_id)
                        << ", key: " << mutation.key()
                        << " is locked conflict, lock_info: " << lock_info.ShortDebugString();

        // set txn_result for response
        // setup lock_info
        *txn_result->mutable_locked() = lock_info;

        // need response to client
        return;
      }
    }

    // 2. check if the key is committed or rollbacked after start_ts
    //    if the key is committed or rollbacked after start_ts, return WriteConflict
    // 2.1 check rollback
    // if there is a rollback, there will be a key | start_ts : WriteInfo| in write_cf
    pb::store::WriteInfo write_info;
    auto ret1 = TxnEngineHelper::GetRollbackInfo(write_reader, start_ts, mutation.key(), write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", get rollback info failed, request: " << request.ShortDebugString()
                       << ", key: " << mutation.key() << ", start_ts: " << start_ts << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == start_ts) {
      DINGO_LOG(INFO) << "find this transaction is rollbacked,return  SelfRolledBack " << request.ShortDebugString();
      // prewrite meet write_conflict here
      // set txn_result for response
      // setup write_conflict ( this may not be necessary, when lock_info is set)
      auto *write_conflict = txn_result->mutable_write_conflict();
      write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_SelfRolledBack);
      write_conflict->set_start_ts(start_ts);
      write_conflict->set_conflict_ts(start_ts);
      write_conflict->set_key(mutation.key());
      // write_conflict->set_primary_key(lock_info.primary_lock());
      return;
    }

    // 2.2 check commit
    // if there is a commit, there will be a key | commit_ts : WriteInfo| in write_cf
    uint64_t commit_ts = 0;
    auto ret2 = TxnEngineHelper::GetWriteInfo(engine, start_ts, UINT64_MAX, 0, mutation.key(), false, true, true,
                                              write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", get write info failed, request: " << request.ShortDebugString()
                       << ", key: " << mutation.key() << ", start_ts: " << start_ts << ", status: " << ret2.error_str();
    }

    if (commit_ts > start_ts) {
      DINGO_LOG(INFO) << "find this transaction is committed,return  WriteConflict " << request.ShortDebugString();
      // prewrite meet write_conflict here
      // set txn_result for response
      // setup write_conflict ( this may not be necessary, when lock_info is set)
      auto *write_conflict = txn_result->mutable_write_conflict();
      write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_PessimisticRetry);
      write_conflict->set_start_ts(start_ts);
      write_conflict->set_conflict_ts(commit_ts);
      write_conflict->set_key(mutation.key());
      // write_conflict->set_primary_key(lock_info.primary_lock());
      return;
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
        lock_info.set_primary_lock(request.primary_lock());
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
          lock_info.set_primary_lock(request.primary_lock());
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
        lock_info.set_primary_lock(request.primary_lock());
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::Delete);
        kv.set_value(lock_info.SerializeAsString());

        kv_puts_lock.push_back(kv);
      }
    } else {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", invalid op, request: " << request.ShortDebugString() << ", key: " << mutation.key()
                       << ", start_ts: " << start_ts << ", op: " << mutation.op();
    }
  }

  // after all mutations is processed, write into raw engine
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<uint32_t, std::vector<std::string>> kv_deletes_with_cf;

  kv_puts_with_cf.insert_or_assign(Constant::kTxnDataCfId, kv_puts_data);
  kv_puts_with_cf.insert_or_assign(Constant::kTxnLockCfId, kv_puts_lock);

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", new multi cf writer failed, request: " << request.ShortDebugString();
    return;
  }

  auto status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString() << ", status: " << status.error_str();
  }
}

void TxnHandler::HandleTxnCommitRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                        std::shared_ptr<RawEngine> engine, const pb::raft::TxnCommitRequest &request,
                                        store::RegionMetricsPtr region_metrics, uint64_t term_id, uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                 log_id)
                  << ", request: " << request.ShortDebugString();

  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }
  auto data_reader = engine->NewReader(Constant::kTxnDataCF);

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  const uint64_t &start_ts = request.start_ts();
  const uint64_t &commit_ts = request.commit_ts();

  auto *response = dynamic_cast<pb::store::TxnPrewriteResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // for vector index region, commit to vector index
  pb::raft::Request raft_request_for_vector_add;
  pb::raft::Request raft_request_for_vector_del;
  auto *vector_add = raft_request_for_vector_add.mutable_vector_add();
  auto *vector_del = raft_request_for_vector_del.mutable_vector_delete();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  std::vector<pb::store::LockInfo> lock_infos;
  for (const auto &key : request.keys()) {
    pb::store::LockInfo lock_info;
    auto ret = TxnEngineHelper::GetLockInfo(lock_reader, key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", get lock info failed, request: " << request.ShortDebugString() << ", key: " << key
                       << ", start_ts: " << start_ts << ", status: " << ret.error_str();
    }

    // if lock is not exist, return TxnNotFound
    if (lock_info.primary_lock().empty()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                        term_id, log_id)
                         << ", txn_not_found with lock_info empty, request: " << request.ShortDebugString()
                         << ", key: " << key << ", start_ts: " << start_ts;

      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_start_ts(start_ts);

      return;
    }

    // if lock is exists but start_ts is not equal to lock_ts, return TxnNotFound
    if (lock_info.lock_ts() != start_ts) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                        term_id, log_id)
                         << ", txn_not_found with lock_info.lock_ts not equal to start_ts, request: "
                         << request.ShortDebugString() << ", key: " << key << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();

      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_start_ts(start_ts);

      return;
    }

    // check if the key is already committed, if it is committed can skip it
    pb::store::WriteInfo write_info;
    uint64_t commit_ts = 0;
    auto ret2 = TxnEngineHelper::GetWriteInfo(engine, start_ts, UINT64_MAX, start_ts, key, false, true, true,
                                              write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", get write info failed, request: " << request.ShortDebugString()
                       << ", key: " << Helper::StringToHex(key) << ", start_ts: " << start_ts
                       << ", status: " << ret2.error_str();
    }

    // if commit_ts > 0, means this key of start_ts is already committed, can skip it
    if (commit_ts > 0) {
      DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                     term_id, log_id)
                      << ", key: " << key << " is already committed, commit_ts: " << commit_ts
                      << ", start_ts: " << start_ts << ", request: " << request.ShortDebugString();
      continue;
    }

    // now txn is match, prepare to commit
    lock_infos.push_back(lock_info);
  }

  auto ret = DoTxnCommit(ctx, region, engine, lock_infos, start_ts, commit_ts, region_metrics, term_id, log_id);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", do txn commit failed, request: " << request.ShortDebugString()
                     << ", status: " << ret.error_str();
  }
}

butil::Status TxnHandler::DoTxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                      std::shared_ptr<RawEngine> engine,
                                      const std::vector<pb::store::LockInfo> &lock_infos, uint64_t start_ts,
                                      uint64_t commit_ts, store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                      uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                 log_id)
                  << ", lock_infos count: " << lock_infos.size();

  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                    log_id)
                     << ", new reader failed, lock_infos count: " << lock_infos.size();
  }
  auto data_reader = engine->NewReader(Constant::kTxnDataCF);

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  // for vector index region, commit to vector index
  pb::raft::Request raft_request_for_vector_add;
  pb::raft::Request raft_request_for_vector_del;
  auto *vector_add = raft_request_for_vector_add.mutable_vector_add();
  auto *vector_del = raft_request_for_vector_del.mutable_vector_delete();

  // for every key, check and do commit, if primary key is failed, the whole commit is failed
  for (const auto &lock_info : lock_infos) {
    // 1.put data to write_cf
    std::string data_value;
    if (lock_info.short_value().length() > 0) {
      data_value = lock_info.short_value();
    } else if (lock_info.lock_type() == pb::store::Put) {
      auto ret = data_reader->KvGet(Helper::EncodeTxnKey(lock_info.key(), start_ts), data_value);
      if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                        term_id, log_id)
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

      if (region->Type() == pb::common::INDEX_REGION) {
        if (lock_info.lock_type() == pb::store::Op::Put) {
          pb::common::VectorWithId vector_with_id;
          auto ret = vector_with_id.ParseFromString(data_value);
          if (!ret) {
            DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                            term_id, log_id)
                             << ", parse vector_with_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", start_ts: " << start_ts << ", data_value: " << Helper::StringToHex(data_value)
                             << ", lock_info: " << lock_info.ShortDebugString();
          }

          *(vector_add->add_vectors()) = vector_with_id;
        } else if (lock_info.lock_type() == pb::store::Op::Delete) {
          auto vector_id = Helper::DecodeVectorId(lock_info.key());
          if (vector_id == 0) {
            DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                            term_id, log_id)
                             << ", decode vector_id failed, key: " << Helper::StringToHex(lock_info.key())
                             << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();
          }

          vector_del->add_ids(vector_id);
        } else {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(),
                                          term_id, log_id)
                           << ", invalid lock_type, key: " << Helper::StringToHex(lock_info.key())
                           << ", start_ts: " << start_ts << ", lock_info: " << lock_info.ShortDebugString();
        }
      }
    }

    // 3.delete lock from lock_cf
    { kv_deletes_lock.push_back(Helper::EncodeTxnKey(lock_info.key(), Constant::kLockVer)); }
  }

  // after all mutations is processed, write into raw engine
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<uint32_t, std::vector<std::string>> kv_deletes_with_cf;

  kv_puts_with_cf.insert_or_assign(Constant::kTxnWriteCfId, kv_puts_write);
  kv_deletes_with_cf.insert_or_assign(Constant::kTxnLockCfId, kv_deletes_lock);

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", new multi cf writer failed, lock_infos count: " << lock_infos.size();
  }

  auto status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCOmmit, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", write failed, status: " << status.error_str() << ", lock_infos count: " << lock_infos.size();
  }

  // check if need to commit to vector index
  if (vector_add->vectors_size() > 0) {
    DINGO_LOG(INFO) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                   log_id)
                    << ", commit to vector index count: " << vector_add->vectors_size()
                    << ", vector_add: " << vector_add->ShortDebugString();
    auto handler = std::make_shared<VectorAddHandler>();
    if (handler == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                      log_id)
                       << ", new vector add handler failed, lock_infos count: " << lock_infos.size();
    }
    auto add_ctx = std::make_shared<Context>();
    add_ctx->SetRegionId(ctx->RegionId()).SetCfName(Constant::kStoreDataCF);
    add_ctx->SetRegionEpoch(ctx->RegionEpoch());
    add_ctx->SetIsolationLevel(ctx->IsolationLevel());

    handler->Handle(add_ctx, region, engine, raft_request_for_vector_add, region_metrics, term_id, log_id);
    if (!add_ctx->Status().ok()) {
      ctx->SetStatus(add_ctx->Status());
    }
  }

  if (vector_del->ids_size() > 0) {
    DINGO_LOG(INFO) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                   log_id)
                    << ", commit to vector index count: " << vector_del->ids_size()
                    << ", vector_del: " << vector_del->ShortDebugString();
    auto handler = std::make_shared<VectorDeleteHandler>();
    if (handler == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                      log_id)
                       << ", new vector delete handler failed, lock_infos count: " << lock_infos.size();
    }

    auto del_ctx = std::make_shared<Context>();
    del_ctx->SetRegionId(ctx->RegionId()).SetCfName(Constant::kStoreDataCF);
    del_ctx->SetRegionEpoch(ctx->RegionEpoch());
    del_ctx->SetIsolationLevel(ctx->IsolationLevel());

    handler->Handle(del_ctx, region, engine, raft_request_for_vector_add, region_metrics, term_id, log_id);
    if (!del_ctx->Status().ok()) {
      ctx->SetStatus(del_ctx->Status());
    }
  }

  return butil::Status::OK();
}

void TxnHandler::HandleTxnCheckTxnStatusRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                std::shared_ptr<RawEngine> engine,
                                                const pb::raft::TxnCheckTxnStatusRequest &request,
                                                store::RegionMetricsPtr /*region_metrics*/, uint64_t term_id,
                                                uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  // we need to do if primay_key is in this region'range in service before apply to raft state machine
  // use reader to get if the lock is exists, if lock is exists, check if the lock is expired its ttl, if expired do
  // rollback and return if not expired, return conflict if the lock is not exists, return commited the the lock's ts is
  // matched, but it is not a primary_key, return PrimaryMismatch

  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }
  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  auto write_reader = engine->NewReader(Constant::kTxnWriteCF);

  const std::string &primary_key = request.primary_key();
  const uint64_t &lock_ts = request.lock_ts();
  const uint64_t &caller_start_ts = request.caller_start_ts();
  const uint64_t &current_ts = request.current_ts();

  auto *response = dynamic_cast<pb::store::TxnCheckTxnStatusResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // get lock info
  pb::store::LockInfo lock_info;
  auto ret = TxnEngineHelper::GetLockInfo(lock_reader, primary_key, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", get lock info failed, request: " << request.ShortDebugString()
                     << ", primary_key: " << primary_key << ", lock_ts: " << lock_ts << ", status: " << ret.error_str();
  }

  if (lock_info.lock_ts() == lock_ts) {
    // the lock is exists, check if it is expired, if not expired, return conflict, if expired, do rollback
    // check if this is a primary key
    if (lock_info.key() != lock_info.primary_lock()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                        region->Id(), term_id, log_id)
                         << ", primary mismatch, request: " << request.ShortDebugString()
                         << ", primary_key: " << primary_key << ", lock_ts: " << lock_ts
                         << ", lock_info: " << lock_info.ShortDebugString();

      auto *primary_mismatch = txn_result->mutable_primary_mismatch();
      *(primary_mismatch->mutable_lock_info()) = lock_info;
      return;
    }

    uint64_t current_ms = request.current_ts() >> 18;

    DINGO_LOG(INFO) << "lock is exists, check ttl, lock_info: " << lock_info.ShortDebugString()
                    << ", request: " << request.ShortDebugString() << ", current_ms: " << current_ms;

    if (lock_info.lock_ttl() >= current_ms) {
      DINGO_LOG(INFO) << "lock is not expired, return conflict, lock_info: " << lock_info.ShortDebugString()
                      << ", request: " << request.ShortDebugString() << ", current_ms: " << current_ms;

      response->set_lock_ttl(lock_info.lock_ttl());
      response->set_commit_ts(0);
      response->set_action(::dingodb::pb::store::Action::NoAction);
      return;
    }

    DINGO_LOG(INFO) << "lock is expired, do rollback, lock_info: " << lock_info.ShortDebugString()
                    << ", request: " << request.ShortDebugString() << ", current_ms: " << current_ms;

    // lock is expired, do rollback
    std::vector<std::string> keys_to_rollback_with_data;
    std::vector<std::string> keys_to_rollback_without_data;
    if (lock_info.short_value().empty()) {
      keys_to_rollback_with_data.push_back(primary_key);
    } else {
      keys_to_rollback_without_data.push_back(primary_key);
    }
    auto ret = TxnEngineHelper::Rollback(engine, keys_to_rollback_with_data, keys_to_rollback_without_data, lock_ts);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", rollback failed, request: " << request.ShortDebugString()
                       << ", primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret.error_str();
    }

    response->set_lock_ttl(0);
    response->set_commit_ts(0);
    response->set_action(::dingodb::pb::store::Action::TTLExpireRollback);
    return;
  } else {
    // the lock is not exists, check if it is rollbacked or committed
    // try to get if there is a rollback to lock_ts
    pb::store::WriteInfo write_info;
    auto ret1 = TxnEngineHelper::GetRollbackInfo(write_reader, lock_ts, primary_key, write_info);
    if (!ret1.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", get rollback info failed, request: " << request.ShortDebugString()
                       << ", primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret1.error_str();
    }

    if (write_info.start_ts() == lock_ts) {
      // rollback, return rollback
      response->set_lock_ttl(0);
      response->set_commit_ts(0);
      response->set_action(::dingodb::pb::store::Action::LockNotExistDoNothing);
      return;
    }

    // if there is not a rollback to lock_ts, try to get the commit_ts
    uint64_t commit_ts = 0;
    auto ret2 = TxnEngineHelper::GetWriteInfo(engine, lock_ts, UINT64_MAX, lock_ts, primary_key, false, true, true,
                                              write_info, commit_ts);
    if (!ret2.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnCheckTxnStatus, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", get write info failed, request: " << request.ShortDebugString()
                       << ", primary_key: " << primary_key << ", lock_ts: " << lock_ts
                       << ", status: " << ret2.error_str();
    }

    if (commit_ts == 0) {
      // it seems there is a lock previously exists, but it is not committed, and there is no rollback, there must be
      // some error, return TxnNotFound
      auto *txn_not_found = txn_result->mutable_txn_not_found();
      txn_not_found->set_primary_key(request.primary_key());
      txn_not_found->set_start_ts(request.lock_ts());
      return;
    }

    // commit, return committed
    response->set_lock_ttl(0);
    response->set_commit_ts(commit_ts);
    response->set_action(::dingodb::pb::store::Action::NoAction);
    return;
  }
}

void TxnHandler::HandleTxnResolveLockRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<RawEngine> engine,
                                             const pb::raft::TxnResolveLockRequest &request,
                                             store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                             uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  // if commit_ts = 0, do rollback else do commit
  // scan lock_cf to search if transaction with start_ts is exists, if exists, do rollback or commit
  // if not exists, do nothing
  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleResolveLock, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }
  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  auto write_reader = engine->NewReader(Constant::kTxnWriteCF);

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  const uint64_t &start_ts = request.start_ts();
  const uint64_t &commit_ts = request.commit_ts();

  auto *response = dynamic_cast<pb::store::TxnResolveLockResponse *>(ctx->Response());
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
  if (request.keys_size() > 0) {
    for (const auto &key : request.keys()) {
      pb::store::LockInfo lock_info;
      auto ret = TxnEngineHelper::GetLockInfo(lock_reader, key, lock_info);
      if (!ret.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}",
                                        region->Id(), term_id, log_id)
                         << ", get lock info failed, request: " << request.ShortDebugString() << ", key: " << key
                         << ", start_ts: " << start_ts << ", status: " << ret.error_str();
      }

      // if lock is not exist, nothing to do
      if (lock_info.primary_lock().empty()) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}",
                                          region->Id(), term_id, log_id)
                           << ", txn_not_found with lock_info empty, request: " << request.ShortDebugString()
                           << ", key: " << key << ", start_ts: " << start_ts;

        // auto *txn_not_found = txn_result->mutable_txn_not_found();
        // txn_not_found->set_start_ts(start_ts);
        continue;
      }

      if (lock_info.lock_ts() != start_ts) {
        DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}",
                                          region->Id(), term_id, log_id)
                           << ", txn_not_found with lock_info.lock_ts not equal to start_ts, request: "
                           << request.ShortDebugString() << ", key: " << key << ", start_ts: " << start_ts
                           << ", lock_info: " << lock_info.ShortDebugString();
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
    auto ret = TxnEngineHelper::ScanLockInfo(engine, start_ts, start_ts + 1, region->Range().start_key(),
                                             region->Range().end_key(), 0, tmp_lock_infos);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", get lock info failed, request: " << request.ShortDebugString()
                       << ", start_ts: " << start_ts << ", status: " << ret.error_str();
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
    auto ret =
        DoTxnCommit(ctx, region, engine, lock_infos_to_commit, start_ts, commit_ts, region_metrics, term_id, log_id);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", do txn commit failed, request: " << request.ShortDebugString()
                       << ", start_ts: " << start_ts << ", status: " << ret.error_str();
    }
  }

  if (!keys_to_rollback_with_data.empty() || !keys_to_rollback_without_data.empty()) {
    auto ret = TxnEngineHelper::Rollback(engine, keys_to_rollback_with_data, keys_to_rollback_without_data, start_ts);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnResolveLock, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", rollback failed, request: " << request.ShortDebugString() << ", start_ts: " << start_ts
                       << ", status: " << ret.error_str();
    }
  }
}

void TxnHandler::HandleTxnBatchRollbackRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                               std::shared_ptr<RawEngine> engine,
                                               const pb::raft::TxnBatchRollbackRequest &request,
                                               store::RegionMetricsPtr /*region_metrics*/, uint64_t term_id,
                                               uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  // create reader and writer
  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleBatchRollback, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }
  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  auto write_reader = engine->NewReader(Constant::kTxnWriteCF);

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  const uint64_t &start_ts = request.start_ts();

  auto *response = dynamic_cast<pb::store::TxnBatchRollbackResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  std::vector<std::string> keys_to_rollback;

  // if keys is not empty, we only do resolve lock for these keys
  if (request.keys_size() == 0) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", nothing to do, request: " << request.ShortDebugString();
    return;
  }

  std::vector<std::string> keys_to_rollback_with_data;
  std::vector<std::string> keys_to_rollback_without_data;
  for (const auto &key : request.keys()) {
    pb::store::LockInfo lock_info;
    auto ret = TxnEngineHelper::GetLockInfo(lock_reader, key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}",
                                      region->Id(), term_id, log_id)
                       << ", get lock info failed, request: " << request.ShortDebugString() << ", key: " << key
                       << ", start_ts: " << start_ts << ", status: " << ret.error_str();
    }

    // if lock is not exist, nothing to do
    if (lock_info.primary_lock().empty()) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}",
                                        region->Id(), term_id, log_id)
                         << ", txn_not_found with lock_info empty, request: " << request.ShortDebugString()
                         << ", key: " << key << ", start_ts: " << start_ts;

      // auto *txn_not_found = txn_result->mutable_txn_not_found();
      // txn_not_found->set_start_ts(start_ts);
      continue;
    }

    if (lock_info.lock_ts() != start_ts) {
      DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}",
                                        region->Id(), term_id, log_id)
                         << ", txn_not_found with lock_info.lock_ts not equal to start_ts, request: "
                         << request.ShortDebugString() << ", key: " << key << ", start_ts: " << start_ts
                         << ", lock_info: " << lock_info.ShortDebugString();
      continue;
    }

    if (lock_info.short_value().empty()) {
      keys_to_rollback_with_data.push_back(key);
    } else {
      keys_to_rollback_without_data.push_back(key);
    }
  }

  // do rollback
  auto ret = TxnEngineHelper::Rollback(engine, keys_to_rollback_with_data, keys_to_rollback_without_data, start_ts);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnBatchRollback, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", rollback failed, request: " << request.ShortDebugString() << ", status: " << ret.error_str();
  }
}

void TxnHandler::HandleTxnHeartBeatRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                           std::shared_ptr<RawEngine> engine,
                                           const pb::raft::TxnHeartBeatRequest &request,
                                           store::RegionMetricsPtr /*region_metrics*/, uint64_t term_id,
                                           uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new reader failed, request: " << request.ShortDebugString();
  }

  auto *response = dynamic_cast<pb::store::TxnHeartBeatResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  const std::string &primary_lock = request.primary_lock();
  const uint64_t &start_ts = request.start_ts();
  const uint64_t &advise_lock_ttl = request.advise_lock_ttl();

  pb::store::LockInfo lock_info;
  auto ret = TxnEngineHelper::GetLockInfo(lock_reader, primary_lock, lock_info);
  if (!ret.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", get lock info failed, request: " << request.ShortDebugString()
                     << ", primary_lock: " << primary_lock << ", start_ts: " << start_ts
                     << ", status: " << ret.error_str();
  }

  if (lock_info.primary_lock().empty()) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", txn_not_found with lock_info empty, request: " << request.ShortDebugString()
                       << ", primary_lock: " << primary_lock << ", start_ts: " << start_ts;

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
    return;
  }

  if (lock_info.lock_ts() != start_ts) {
    DINGO_LOG(WARNING) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", txn_not_found with lock_info.lock_ts not equal to start_ts, request: "
                       << request.ShortDebugString() << ", primary_lock: " << primary_lock << ", start_ts: " << start_ts
                       << ", lock_info: " << lock_info.ShortDebugString();

    auto *txn_not_found = txn_result->mutable_txn_not_found();
    txn_not_found->set_start_ts(start_ts);
    return;
  }

  // update lock_info
  lock_info.set_lock_ttl(advise_lock_ttl);

  pb::common::KeyValue kv;
  kv.set_key(Helper::EncodeTxnKey(primary_lock, Constant::kLockVer));
  kv.set_value(lock_info.SerializeAsString());

  auto writer = engine->NewWriter(Constant::kTxnLockCF);
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new writer failed, request: " << request.ShortDebugString();
    return;
  }

  auto status = writer->KvPut(kv);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnHeartBeat, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString() << ", status: " << status.error_str();
  }
}

void TxnHandler::HandleTxnDeleteRangeRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<RawEngine> engine,
                                             const pb::raft::TxnDeleteRangeRequest &request,
                                             store::RegionMetricsPtr /*region_metrics*/, uint64_t term_id,
                                             uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnDeleteRange, term: {} apply_log_id: {}", region->Id(),
                                 term_id, log_id)
                  << ", request: " << request.ShortDebugString();

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnDeleteRange, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", new multi cf writer failed, request: " << request.ShortDebugString();
    return;
  }

  auto *response = dynamic_cast<pb::store::TxnDeleteRangeResponse *>(ctx->Response());
  auto *error = response->mutable_error();

  pb::common::Range range;

  range.set_start_key(Helper::EncodeTxnKey(request.start_key(), UINT64_MAX));
  range.set_end_key(Helper::EncodeTxnKey(request.end_key(), 0));

  std::vector<pb::common::Range> data_ranges;
  std::vector<pb::common::Range> lock_ranges;
  std::vector<pb::common::Range> write_ranges;

  data_ranges.push_back(range);
  lock_ranges.push_back(range);
  write_ranges.push_back(range);

  std::map<uint32_t, std::vector<pb::common::Range>> ranges_with_cf;

  ranges_with_cf.insert_or_assign(Constant::kTxnDataCfId, data_ranges);
  ranges_with_cf.insert_or_assign(Constant::kTxnLockCfId, lock_ranges);
  ranges_with_cf.insert_or_assign(Constant::kTxnWriteCfId, write_ranges);

  auto status = writer->KvBatchDeleteRange(ranges_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnDeleteRange, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString() << ", status: " << status.error_str();
  }
}

int TxnHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                       const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term,
                       uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[txn][region({})] Handle txn, term: {} apply_log_id: {}", region->Id(), term, log_id);

  const auto &txn_raft_req = req.txn_raft_req();

  if (txn_raft_req.has_multi_cf_put_and_delete()) {
    HandleMultiCfPutAndDeleteRequest(ctx, region, engine, txn_raft_req.multi_cf_put_and_delete(), region_metrics, term,
                                     log_id);
  } else if (txn_raft_req.has_prewrite()) {
    HandleTxnPrewriteRequest(ctx, region, engine, txn_raft_req.prewrite(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_commit()) {
    HandleTxnCommitRequest(ctx, region, engine, txn_raft_req.commit(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_check_txn_status()) {
    HandleTxnCheckTxnStatusRequest(ctx, region, engine, txn_raft_req.check_txn_status(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_resolve_lock()) {
    HandleTxnResolveLockRequest(ctx, region, engine, txn_raft_req.resolve_lock(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_rollback()) {
    HandleTxnBatchRollbackRequest(ctx, region, engine, txn_raft_req.rollback(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_lock_heartbeat()) {
    // TODO: implement lock_heartbeat
    HandleTxnHeartBeatRequest(ctx, region, engine, txn_raft_req.lock_heartbeat(), region_metrics, term, log_id);
  } else if (txn_raft_req.has_mvcc_delete_range()) {
    HandleTxnDeleteRangeRequest(ctx, region, engine, txn_raft_req.mvcc_delete_range(), region_metrics, term, log_id);
  } else {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Unknown txn request", region->Id())
                     << ", txn_raft_req: " << txn_raft_req.DebugString();
  }

  return 0;
}

}  // namespace dingodb
