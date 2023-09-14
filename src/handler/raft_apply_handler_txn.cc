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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "handler/raft_apply_handler.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

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
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_deletes_with_cf;

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
      pb::common::KeyValue kv;
      // kv.mutable_key()->swap(const_cast<std::string &>(key));
      kv.mutable_key()->assign(key);
      kv_deletes_with_cf[cf_id].push_back(kv);
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
  // auto write_reader = engine->NewReader(Constant::kTxnWriteCF);

  std::vector<pb::common::KeyValue> kv_puts_data;
  std::vector<pb::common::KeyValue> kv_puts_lock;

  uint64_t start_ts = request.start_ts();
  uint64_t lock_ttl = request.lock_ttl();
  uint64_t txn_size = request.mutations_size();
  bool try_one_pc = request.try_one_pc();
  uint64_t max_commit_ts = request.max_commit_ts();

  auto *response = dynamic_cast<pb::store::TxnPrewriteResponse *>(ctx->Response());
  auto *error = response->mutable_error();
  auto *txn_result = response->mutable_txn_result();

  // for every mutation, check and do prewrite, if any one of the mutation is failed, the whole prewrite is failed
  for (const auto &mutation : request.mutations()) {
    // 1.check if the key is locked
    //   if the key is locked, return LockInfo
    std::string lock_value;
    auto status = lock_reader->KvGet(mutation.key(), lock_value);
    // if lock_value is not found or it is empty, then the key is not locked
    // else the key is locked, return WriteConflict
    if (status.ok()) {
      if (!lock_value.empty()) {
        pb::store::LockInfo lock_info;
        auto ret = lock_info.ParseFromString(lock_value);
        if (!ret) {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}",
                                          region->Id(), term_id, log_id)
                           << ", parse lock info failed, request: " << request.ShortDebugString()
                           << ", lock_key: " << mutation.key() << ", lock_value: " << lock_value
                           << ", start_ts: " << start_ts;
        }

        // set txn_result for response
        // setup lock_info
        *txn_result->mutable_locked() = lock_info;

        // setup write_conflict ( this may not be necessary, when lock_info is set)
        // auto *write_conflict = txn_result->mutable_write_conflict();
        // write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        // write_conflict->set_start_ts(start_ts);
        // write_conflict->set_conflict_ts(lock_info.lock_ts());
        // write_conflict->set_key(mutation.key());
        // write_conflict->set_primary_key(lock_info.primary_lock());

        // need response to client
        return;
      } else {
        // lock_value is empty, the key is not locked
        DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                       term_id, log_id)
                        << ", key: " << mutation.key() << " is not locked, lock_value is null, start_ts: " << start_ts;
      }
    } else if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
      // lock_value is empty, the key is not locked
      DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                     term_id, log_id)
                      << ", key: " << mutation.key() << " is not locked, lock_key is not exist, start_ts: " << start_ts;
    } else {
      // other error, return error
      DINGO_LOG(ERROR) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", read lock_key failed, request: " << request.ShortDebugString()
                       << ", lock_key: " << mutation.key() << ", start_ts: " << start_ts
                       << ", status: " << status.error_str();
      error->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
      error->set_errmsg(status.error_str());

      // need response to client
      return;
    }

    // 2. check if the key is committed after start_ts
    //    if the key is committed after start_ts, return WriteConflict
    std::string write_value;
    IteratorOptions iter_options;
    iter_options.lower_bound = mutation.key();
    iter_options.upper_bound = Helper::EncodeTxnKey(mutation.key(), UINT64_MAX);
    auto iter = engine->NewIterator(Constant::kTxnWriteCF, iter_options);
    if (iter == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                      term_id, log_id)
                       << ", new iterator failed, request: " << request.ShortDebugString()
                       << ", key: " << mutation.key() << ", start_ts: " << start_ts;
    }

    // if the key is committed after start_ts, return WriteConflict
    if (iter->Valid()) {
      if (iter->Key().length() <= 8) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                        term_id, log_id)
                         << ", invalid key, request: " << request.ShortDebugString() << ", key: " << mutation.key()
                         << ", start_ts: " << start_ts << ", write_key is less than 8 bytes: " << iter->Key();
      }
      std::string write_key;
      uint64_t write_ts;
      Helper::DecodeTxnKey(iter->Key(), write_key, write_ts);

      if (write_ts == start_ts) {
        DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}", region->Id(),
                                        term_id, log_id)
                         << ", invalid key, request: " << request.ShortDebugString() << ", key: " << mutation.key()
                         << ", start_ts: " << start_ts << ", write_ts is equal to start_ts: " << write_ts;
      } else if (write_ts > start_ts) {
        // prewrite meet write_conflict here
        // set txn_result for response
        // setup write_conflict ( this may not be necessary, when lock_info is set)
        auto *write_conflict = txn_result->mutable_write_conflict();
        write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        write_conflict->set_start_ts(start_ts);
        write_conflict->set_conflict_ts(write_ts);
        write_conflict->set_key(mutation.key());
        // write_conflict->set_primary_key(lock_info.primary_lock());

        // need response to client
        return;
      }
    }

    // 3.do Put/Delete/PutIfAbsent
    if (mutation.op() == pb::store::Op::Put) {
      // put data
      {
        pb::common::KeyValue kv;
        std::string data_key = Helper::EncodeTxnKey(mutation.key(), start_ts);
        kv.set_key(data_key);
        kv.set_value(mutation.value());

        kv_puts_data.push_back(kv);
      }

      // put lock
      {
        pb::common::KeyValue kv;
        kv.set_key(mutation.key());

        pb::store::LockInfo lock_info;
        lock_info.set_primary_lock(request.primary_lock());
        lock_info.set_lock_ts(start_ts);
        lock_info.set_key(mutation.key());
        lock_info.set_lock_ttl(lock_ttl);
        lock_info.set_txn_size(txn_size);
        lock_info.set_lock_type(pb::store::Op::Put);
        kv.set_value(lock_info.SerializeAsString());

        kv_puts_lock.push_back(kv);
      }
    } else if (mutation.op() == pb::store::Op::PutIfAbsent) {
      // check if key is exist
      bool key_exist = false;

      while (iter->Valid()) {
        pb::store::WriteInfo write_info;
        auto ret = write_info.ParseFromArray(iter->Value().data(), iter->Value().size());
        if (!ret) {
          DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnPrewrite, term: {} apply_log_id: {}",
                                          region->Id(), term_id, log_id)
                           << ", parse write info failed, request: " << request.ShortDebugString()
                           << ", key: " << mutation.key() << ", start_ts: " << start_ts
                           << ", write_info: " << iter->Value();
        }

        if (write_info.op() == pb::store::Op::Delete) {
          break;
        } else if (write_info.op() == pb::store::Op::Put) {
          key_exist = true;
          break;
        } else {
          iter->Next();
          continue;
        }
      }

      if (key_exist) {
        response->add_keys_already_exist()->set_key(mutation.key());
        // this mutation is success with key_exist, go to next mutation
        continue;
      } else {
        // put data
        {
          pb::common::KeyValue kv;
          std::string data_key = Helper::EncodeTxnKey(mutation.key(), start_ts);
          kv.set_key(data_key);
          kv.set_value(mutation.value());

          kv_puts_data.push_back(kv);
        }

        // put lock
        {
          pb::common::KeyValue kv;
          kv.set_key(mutation.key());

          pb::store::LockInfo lock_info;
          lock_info.set_primary_lock(request.primary_lock());
          lock_info.set_lock_ts(start_ts);
          lock_info.set_key(mutation.key());
          lock_info.set_lock_ttl(lock_ttl);
          lock_info.set_txn_size(txn_size);
          lock_info.set_lock_type(pb::store::Op::Put);
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
        kv.set_key(mutation.key());

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
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_deletes_with_cf;

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
  // DINGO_LOG(INFO) << fmt::format("[txn][region({})] HandleTxnCommit, term: {} apply_log_id: {}", region->Id(),
  // term_id,
  //                                log_id)
  //                 << ", request: " << request.ShortDebugString();
}

void TxnHandler::HandleTxnCheckTxnStatusRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                std::shared_ptr<RawEngine> engine,
                                                const pb::raft::TxnCheckTxnStatusRequest &request,
                                                store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                                uint64_t log_id) {}

void TxnHandler::HandleTxnResolveLockRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<RawEngine> engine,
                                             const pb::raft::TxnResolveLockRequest &request,
                                             store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                             uint64_t log_id) {}

void TxnHandler::HandleTxnBatchRollbackRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                               std::shared_ptr<RawEngine> engine,
                                               const pb::raft::TxnBatchRollbackRequest &request,
                                               store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                               uint64_t log_id) {}

void TxnHandler::HandleTxnHeartBeatRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                           std::shared_ptr<RawEngine> engine,
                                           const pb::raft::TxnHeartBeatRequest &request,
                                           store::RegionMetricsPtr region_metrics, uint64_t term_id, uint64_t log_id) {}

void TxnHandler::HandleTxnDeleteRangeRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<RawEngine> engine,
                                             const pb::raft::TxnDeleteRangeRequest &request,
                                             store::RegionMetricsPtr region_metrics, uint64_t term_id,
                                             uint64_t log_id) {}

void TxnHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
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
    HandleTxnCommitRequest(ctx, region, engine, txn_raft_req.commit(), region_metrics, term, log_id);
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
}

}  // namespace dingodb
