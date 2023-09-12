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
#include "common/helper.h"
#include "common/logging.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "handler/raft_apply_handler.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"

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
