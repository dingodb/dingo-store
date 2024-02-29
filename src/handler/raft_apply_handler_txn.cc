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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "handler/raft_apply_handler.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_bool(dingo_log_switch_txn_detail);

void TxnHandler::HandleMultiCfPutAndDeleteRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                  std::shared_ptr<RawEngine> engine,
                                                  const pb::raft::MultiCfPutAndDeleteRequest &request,
                                                  [[maybe_unused]] store::RegionMetricsPtr region_metrics,
                                                  int64_t term_id, int64_t log_id) {
  DINGO_LOG(DEBUG) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                  region->Id(), term_id, log_id)
                   << ", request: " << request.ShortDebugString();

  if (request.puts_with_cf_size() > 0) {
    for (const auto &puts : request.puts_with_cf()) {
      for (const auto &kv : puts.kvs()) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}", region->Id(),
                           term_id, log_id)
            << ", put cf: " << puts.cf_name() << ", put key: " << Helper::StringToHex(kv.key())
            << ", value: " << Helper::StringToHex(kv.value());
      }
    }
  }

  if (request.deletes_with_cf_size() > 0) {
    for (const auto &dels : request.deletes_with_cf()) {
      for (const auto &key : dels.keys()) {
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
            << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}", region->Id(),
                           term_id, log_id)
            << ", delete cf: " << dels.cf_name() << ", delete key: " << Helper::StringToHex(key);
      }
    }
  }

  std::map<std::string, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<std::string, std::vector<std::string>> kv_deletes_with_cf;

  for (const auto &puts : request.puts_with_cf()) {
    std::vector<pb::common::KeyValue> kv_puts;
    for (const auto &kv : puts.kvs()) {
      kv_puts.push_back(kv);
    }

    kv_puts_with_cf.insert_or_assign(puts.cf_name(), kv_puts);
  }

  for (const auto &dels : request.deletes_with_cf()) {
    std::vector<std::string> kv_deletes;

    for (const auto &key : dels.keys()) {
      kv_deletes.push_back(key);
    }

    kv_deletes_with_cf.insert_or_assign(dels.cf_name(), kv_deletes);
  }

  auto writer = engine->Writer();
  auto status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleMultiCfPutAndDelete, term: {} apply_log_id: {}",
                                    region->Id(), term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString();
  }

  // check if need to commit to vector index
  const auto &vector_add = request.vector_add();
  if (vector_add.vectors_size() > 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id, log_id)
        << ", commit to vector index count: " << vector_add.vectors_size()
        << ", vector_add: " << vector_add.ShortDebugString();

    auto handler = std::make_shared<VectorAddHandler>();
    if (handler == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                      log_id)
                       << ", new vector add handler failed, vector add count: " << vector_add.vectors_size();
    }

    auto add_ctx = std::make_shared<Context>();
    add_ctx->SetRegionId(region->Id());
    add_ctx->SetCfName(Constant::kVectorDataCF);
    add_ctx->SetRegionEpoch(region->Definition().epoch());

    pb::raft::Request raft_request_for_vector_add;
    for (const auto &vector : vector_add.vectors()) {
      auto *new_vector = raft_request_for_vector_add.mutable_vector_add()->add_vectors();
      *new_vector = vector;
    }
    raft_request_for_vector_add.mutable_vector_add()->set_cf_name(Constant::kVectorDataCF);

    handler->Handle(add_ctx, region, engine, raft_request_for_vector_add, region_metrics, term_id, log_id);
    if (!add_ctx->Status().ok() && ctx != nullptr) {
      ctx->SetStatus(add_ctx->Status());
    }
  }

  const auto &vector_del = request.vector_del();
  if (vector_del.ids_size() > 0) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
        << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id, log_id)
        << ", commit to vector index count: " << vector_del.ids_size()
        << ", vector_del: " << vector_del.ShortDebugString();
    auto handler = std::make_shared<VectorDeleteHandler>();
    if (handler == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[txn][region({})] DoTxnCommit, term: {} apply_log_id: {}", region->Id(), term_id,
                                      log_id)
                       << ", new vector delete handler failed, vector del count: " << vector_del.ids_size();
    }

    auto del_ctx = std::make_shared<Context>();
    del_ctx->SetRegionId(region->Id());
    del_ctx->SetCfName(Constant::kVectorDataCF);
    del_ctx->SetRegionEpoch(region->Definition().epoch());

    pb::raft::Request raft_request_for_vector_del;
    for (const auto &id : vector_del.ids()) {
      raft_request_for_vector_del.mutable_vector_delete()->add_ids(id);
    }
    raft_request_for_vector_del.mutable_vector_delete()->set_cf_name(Constant::kVectorDataCF);

    handler->Handle(del_ctx, region, engine, raft_request_for_vector_del, region_metrics, term_id, log_id);
    if (!del_ctx->Status().ok() && ctx != nullptr) {
      ctx->SetStatus(del_ctx->Status());
    }
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  // if (region_metrics != nullptr) {
  //   region_metrics->UpdateMaxAndMinKey(request.kvs());
  // }
}

void TxnHandler::HandleTxnDeleteRangeRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<RawEngine> engine,
                                             const pb::raft::TxnDeleteRangeRequest &request,
                                             store::RegionMetricsPtr /*region_metrics*/, int64_t term_id,
                                             int64_t log_id) {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] HandleTxnDeleteRange, term: {} apply_log_id: {}", region->Id(), term_id, log_id)
      << ", request: " << request.ShortDebugString();

  auto *response = dynamic_cast<pb::store::TxnDeleteRangeResponse *>(ctx->Response());
  auto *error = response->mutable_error();

  pb::common::Range range;

  range.set_start_key(Helper::EncodeTxnKey(request.start_key(), Constant::kMaxVer));
  range.set_end_key(Helper::EncodeTxnKey(request.end_key(), 0));

  std::vector<pb::common::Range> data_ranges;
  std::vector<pb::common::Range> lock_ranges;
  std::vector<pb::common::Range> write_ranges;

  data_ranges.push_back(range);
  lock_ranges.push_back(range);
  write_ranges.push_back(range);

  std::map<std::string, std::vector<pb::common::Range>> ranges_with_cf;

  ranges_with_cf.insert_or_assign(Constant::kTxnDataCF, data_ranges);
  ranges_with_cf.insert_or_assign(Constant::kTxnLockCF, lock_ranges);
  ranges_with_cf.insert_or_assign(Constant::kTxnWriteCF, write_ranges);

  auto writer = engine->Writer();
  auto status = writer->KvBatchDeleteRange(ranges_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] HandleTxnDeleteRange, term: {} apply_log_id: {}", region->Id(),
                                    term_id, log_id)
                     << ", write failed, request: " << request.ShortDebugString() << ", status: " << status.error_str();
  }
}

int TxnHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                       const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term,
                       int64_t log_id) {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_detail)
      << fmt::format("[txn][region({})] Handle txn, term: {} apply_log_id: {}", region->Id(), term, log_id);

  const auto &txn_raft_req = req.txn_raft_req();

  if (txn_raft_req.has_multi_cf_put_and_delete()) {
    HandleMultiCfPutAndDeleteRequest(ctx, region, engine, txn_raft_req.multi_cf_put_and_delete(), region_metrics, term,
                                     log_id);
  } else if (txn_raft_req.has_mvcc_delete_range()) {
    HandleTxnDeleteRangeRequest(ctx, region, engine, txn_raft_req.mvcc_delete_range(), region_metrics, term, log_id);
  } else {
    DINGO_LOG(FATAL) << fmt::format("[txn][region({})] Unknown txn request", region->Id())
                     << ", txn_raft_req: " << txn_raft_req.DebugString();
  }

  return 0;
}

}  // namespace dingodb
