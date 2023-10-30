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

#include "engine/storage.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/iterator.h"
#include "engine/raft_store_engine.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
#include "serial/buf.h"
#include "server/server.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

Storage::Storage(std::shared_ptr<Engine> engine) : engine_(engine) {}

Snapshot* Storage::GetSnapshot() { return nullptr; }

void Storage::ReleaseSnapshot() {}

butil::Status Storage::ValidateLeader(int64_t region_id) {
  if (engine_->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine_);
    auto node = raft_kv_engine->GetNode(region_id);
    if (node == nullptr) {
      return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
    }

    if (!node->IsLeader()) {
      return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
    }
  }

  return butil::Status();
}

bool Storage::IsLeader(int64_t region_id) {
  if (engine_ == nullptr || engine_->GetID() != pb::common::ENG_RAFT_STORE) {
    return false;
  }
  auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine_);
  return raft_kv_engine->IsLeader(region_id);
}

butil::Status Storage::KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                             std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }
  auto reader = engine_->NewReader();
  for (const auto& key : keys) {
    std::string value;
    auto status = reader->KvGet(ctx, key, value);
    if (!status.ok()) {
      if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
        continue;
      }
      kvs.clear();
      return status;
    }

    pb::common::KeyValue kv;
    kv.set_key(key);
    kv.set_value(value);
    kvs.emplace_back(kv);
  }

  return butil::Status();
}

butil::Status Storage::KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs));
  }

  return engine_->AsyncWrite(
      ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs), [](std::shared_ptr<Context> ctx, butil::Status status) {
        if (!status.ok()) {
          Helper::SetPbMessageError(status, ctx->Response());
          if (ctx->Request() != nullptr && ctx->Response() != nullptr) {
            LOG(ERROR) << fmt::format("KvPut request: {} response: {}", ctx->Request()->ShortDebugString(),
                                      ctx->Response()->ShortDebugString());
          }
        }
      });
}

butil::Status Storage::KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                     bool is_atomic) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs, is_atomic));
  }

  return engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs, is_atomic),
                             [](std::shared_ptr<Context> ctx, butil::Status status) {
                               if (!status.ok()) {
                                 Helper::SetPbMessageError(status, ctx->Response());
                                 if (ctx->Request() != nullptr && ctx->Response() != nullptr) {
                                   LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}",
                                                             ctx->Request()->ShortDebugString(),
                                                             ctx->Response()->ShortDebugString());
                                 }
                               }
                             });
}

butil::Status Storage::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), keys));
  }

  return engine_->AsyncWrite(
      ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), keys), [](std::shared_ptr<Context> ctx, butil::Status status) {
        if (!status.ok()) {
          Helper::SetPbMessageError(status, ctx->Response());
          if (ctx->Request() != nullptr && ctx->Response() != nullptr) {
            LOG(ERROR) << fmt::format("KvDelete request: {} response: {}", ctx->Request()->ShortDebugString(),
                                      ctx->Response()->ShortDebugString());
          }
        }
      });
}

butil::Status Storage::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), range));
  }

  return engine_->AsyncWrite(
      ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), range), [](std::shared_ptr<Context> ctx, butil::Status status) {
        if (!status.ok()) {
          Helper::SetPbMessageError(status, ctx->Response());
          if (ctx->Request() != nullptr && ctx->Response() != nullptr) {
            LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", ctx->Request()->ShortDebugString(),
                                      ctx->Response()->ShortDebugString());
          }
        }
      });
}

butil::Status Storage::KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                       const std::vector<std::string>& expect_values, bool is_atomic) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs, expect_values, is_atomic));
  }

  return engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs, expect_values, is_atomic),
                             [](std::shared_ptr<Context> ctx, butil::Status status) {
                               if (!status.ok()) {
                                 Helper::SetPbMessageError(status, ctx->Response());
                                 if (ctx->Request() != nullptr && ctx->Response() != nullptr) {
                                   LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}",
                                                             ctx->Request()->ShortDebugString(),
                                                             ctx->Response()->ShortDebugString());
                                 }
                               }
                             });
}

butil::Status Storage::KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                                   const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                                   bool disable_auto_release, bool disable_coprocessor,
                                   const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                                   std::vector<pb::common::KeyValue>* kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.CreateScan(scan_id);

  status = scan->Open(*scan_id, engine_->GetRawEngine(), cf_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::Open failed : {}", *scan_id);
    manager.DeleteScan(*scan_id);
    *scan_id = "";
    return status;
  }

  status = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release,
                                  disable_coprocessor, coprocessor, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed: {}", *scan_id);
    manager.DeleteScan(*scan_id);
    *scan_id = "";
    kvs->clear();
    return status;
  }

  return status;
}

butil::Status Storage::KvScanContinue(std::shared_ptr<Context>, const std::string& scan_id, int64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs) {
  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (!scan) {
    DINGO_LOG(ERROR) << fmt::format("scan_id: {} not found", scan_id);
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, kvs);
  if (!status.ok()) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed scan : {} max_fetch_cnt : {}", scan_id,
                                    max_fetch_cnt);
    return status;
  }

  return status;
}

butil::Status Storage::KvScanRelease(std::shared_ptr<Context>, const std::string& scan_id) {
  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (!scan) {
    DINGO_LOG(ERROR) << fmt::format("scan_id: {} not found", scan_id);
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanRelease(scan, scan_id);
  if (!status.ok()) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanRelease failed : {}", scan_id);
    return status;
  }

  // if set auto release. directly delete
  manager.TryDeleteScan(scan_id);

  return status;
}

butil::Status Storage::VectorAdd(std::shared_ptr<Context> ctx, const std::vector<pb::common::VectorWithId>& vectors) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), vectors));
  }

  return engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), vectors),
                             [](std::shared_ptr<Context> ctx, butil::Status status) {
                               if (!status.ok()) {
                                 Helper::SetPbMessageError(status, ctx->Response());
                               }
                             });
}

butil::Status Storage::VectorDelete(std::shared_ptr<Context> ctx, const std::vector<int64_t>& ids) {
  if (ctx->IsSyncMode()) {
    return engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ids));
  }

  return engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ids),
                             [](std::shared_ptr<Context> ctx, butil::Status status) {
                               if (!status.ok()) {
                                 Helper::SetPbMessageError(status, ctx->Response());
                               }
                             });
}

butil::Status Storage::VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                        std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorBatchQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status::OK();
}

butil::Status Storage::VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                         std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorBatchSearch(ctx, results);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorGetBorderId(int64_t region_id, const pb::common::Range& region_range, bool get_min,
                                         int64_t& vector_id) {
  auto status = ValidateLeader(region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorGetBorderId(region_range, get_min, vector_id);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorScanQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                              VectorIndexWrapperPtr vector_index_wrapper,
                                              pb::common::VectorIndexMetrics& region_metrics) {
  auto status = ValidateLeader(region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorGetRegionMetrics(region_id, region_range, vector_index_wrapper, region_metrics);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorCount(int64_t region_id, const pb::common::Range& range, int64_t& count) {
  auto status = ValidateLeader(region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status = reader->VectorCount(range, count);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorCalcDistance(const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
                                          std::vector<std::vector<float>>& distances,
                                          std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,
                                          std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors) {
  // param check
  auto algorithm_type = request.algorithm_type();
  auto metric_type = request.metric_type();
  const auto& op_left_vectors = request.op_left_vectors();
  const auto& op_right_vectors = request.op_right_vectors();
  auto is_return_normlize = request.is_return_normlize();

  if (BAIDU_UNLIKELY(::dingodb::pb::index::AlgorithmType::ALGORITHM_NONE == algorithm_type)) {
    std::string s = fmt::format("invalid algorithm type : ALGORITHM_NONE");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (BAIDU_UNLIKELY(::dingodb::pb::common::MetricType::METRIC_TYPE_NONE == metric_type)) {
    std::string s = fmt::format("invalid metric type : METRIC_TYPE_NONE");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (BAIDU_UNLIKELY(op_left_vectors.empty() || op_right_vectors.empty())) {
    std::string s = fmt::format("op_left_vectors empty or op_right_vectors empty. ignore");
    DINGO_LOG(DEBUG) << s;
    return butil::Status();
  }

  int32_t dimension = 0;

  auto lambda_op_vector_check_function = [&dimension](const auto& op_vector, const std::string& name) {
    if (!op_vector.empty()) {
      size_t i = 0;
      for (const auto& vector : op_vector) {
        int32_t current_dimension = static_cast<int32_t>(vector.float_values().size());
        if (0 == dimension) {
          dimension = current_dimension;
        }

        if (dimension != current_dimension) {
          std::string s = fmt::format("{} index : {}  dimension : {} unequal current_dimension : {}", name, i,
                                      dimension, current_dimension);
          LOG(ERROR) << s;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
        }
        i++;
      }
    }

    return butil::Status();
  };

  butil::Status status;

  status = lambda_op_vector_check_function(op_left_vectors, "op_left_vectors");
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("lambda_op_vector_check_function : op_left_vectors failed");
    return status;
  }

  status = lambda_op_vector_check_function(op_right_vectors, "op_right_vectors");
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("lambda_op_vector_check_function : op_right_vectors failed");
    return status;
  }

  status = VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("VectorIndexUtils::CalcDistanceEntry failed : {}", status.error_cstr());
  }

  return status;
}

butil::Status Storage::VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                              std::vector<pb::index::VectorWithDistanceResult>& results,
                                              int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                              int64_t& search_time_us) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader();
  status =
      reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us, search_time_us);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

// txn

butil::Status Storage::TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys,
                                   pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnBatchGet keys size : " << keys.size() << ", start_ts: " << start_ts
                  << ", kvs size : " << kvs.size() << " txn_result_info : " << txn_result_info.ShortDebugString();

  auto reader = engine_->NewTxnReader();
  status = reader->TxnBatchGet(ctx, start_ts, keys, kvs, txn_result_info);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnScan(std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range,
                               int64_t limit, bool key_only, bool is_reverse, pb::store::TxnResultInfo& txn_result_info,
                               std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_key) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnScan region_id: " << ctx->RegionId() << " range: " << range.ShortDebugString()
                  << " limit: " << limit << " start_ts: " << start_ts << " key_only: " << key_only
                  << " is_reverse: " << is_reverse << " txn_result_info: " << txn_result_info.ShortDebugString()
                  << " kvs size: " << kvs.size() << " has_more: " << has_more << " end_key: " << end_key;

  auto reader = engine_->NewTxnReader();
  status = reader->TxnScan(ctx, start_ts, range, limit, key_only, is_reverse, txn_result_info, kvs, has_more, end_key);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnPrewrite(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                                   const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                   int64_t txn_size, bool try_one_pc, int64_t max_commit_ts) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnPrewrite mutations size : " << mutations.size() << " primary_lock : " << primary_lock
                  << " start_ts : " << start_ts << " lock_ttl : " << lock_ttl << " txn_size : " << txn_size
                  << " try_one_pc : " << try_one_pc << " max_commit_ts : " << max_commit_ts;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnPrewrite(ctx, mutations, primary_lock, start_ts, lock_ttl, txn_size, try_one_pc, max_commit_ts);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnCommit(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                 const std::vector<std::string>& keys) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnCommit start_ts : " << start_ts << " commit_ts : " << commit_ts
                  << " keys size : " << keys.size();

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnCommit(ctx, start_ts, commit_ts, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                         int64_t caller_start_ts, int64_t current_ts) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnCheckTxnStatus primary_key : " << primary_key << " lock_ts : " << lock_ts
                  << " caller_start_ts : " << caller_start_ts << " current_ts : " << current_ts;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnCheckTxnStatus(ctx, primary_key, lock_ts, caller_start_ts, current_ts);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                      const std::vector<std::string>& keys) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnResolveLock start_ts : " << start_ts << " commit_ts : " << commit_ts
                  << " keys size : " << keys.size();

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnResolveLock(ctx, start_ts, commit_ts, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                        const std::vector<std::string>& keys) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnBatchRollback keys size : " << keys.size() << ", start_ts: " << start_ts;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnBatchRollback(ctx, start_ts, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnScanLock(std::shared_ptr<Context> ctx, int64_t max_ts, const std::string& start_key,
                                   int64_t limit, const std::string& end_key, pb::store::TxnResultInfo& txn_result_info,
                                   std::vector<pb::store::LockInfo>& locks) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnScanLock max_ts : " << max_ts << " start_key : " << start_key << " limit : " << limit
                  << " end_key : " << end_key << " txn_result_info : " << txn_result_info.ShortDebugString()
                  << " locks size : " << locks.size();

  pb::common::Range range;
  range.set_start_key(start_key);
  range.set_end_key(end_key);
  std::vector<pb::store::LockInfo> lock_infos;

  auto reader = engine_->NewTxnReader();
  status = reader->TxnScanLock(ctx, 0, max_ts, range, limit, lock_infos);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                                    int64_t advise_lock_ttl) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnHeartBeat primary_lock : " << primary_lock << " start_ts : " << start_ts
                  << " advise_lock_ttl : " << advise_lock_ttl;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnHeartBeat(ctx, primary_lock, start_ts, advise_lock_ttl);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnGc safe_point_ts : " << safe_point_ts;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnGc(ctx, safe_point_ts);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                      const std::string& end_key) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnDeleteRange start_key : " << start_key << " end_key : " << end_key;

  auto writer = engine_->NewTxnWriter(engine_);
  status = writer->TxnDeleteRange(ctx, start_key, end_key);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnDump(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                               int64_t start_ts, int64_t end_ts, pb::store::TxnResultInfo& txn_result_info,
                               std::vector<pb::store::TxnWriteKey>& txn_write_keys,
                               std::vector<pb::store::TxnWriteValue>& txn_write_values,
                               std::vector<pb::store::TxnLockKey>& txn_lock_keys,
                               std::vector<pb::store::TxnLockValue>& txn_lock_values,
                               std::vector<pb::store::TxnDataKey>& txn_data_keys,
                               std::vector<pb::store::TxnDataValue>& txn_data_values) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << "TxnDump start_key : " << start_key << " end_key : " << end_key
                  << " txn_result_info : " << txn_result_info.ShortDebugString()
                  << " txn_write_keys size : " << txn_write_keys.size()
                  << " txn_write_values size : " << txn_write_values.size()
                  << " txn_lock_keys size : " << txn_lock_keys.size()
                  << " txn_lock_values size : " << txn_lock_values.size()
                  << " txn_data_keys size : " << txn_data_keys.size()
                  << " txn_data_values size : " << txn_data_values.size();
  auto snapshot = engine_->GetSnapshot();
  if (snapshot == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "get snapshot failed");
  }

  auto data_reader = engine_->NewReader();
  auto lock_reader = engine_->NewReader();
  auto write_reader = engine_->NewReader();

  // scan [start_key, end_key) for data
  std::vector<pb::common::KeyValue> data_kvs;
  Buf buf_start(8);
  buf_start.WriteLong(start_ts);
  Buf buf_end(8);
  buf_end.WriteLong(end_ts);

  ctx->SetCfName(Constant::kTxnDataCF);
  auto ret = data_reader->KvScan(ctx, start_key + buf_start.GetString(), end_key + buf_end.GetString(), data_kvs);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }
  for (const auto& kv : data_kvs) {
    if (kv.key().length() < 16) {
      DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "data_reader->KvScan failed");
    }
    Buf buf(kv.key().length());
    buf.Write(kv.key());
    buf.Skip(kv.key().length() - 8);

    pb::store::TxnDataKey txn_data_key;
    txn_data_key.set_key(kv.key().substr(kv.key().length() - 8));
    txn_data_key.set_start_ts(buf.ReadLong());

    txn_data_keys.push_back(txn_data_key);

    pb::store::TxnDataValue txn_data_value;
    txn_data_value.set_value(kv.value());
    txn_data_values.push_back(txn_data_value);
  }

  // scan [start_key, end_key) for lock
  std::vector<pb::common::KeyValue> lock_kvs;
  ctx->SetCfName(Constant::kTxnLockCF);
  ret = lock_reader->KvScan(ctx, start_key, end_key, lock_kvs);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("lock_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }

  for (const auto& kv : lock_kvs) {
    if (kv.key().length() < 16) {
      DINGO_LOG(ERROR) << fmt::format("lock_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "lock_reader->KvScan failed");
    }
    pb::store::TxnLockKey txn_lock_key;
    txn_lock_key.set_key(kv.key());
    txn_lock_keys.push_back(txn_lock_key);

    pb::store::TxnLockValue txn_lock_value;
    txn_lock_value.mutable_lock_info()->ParseFromString(kv.value());
    txn_lock_values.push_back(txn_lock_value);
  }

  // scan [start_key, end_key) for write
  std::vector<pb::common::KeyValue> write_kvs;
  ctx->SetCfName(Constant::kTxnWriteCF);
  ret = write_reader->KvScan(ctx, start_key + buf_start.GetString(), end_key + buf_end.GetString(), write_kvs);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }

  for (const auto& kv : write_kvs) {
    if (kv.key().length() < 16) {
      DINGO_LOG(ERROR) << fmt::format("write_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "write_reader->KvScan failed");
    }
    Buf buf(kv.key().length());
    buf.Write(kv.key());
    buf.Skip(kv.key().length() - 8);

    pb::store::TxnWriteKey txn_write_key;
    txn_write_key.set_key(kv.key().substr(kv.key().length() - 8));
    txn_write_key.set_commit_ts(buf.ReadLong());

    txn_write_keys.push_back(txn_write_key);

    pb::store::TxnWriteValue txn_write_value;
    txn_write_value.mutable_write_info()->ParseFromString(kv.value());

    txn_write_values.push_back(txn_write_value);
  }

  return butil::Status::OK();
}

}  // namespace dingodb
