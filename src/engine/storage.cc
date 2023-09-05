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
#include <vector>

#include "butil/compiler_specific.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/write_data.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
#include "vector/vector_index_utils.h"
namespace dingodb {

Storage::Storage(std::shared_ptr<Engine> engine) : engine_(engine) {}

Storage::~Storage() = default;

Snapshot* Storage::GetSnapshot() { return nullptr; }

void Storage::ReleaseSnapshot() {}

butil::Status Storage::ValidateLeader(uint64_t region_id) {
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

butil::Status Storage::KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                             std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }
  auto reader = engine_->NewReader(Constant::kStoreDataCF);
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

butil::Status Storage::KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, uint64_t region_id,
                                   const pb::common::Range& range, uint64_t max_fetch_cnt, bool key_only,
                                   bool disable_auto_release, bool disable_coprocessor,
                                   const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                                   std::vector<pb::common::KeyValue>* kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  ScanManager* manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager->CreateScan(scan_id);

  status = scan->Open(*scan_id, engine_->GetRawEngine(), cf_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::Open failed : {}", *scan_id);
    manager->DeleteScan(*scan_id);
    *scan_id = "";
    return status;
  }

  status = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release,
                                  disable_coprocessor, coprocessor, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed: {}", *scan_id);
    manager->DeleteScan(*scan_id);
    *scan_id = "";
    kvs->clear();
    return status;
  }

  return status;
}

butil::Status Storage::KvScanContinue(std::shared_ptr<Context>, const std::string& scan_id, uint64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs) {
  ScanManager* manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager->FindScan(scan_id);
  butil::Status status;
  if (!scan) {
    DINGO_LOG(ERROR) << fmt::format("scan_id : %s not found", scan_id.c_str());
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, kvs);
  if (!status.ok()) {
    manager->DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed scan : {} max_fetch_cnt : {}", scan_id,
                                    max_fetch_cnt);
    return status;
  }

  return status;
}

butil::Status Storage::KvScanRelease(std::shared_ptr<Context>, const std::string& scan_id) {
  ScanManager* manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager->FindScan(scan_id);
  butil::Status status;
  if (!scan) {
    DINGO_LOG(ERROR) << fmt::format("scan_id : %s not found", scan_id.c_str());
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanRelease(scan, scan_id);
  if (!status.ok()) {
    manager->DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanRelease failed : {}", scan_id);
    return status;
  }

  // if set auto release. directly delete
  manager->TryDeleteScan(scan_id);

  return status;
}

butil::Status Storage::VectorAdd(std::shared_ptr<Context> ctx, const std::vector<pb::common::VectorWithId>& vectors) {
  return engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), vectors),
                             [](std::shared_ptr<Context> ctx, butil::Status status) {
                               if (!status.ok()) {
                                 Helper::SetPbMessageError(status, ctx->Response());
                               }
                             });
}

butil::Status Storage::VectorDelete(std::shared_ptr<Context> ctx, const std::vector<uint64_t>& ids) {
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

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
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

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
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

butil::Status Storage::VectorGetBorderId(uint64_t region_id, const pb::common::Range& region_range, bool get_min,
                                         uint64_t& vector_id) {
  auto status = ValidateLeader(region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
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

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
  status = reader->VectorScanQuery(ctx, vector_with_ids);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorGetRegionMetrics(uint64_t region_id, const pb::common::Range& region_range,
                                              VectorIndexWrapperPtr vector_index_wrapper,
                                              pb::common::VectorIndexMetrics& region_metrics) {
  auto status = ValidateLeader(region_id);
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
  status = reader->VectorGetRegionMetrics(region_id, region_range, vector_index_wrapper, region_metrics);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorCalcDistance([[maybe_unused]] std::shared_ptr<Context> ctx,
                                          [[maybe_unused]] uint64_t region_id,
                                          const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
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

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
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

}  // namespace dingodb
