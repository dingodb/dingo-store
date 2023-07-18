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

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/write_data.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
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

butil::Status Storage::VectorBatchQuery(std::shared_ptr<Context> ctx, std::vector<uint64_t> vector_ids,
                                        bool with_vector_data, bool with_scalar_data,
                                        std::vector<std::string> selected_scalar_keys,
                                        std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
  status = reader->VectorBatchQuery(ctx, vector_ids, with_vector_data, with_scalar_data, selected_scalar_keys,
                                    vector_with_ids);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status::OK();
}

butil::Status Storage::VectorGetBorderId(std::shared_ptr<Context> ctx, uint64_t& id, bool get_min) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
  status = reader->VectorGetBorderId(ctx, id, get_min);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorBatchSearch(std::shared_ptr<Context> ctx,
                                         const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                         const pb::common::VectorSearchParameter& parameter,
                                         std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto status = ValidateLeader(ctx->RegionId());
  if (!status.ok()) {
    return status;
  }

  auto reader = engine_->NewVectorReader(Constant::kStoreDataCF);
  status = reader->VectorBatchSearch(ctx, vector_with_ids, parameter, results);
  if (!status.ok()) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
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

}  // namespace dingodb
