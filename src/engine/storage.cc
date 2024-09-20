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
#include <limits>
#include <string>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "engine/raft_store_engine.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "mvcc/ts_provider.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

Storage::Storage(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Engine> mono_engine,
                 mvcc::TsProviderPtr ts_provider)
    : raft_engine_(raft_engine), mono_engine_(mono_engine), ts_provider_(ts_provider) {}

RaftStoreEnginePtr Storage::GetRaftStoreEngine() {
  auto engine = std::dynamic_pointer_cast<RaftStoreEngine>(raft_engine_);
  CHECK(engine != nullptr) << "Cast RaftStoreEngine type failed.";

  return engine;
}

std::shared_ptr<Engine> Storage::GetStoreEngine(pb::common::StorageEngine store_engine_type) {
  if (BAIDU_LIKELY(store_engine_type == pb::common::StorageEngine::STORE_ENG_RAFT_STORE)) {
    return raft_engine_;
  } else if (store_engine_type == pb::common::StorageEngine::STORE_ENG_MONO_STORE) {
    return mono_engine_;
  }

  DINGO_LOG(FATAL) << "Not support store engine type.";

  return nullptr;
}

mvcc::ReaderPtr Storage::GetEngineMVCCReader(pb::common::StorageEngine store_engine_type,
                                             pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewMVCCReader(raw_engine_type);
}

Engine::ReaderPtr Storage::GetEngineReader(pb::common::StorageEngine store_engine_type,
                                           pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewReader(raw_engine_type);
}

Engine::TxnReaderPtr Storage::GetEngineTxnReader(pb::common::StorageEngine store_engine_type,
                                                 pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewTxnReader(raw_engine_type);
}

Engine::VectorReaderPtr Storage::GetEngineVectorReader(pb::common::StorageEngine store_engine_type,
                                                       pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewVectorReader(raw_engine_type);
}

Engine::DocumentReaderPtr Storage::GetEngineDocumentReader(pb::common::StorageEngine store_engine_type,
                                                           pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewDocumentReader(raw_engine_type);
}

Engine::WriterPtr Storage::GetEngineWriter(pb::common::StorageEngine store_engine_type,
                                           pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewWriter(raw_engine_type);
}

Engine::TxnWriterPtr Storage::GetEngineTxnWriter(pb::common::StorageEngine store_engine_type,
                                                 pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->NewTxnWriter(raw_engine_type);
}

RawEnginePtr Storage::GetRawEngine(pb::common::StorageEngine store_engine_type, pb::common::RawEngine raw_engine_type) {
  return GetStoreEngine(store_engine_type)->GetRawEngine(raw_engine_type);
}

Snapshot* Storage::GetSnapshot() { return nullptr; }

void Storage::ReleaseSnapshot() {}

butil::Status Storage::ValidateLeader(int64_t region_id) {
  auto region = Server::GetInstance().GetRegion(region_id);
  if (BAIDU_UNLIKELY(region == nullptr)) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (BAIDU_LIKELY(region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE)) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(raft_engine_);
    auto node = raft_kv_engine->GetNode(region_id);
    if (BAIDU_UNLIKELY(node == nullptr)) {
      return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found region");
    }

    if (!node->IsLeader()) {
      return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
    }
  }

  return butil::Status();
}

butil::Status Storage::ValidateLeader(store::RegionPtr region) {
  if (BAIDU_UNLIKELY(region == nullptr)) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  if (BAIDU_LIKELY(region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE)) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(raft_engine_);
    auto node = raft_kv_engine->GetNode(region->Id());
    if (BAIDU_UNLIKELY(node == nullptr)) {
      return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
    }

    if (!node->IsLeader()) {
      return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
    }
  }

  return butil::Status();
}

bool Storage::IsLeader(int64_t region_id) {
  auto region = Server::GetInstance().GetRegion(region_id);
  if (BAIDU_UNLIKELY(region == nullptr)) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] Not found region.", region_id);
    return false;
  }

  if (BAIDU_LIKELY(region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE)) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(raft_engine_);
    return raft_kv_engine->IsLeader(region_id);
  } else if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    return true;
  }

  return false;
}

bool Storage::IsLeader(store::RegionPtr region) {
  if (BAIDU_UNLIKELY(region == nullptr)) {
    return false;
  }

  if (BAIDU_LIKELY(region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE)) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(raft_engine_);
    return raft_kv_engine->IsLeader(region->Id());
  } else if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    return true;
  }

  return false;
}

butil::Status Storage::KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                             std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto reader = GetEngineMVCCReader(ctx->StoreEngineType(), ctx->RawEngineType());

  for (const auto& key : keys) {
    std::string value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), key, value);
    if (BAIDU_UNLIKELY(!status.ok())) {
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

butil::Status Storage::KvPut(std::shared_ptr<Context> ctx, std::vector<pb::common::KeyValue>& kvs) {
  auto writer = GetEngineWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  auto status = writer->KvPut(ctx, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                     bool is_atomic, std::vector<bool>& key_states) {
  auto writer = GetEngineWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  auto status = writer->KvPutIfAbsent(ctx, kvs, is_atomic, key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                std::vector<bool>& key_states) {
  auto writer = GetEngineWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  auto status = writer->KvDelete(ctx, keys, key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  auto writer = GetEngineWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  auto status = writer->KvDeleteRange(ctx, range);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                       const std::vector<std::string>& expect_values, bool is_atomic,
                                       std::vector<bool>& key_states) {
  auto writer = GetEngineWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  auto status = writer->KvCompareAndSet(ctx, kvs, expect_values, is_atomic, key_states);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                                   const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                                   bool disable_auto_release, bool disable_coprocessor,
                                   const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                                   std::vector<pb::common::KeyValue>* kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.CreateScan(scan_id);

  auto reader = GetEngineMVCCReader(ctx->StoreEngineType(), ctx->RawEngineType());

  status = scan->Open(*scan_id, reader, cf_name, ctx->Ts());
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::Open failed : {}", *scan_id);
    manager.DeleteScan(*scan_id);
    *scan_id = "";
    return status;
  }

  status = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release,
                                  disable_coprocessor, coprocessor, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed: {}", *scan_id);
    manager.DeleteScan(*scan_id);
    *scan_id = "";
    kvs->clear();
    return status;
  }

  return status;
}

butil::Status Storage::KvScanContinue(std::shared_ptr<Context>, const std::string& scan_id, int64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs, bool& has_more) {
  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (BAIDU_UNLIKELY(!scan)) {
    DINGO_LOG(ERROR) << fmt::format("scan_id: {} not found", scan_id);
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, kvs, has_more);
  if (BAIDU_UNLIKELY(!status.ok())) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanContinue failed scan : {} max_fetch_cnt : {}", scan_id,
                                    max_fetch_cnt);
    return status;
  }

  return status;
}

butil::Status Storage::KvScanRelease(std::shared_ptr<Context>, const std::string& scan_id) {
  ScanManager& manager = ScanManager::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (BAIDU_UNLIKELY(!scan)) {
    DINGO_LOG(ERROR) << fmt::format("scan_id: {} not found", scan_id);
    return butil::Status(pb::error::ESCAN_NOTFOUND, "Not found scan_id");
  }

  status = ScanHandler::ScanRelease(scan, scan_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanRelease failed : {}", scan_id);
    return status;
  }

  // if set auto release. directly delete
  manager.TryDeleteScan(scan_id);

  return status;
}

butil::Status Storage::KvScanBeginV2(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                                     const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                                     bool disable_auto_release, bool disable_coprocessor,
                                     const pb::common::CoprocessorV2& coprocessor, int64_t scan_id,
                                     std::vector<pb::common::KeyValue>* kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  ScanManagerV2& manager = ScanManagerV2::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.CreateScan(scan_id);
  if (!scan) {
    std::string s = fmt::format("ScanManagerV2::CreateScan failed, scan_id  {} repeated.", scan_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
  }

  auto reader = GetEngineMVCCReader(ctx->StoreEngineType(), ctx->RawEngineType());

  status = scan->Open(std::to_string(scan_id), reader, cf_name, ctx->Ts());
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::Open failed : {}", scan_id);
    manager.DeleteScan(scan_id);
    scan_id = std::numeric_limits<int64_t>::max();
    return status;
  }

  status = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release,
                                  disable_coprocessor, coprocessor, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanBegin failed: {}", scan_id);
    manager.DeleteScan(scan_id);
    scan_id = std::numeric_limits<int64_t>::max();
    kvs->clear();
    return status;
  }

  return status;
}

butil::Status Storage::KvScanContinueV2(std::shared_ptr<Context> /*ctx*/, int64_t scan_id, int64_t max_fetch_cnt,
                                        std::vector<pb::common::KeyValue>* kvs, bool& has_more) {
  ScanManagerV2& manager = ScanManagerV2::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (BAIDU_UNLIKELY(!scan)) {
    std::string s = fmt::format("scan_id: {} not found", scan_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::ESCAN_NOTFOUND, s);
  }

  status = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, kvs, has_more);
  if (BAIDU_UNLIKELY(!status.ok())) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanContinue failed scan : {} max_fetch_cnt : {}", scan_id,
                                    max_fetch_cnt);
    return status;
  }

  return status;
}

butil::Status Storage::KvScanReleaseV2(std::shared_ptr<Context> /*ctx*/, int64_t scan_id) {
  ScanManagerV2& manager = ScanManagerV2::GetInstance();
  std::shared_ptr<ScanContext> scan = manager.FindScan(scan_id);
  butil::Status status;
  if (BAIDU_UNLIKELY(!scan)) {
    std::string s = fmt::format("scan_id: {} not found", scan_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::ESCAN_NOTFOUND, s);
  }

  status = ScanHandler::ScanRelease(scan, std::to_string(scan_id));
  if (BAIDU_UNLIKELY(!status.ok())) {
    manager.DeleteScan(scan_id);
    DINGO_LOG(ERROR) << fmt::format("ScanContext::ScanRelease failed : {}", scan_id);
    return status;
  }

  // if set auto release. directly delete
  manager.TryDeleteScan(scan_id);

  return status;
}

butil::Status Storage::VectorAdd(std::shared_ptr<Context> ctx, bool is_sync,
                                 const std::vector<pb::common::VectorWithId>& vectors, bool is_update) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  auto engine = GetStoreEngine(ctx->StoreEngineType());

  if (is_sync) {
    auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ctx->Ttl(), vectors, is_update));
    auto* response = dynamic_cast<pb::index::VectorAddResponse*>(ctx->Response());
    CHECK(response != nullptr) << "VectorAddResponse is nullptr.";
    if (status.ok()) {
      response->mutable_key_states()->Resize(vectors.size(), true);
    } else {
      response->mutable_key_states()->Resize(vectors.size(), false);
    }

    response->set_ts(ts);

    return status;
  }

  return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ctx->Ttl(), vectors, is_update),
                            [ts, &vectors](std::shared_ptr<Context> ctx, butil::Status status) {
                              auto* response = dynamic_cast<pb::index::VectorAddResponse*>(ctx->Response());
                              CHECK(response != nullptr) << "VectorAddResponse is nullptr.";
                              if (!status.ok()) {
                                response->mutable_key_states()->Resize(vectors.size(), false);
                                Helper::SetPbMessageError(status, ctx->Response());
                              } else {
                                response->mutable_key_states()->Resize(vectors.size(), true);
                              }

                              response->set_ts(ts);
                            });
}

butil::Status Storage::VectorDelete(std::shared_ptr<Context> ctx, bool is_sync, store::RegionPtr region,
                                    const std::vector<int64_t>& ids) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  auto engine = GetStoreEngine(ctx->StoreEngineType());

  // get key state
  char prefix = region->GetKeyPrefix();
  auto reader = engine->NewMVCCReader(region->GetRawEngineType());

  std::vector<bool> key_states(ids.size(), false);
  for (int i = 0; i < ids.size(); ++i) {
    std::string plain_key = VectorCodec::PackageVectorKey(prefix, region->PartitionId(), ids[i]);

    std::string value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), plain_key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
  }

  if (is_sync) {
    auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ids));
    auto* response = dynamic_cast<pb::index::VectorDeleteResponse*>(ctx->Response());
    CHECK(response != nullptr) << "VectorDeleteResponse is nullptr.";
    if (status.ok()) {
      for (auto state : key_states) {
        response->add_key_states(state);
      }
    } else {
      response->mutable_key_states()->Resize(ids.size(), false);
    }

    response->set_ts(ts);

    return status;
  }

  return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ids),
                            [&ids, &key_states, &ts](std::shared_ptr<Context> ctx, butil::Status status) {
                              auto* response = dynamic_cast<pb::index::VectorDeleteResponse*>(ctx->Response());
                              CHECK(response != nullptr) << "VectorDeleteResponse is nullptr.";
                              if (!status.ok()) {
                                response->mutable_key_states()->Resize(ids.size(), false);
                                Helper::SetPbMessageError(status, ctx->Response());
                              } else {
                                for (auto state : key_states) {
                                  response->add_key_states(state);
                                }
                              }

                              response->set_ts(ts);
                            });
}

butil::Status Storage::VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                        std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorBatchQuery(ctx, vector_with_ids);
  if (BAIDU_UNLIKELY(!status.ok())) {
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
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorBatchSearch(ctx, results);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorGetBorderId(store::RegionPtr region, bool get_min, int64_t ts, int64_t& vector_id) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status = vector_reader->VectorGetBorderId(ts, region->Range(false), get_min, vector_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorScanQuery(ctx, vector_with_ids);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorGetRegionMetrics(store::RegionPtr region, VectorIndexWrapperPtr vector_index_wrapper,
                                              pb::common::VectorIndexMetrics& region_metrics) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status =
      vector_reader->VectorGetRegionMetrics(region->Id(), region->Range(false), vector_index_wrapper, region_metrics);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorCount(store::RegionPtr region, pb::common::Range range, int64_t ts, int64_t& count) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status = vector_reader->VectorCount(ts, range, count);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorCountMemory(std::shared_ptr<Engine::VectorReader::Context> ctx, int64_t& count) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorCountMemory(ctx, count);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorImport(std::shared_ptr<Context> ctx, bool is_sync,
                                    const std::vector<pb::common::VectorWithId>& vectors,
                                    const std::vector<int64_t>& delete_ids) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  auto engine = GetStoreEngine(ctx->StoreEngineType());

  if (!vectors.empty()) {
    if (is_sync) {
      auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ctx->Ttl(), vectors, true));
      auto* response = dynamic_cast<pb::index::VectorImportResponse*>(ctx->Response());
      CHECK(response != nullptr) << "VectorImportResponse is nullptr.";
      if (!status.ok()) {
        Helper::SetPbMessageError(status, ctx->Response());
      }
      response->set_ttl(ctx->Ttl());
      return status;
    }

    return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ctx->Ttl(), vectors, true),
                              [ts, &vectors](std::shared_ptr<Context> ctx, butil::Status status) {
                                auto* response = dynamic_cast<pb::index::VectorImportResponse*>(ctx->Response());
                                CHECK(response != nullptr) << "VectorImportResponse is nullptr.";
                                if (!status.ok()) {
                                  Helper::SetPbMessageError(status, ctx->Response());
                                }
                                response->set_ttl(ctx->Ttl());
                              });
  } else {
    if (is_sync) {
      auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, delete_ids));
      auto* response = dynamic_cast<pb::index::VectorImportResponse*>(ctx->Response());
      CHECK(response != nullptr) << "VectorImportResponse is nullptr.";
      if (!status.ok()) {
        Helper::SetPbMessageError(status, ctx->Response());
      }
      response->set_ttl(ctx->Ttl());

      return status;
    }

    return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, delete_ids),
                              [&ts](std::shared_ptr<Context> ctx, butil::Status status) {
                                auto* response = dynamic_cast<pb::index::VectorImportResponse*>(ctx->Response());
                                CHECK(response != nullptr) << "VectorImportResponse is nullptr.";
                                if (!status.ok()) {
                                  Helper::SetPbMessageError(status, ctx->Response());
                                }
                                response->set_ttl(ctx->Ttl());
                              });
  }
}

butil::Status Storage::VectorBuild(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                   const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                   pb::common::VectorStateParameter& vector_state_parameter) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorBuild(ctx, parameter, ts, vector_state_parameter);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorLoad(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  const pb::common::VectorLoadParameter& parameter,
                                  pb::common::VectorStateParameter& vector_state_parameter) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorLoad(ctx, parameter, vector_state_parameter);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorStatus(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                    pb::common::VectorStateParameter& vector_state_parameter,
                                    pb::error::Error& internal_error) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorStatus(ctx, vector_state_parameter, internal_error);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorReset(std::shared_ptr<Engine::VectorReader::Context> ctx, bool delete_data_file,
                                   pb::common::VectorStateParameter& vector_state_parameter) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorReset(ctx, delete_data_file, vector_state_parameter);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::VectorDump(std::shared_ptr<Engine::VectorReader::Context> ctx, bool dump_all,
                                  std::vector<std::string>& dump_datas) {
  auto status = ValidateLeader(ctx->region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorDump(ctx, dump_all, dump_datas);
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

  int64_t dimension = 0;

  auto lambda_op_vector_check_function = [&dimension](const auto& op_vector, const std::string& name) {
    if (!op_vector.empty()) {
      size_t i = 0;
      for (const auto& vector : op_vector) {
        int64_t current_dimension = static_cast<int64_t>(vector.float_values().size());
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
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("lambda_op_vector_check_function : op_left_vectors failed");
    return status;
  }

  status = lambda_op_vector_check_function(op_right_vectors, "op_right_vectors");
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("lambda_op_vector_check_function : op_right_vectors failed");
    return status;
  }

  status = VectorIndexUtils::CalcDistanceEntry(request, distances, result_op_left_vectors, result_op_right_vectors);
  if (BAIDU_UNLIKELY(!status.ok())) {
    DINGO_LOG(ERROR) << fmt::format("VectorIndexUtils::CalcDistanceEntry failed : {}", status.error_cstr());
  }

  return status;
}

butil::Status Storage::VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                              std::vector<pb::index::VectorWithDistanceResult>& results,
                                              int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                              int64_t& search_time_us) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto vector_reader = GetEngineVectorReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = vector_reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us,
                                                 search_time_us);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

// document

butil::Status Storage::DocumentAdd(std::shared_ptr<Context> ctx, bool is_sync,
                                   const std::vector<pb::common::DocumentWithId>& document_with_ids, bool is_update) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  auto engine = GetStoreEngine(ctx->StoreEngineType());

  if (is_sync) {
    auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, document_with_ids, is_update));
    auto* response = dynamic_cast<pb::document::DocumentAddResponse*>(ctx->Response());
    CHECK(response != nullptr) << "DocumentAddResponse is nullptr.";
    if (status.ok()) {
      response->mutable_key_states()->Resize(document_with_ids.size(), true);
    } else {
      response->mutable_key_states()->Resize(document_with_ids.size(), false);
    }

    response->set_ts(ts);

    return status;
  }

  return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, document_with_ids, is_update),
                            [ts, &document_with_ids](std::shared_ptr<Context> ctx, butil::Status status) {
                              auto* response = dynamic_cast<pb::document::DocumentAddResponse*>(ctx->Response());
                              CHECK(response != nullptr) << "DocumentAddResponse is nullptr.";
                              if (!status.ok()) {
                                response->mutable_key_states()->Resize(document_with_ids.size(), false);
                                Helper::SetPbMessageError(status, ctx->Response());
                              } else {
                                response->mutable_key_states()->Resize(document_with_ids.size(), true);
                              }

                              response->set_ts(ts);
                            });
}

butil::Status Storage::DocumentDelete(std::shared_ptr<Context> ctx, bool is_sync, store::RegionPtr region,
                                      const std::vector<int64_t>& ids) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  auto engine = GetStoreEngine(ctx->StoreEngineType());

  // get key state
  char prefix = region->GetKeyPrefix();
  auto reader = engine->NewMVCCReader(region->GetRawEngineType());

  std::vector<bool> key_states(ids.size(), false);
  for (int i = 0; i < ids.size(); ++i) {
    std::string plain_key = DocumentCodec::PackageDocumentKey(prefix, region->PartitionId(), ids[i]);

    std::string value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), plain_key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
  }

  if (is_sync) {
    auto status = engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ids, true));
    auto* response = dynamic_cast<pb::document::DocumentDeleteResponse*>(ctx->Response());
    CHECK(response != nullptr) << "DocumentDeleteResponse is nullptr.";
    if (status.ok()) {
      for (auto state : key_states) {
        response->add_key_states(state);
      }
    } else {
      response->mutable_key_states()->Resize(ids.size(), false);
    }

    response->set_ts(ts);

    return status;
  }

  return engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), ts, ids, true),
                            [&ids, &key_states, &ts](std::shared_ptr<Context> ctx, butil::Status status) {
                              auto* response = dynamic_cast<pb::document::DocumentDeleteResponse*>(ctx->Response());
                              CHECK(response != nullptr) << "DocumentDeleteResponse is nullptr.";
                              if (!status.ok()) {
                                response->mutable_key_states()->Resize(ids.size(), false);
                                Helper::SetPbMessageError(status, ctx->Response());
                              } else {
                                for (auto state : key_states) {
                                  response->add_key_states(state);
                                }
                              }

                              response->set_ts(ts);
                            });
}

butil::Status Storage::DocumentBatchQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                          std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = document_reader->DocumentBatchQuery(ctx, document_with_ids);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status::OK();
}

butil::Status Storage::DocumentSearch(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                      std::vector<pb::common::DocumentWithScore>& results) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = document_reader->DocumentSearch(ctx, results);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::DocumentGetBorderId(store::RegionPtr region, bool get_min, int64_t ts, int64_t& document_id) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status = document_reader->DocumentGetBorderId(ts, region->Range(false), get_min, document_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::DocumentScanQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                         std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto status = ValidateLeader(ctx->region_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(ctx->store_engine_type, ctx->raw_engine_type);

  status = document_reader->DocumentScanQuery(ctx, document_with_ids);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::DocumentGetRegionMetrics(store::RegionPtr region, DocumentIndexWrapperPtr document_index_wrapper,
                                                pb::common::DocumentIndexMetrics& region_metrics) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status = document_reader->DocumentGetRegionMetrics(region->Id(), region->Range(false), document_index_wrapper,
                                                     region_metrics);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::DocumentCount(store::RegionPtr region, pb::common::Range range, int64_t ts, int64_t& count) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  auto document_reader = GetEngineDocumentReader(region->GetStoreEngineType(), region->GetRawEngineType());

  status = document_reader->DocumentCount(ts, range, count);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

// txn

butil::Status Storage::TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys,
                                   const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info,
                                   std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnBatchGet keys size : " << keys.size() << ", start_ts: " << start_ts
                   << ", kvs size : " << kvs.size() << ", resolved_locks size: " << resolved_locks.size()
                   << " txn_result_info : " << txn_result_info.ShortDebugString();

  auto reader = GetEngineTxnReader(ctx->StoreEngineType(), ctx->RawEngineType());

  status = reader->TxnBatchGet(ctx, start_ts, keys, kvs, resolved_locks, txn_result_info);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnScan(std::shared_ptr<Context> ctx, const pb::stream::StreamRequestMeta& req_stream_meta,
                               int64_t start_ts, const pb::common::Range& range, int64_t limit, bool key_only,
                               bool is_reverse, const std::set<int64_t>& resolved_locks,
                               pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                               bool& has_more, std::string& end_scan_key, bool disable_coprocessor,
                               const pb::common::CoprocessorV2& coprocessor) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  // after validate leader
  auto stream_meta = req_stream_meta;
  if (stream_meta.limit() == 0) stream_meta.set_limit(limit);
  auto stream = Server::GetInstance().GetStreamManager()->GetOrNew(stream_meta);
  if (stream == nullptr) {
    return butil::Status(pb::error::ESTREAM_EXPIRED, fmt::format("stream({}) is expired.", stream_meta.stream_id()));
  }
  ctx->SetStream(stream);

  DINGO_LOG(DEBUG) << "TxnScan region_id: " << ctx->RegionId() << ", range: " << Helper::RangeToString(range)
                   << ", limit: " << limit << ", start_ts: " << start_ts << ", key_only: " << key_only
                   << ", is_reverse: " << is_reverse << ", resolved_locks size: " << resolved_locks.size()
                   << ", txn_result_info: " << txn_result_info.ShortDebugString() << ", kvs size: " << kvs.size()
                   << ", has_more: " << has_more << ", end_key: " << Helper::StringToHex(end_scan_key);

  auto reader = GetEngineTxnReader(ctx->StoreEngineType(), ctx->RawEngineType());

  status = reader->TxnScan(ctx, start_ts, range, limit, key_only, is_reverse, resolved_locks, disable_coprocessor,
                           coprocessor, txn_result_info, kvs, has_more, end_scan_key);
  if (BAIDU_UNLIKELY(!status.ok())) {
    if (pb::error::EKEY_NOT_FOUND == status.error_code()) {
      // return OK if not found
      return butil::Status::OK();
    }

    Server::GetInstance().GetStreamManager()->RemoveStream(stream);

    return status;
  }

  if (!has_more || stream_meta.close()) {
    Server::GetInstance().GetStreamManager()->RemoveStream(stream);
  }

  return butil::Status();
}

butil::Status Storage::TxnPessimisticLock(std::shared_ptr<Context> ctx,
                                          const std::vector<pb::store::Mutation>& mutations,
                                          const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                          int64_t for_update_ts, bool return_values,
                                          std::vector<pb::common::KeyValue>& kvs) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnPessimisticLock mutations size : " << mutations.size()
                   << " primary_lock : " << Helper::StringToHex(primary_lock) << " start_ts : " << start_ts
                   << " lock_ttl : " << lock_ttl << " for_update_ts : " << for_update_ts
                   << " return_values: " << return_values;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status =
      writer->TxnPessimisticLock(ctx, mutations, primary_lock, start_ts, lock_ttl, for_update_ts, return_values, kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status::OK();
}

butil::Status Storage::TxnPessimisticRollback(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                              int64_t for_update_ts, const std::vector<std::string>& keys) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnPessimisticRollback start_ts : " << start_ts << " for_update_ts : " << for_update_ts
                   << " keys size : " << keys.size();

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnPessimisticRollback(ctx, region, start_ts, for_update_ts, keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status::OK();
}

butil::Status Storage::TxnPrewrite(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                   const std::vector<pb::store::Mutation>& mutations, const std::string& primary_lock,
                                   int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc,
                                   int64_t min_commit_ts, int64_t max_commit_ts,
                                   const std::vector<int64_t>& pessimistic_checks,
                                   const std::map<int64_t, int64_t>& for_update_ts_checks,
                                   const std::map<int64_t, std::string>& lock_extra_datas) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnPrewrite mutations size : " << mutations.size()
                   << " primary_lock : " << Helper::StringToHex(primary_lock) << " start_ts : " << start_ts
                   << " lock_ttl : " << lock_ttl << " txn_size : " << txn_size << " try_one_pc : " << try_one_pc
                   << " max_commit_ts : " << max_commit_ts;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status =
      writer->TxnPrewrite(ctx, region, mutations, primary_lock, start_ts, lock_ttl, txn_size, try_one_pc, min_commit_ts,
                          max_commit_ts, pessimistic_checks, for_update_ts_checks, lock_extra_datas);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                 int64_t commit_ts, const std::vector<std::string>& keys) {
  auto status = ValidateLeader(region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnCommit start_ts : " << start_ts << " commit_ts : " << commit_ts
                   << " keys size : " << keys.size() << ", keys[0]: " << Helper::StringToHex(keys[0]);

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnCommit(ctx, region, start_ts, commit_ts, keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                         int64_t caller_start_ts, int64_t current_ts) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnCheckTxnStatus primary_key : " << Helper::StringToHex(primary_key) << " lock_ts : " << lock_ts
                   << " caller_start_ts : " << caller_start_ts << " current_ts : " << current_ts;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnCheckTxnStatus(ctx, primary_key, lock_ts, caller_start_ts, current_ts);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                      const std::vector<std::string>& keys) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnResolveLock start_ts : " << start_ts << " commit_ts : " << commit_ts
                   << " keys size : " << keys.size();

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnResolveLock(ctx, start_ts, commit_ts, keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                        const std::vector<std::string>& keys) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnBatchRollback keys size : " << keys.size() << ", start_ts: " << start_ts;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnBatchRollback(ctx, start_ts, keys);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnScanLock(std::shared_ptr<Context> ctx, const pb::stream::StreamRequestMeta& req_stream_meta,
                                   int64_t max_ts, const pb::common::Range& range, int64_t limit,
                                   pb::store::TxnResultInfo& txn_result_info,
                                   std::vector<pb::store::LockInfo>& lock_infos, bool& has_more,
                                   std::string& end_scan_key) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  // after validate leader
  auto stream_meta = req_stream_meta;
  if (stream_meta.limit() == 0) stream_meta.set_limit(limit);
  auto stream = Server::GetInstance().GetStreamManager()->GetOrNew(stream_meta);
  if (stream == nullptr) {
    return butil::Status(pb::error::ESTREAM_EXPIRED, fmt::format("stream({}) is expired.", stream_meta.stream_id()));
  }
  ctx->SetStream(stream);

  DINGO_LOG(DEBUG) << "TxnScanLock max_ts : " << max_ts << " range: " << Helper::RangeToString(range)
                   << " limit : " << limit << " txn_result_info : " << txn_result_info.ShortDebugString()
                   << " lock_infos size : " << lock_infos.size();

  auto reader = GetEngineTxnReader(ctx->StoreEngineType(), ctx->RawEngineType());

  status = reader->TxnScanLock(ctx, 0, max_ts, range, limit, lock_infos, has_more, end_scan_key);
  if (BAIDU_UNLIKELY(!status.ok())) {
    Server::GetInstance().GetStreamManager()->RemoveStream(stream);
    return status;
  }

  if (!has_more || stream_meta.close()) {
    Server::GetInstance().GetStreamManager()->RemoveStream(stream);
  }

  return butil::Status();
}

butil::Status Storage::TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                                    int64_t advise_lock_ttl) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnHeartBeat primary_lock : " << Helper::StringToHex(primary_lock) << " start_ts : " << start_ts
                   << " advise_lock_ttl : " << advise_lock_ttl;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnHeartBeat(ctx, primary_lock, start_ts, advise_lock_ttl);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnGc safe_point_ts : " << safe_point_ts;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnGc(ctx, safe_point_ts);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                      const std::string& end_key) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  DINGO_LOG(DEBUG) << "TxnDeleteRange start_key : " << start_key << " end_key : " << end_key;

  auto writer = GetEngineTxnWriter(ctx->StoreEngineType(), ctx->RawEngineType());

  status = writer->TxnDeleteRange(ctx, start_key, end_key);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  return butil::Status();
}

butil::Status Storage::TxnDump(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                               int64_t start_ts, int64_t end_ts, pb::store::TxnResultInfo& /*txn_result_info*/,
                               std::vector<pb::store::TxnWriteKey>& txn_write_keys,
                               std::vector<pb::store::TxnWriteValue>& txn_write_values,
                               std::vector<pb::store::TxnLockKey>& txn_lock_keys,
                               std::vector<pb::store::TxnLockValue>& txn_lock_values,
                               std::vector<pb::store::TxnDataKey>& txn_data_keys,
                               std::vector<pb::store::TxnDataValue>& txn_data_values) {
  auto status = ValidateLeader(ctx->RegionId());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  std::string encode_start_key = mvcc::Codec::EncodeKey(start_key, end_ts);
  std::string encode_end_key = mvcc::Codec::EncodeKey(end_key, start_ts);

  DINGO_LOG(INFO) << "TxnDump start_key : " << Helper::StringToHex(start_key)
                  << " end_key : " << Helper::StringToHex(end_key) << ", start_ts: " << start_ts
                  << ", end_ts: " << end_ts << ", TxnDump data start_key: " << Helper::StringToHex(encode_start_key)
                  << " end_key: " << Helper::StringToHex(encode_end_key);

  auto reader = GetEngineReader(ctx->StoreEngineType(), ctx->RawEngineType());

  // scan [start_key, end_key) for data
  std::vector<pb::common::KeyValue> data_kvs;

  ctx->SetCfName(Constant::kTxnDataCF);
  auto ret = reader->KvScan(ctx, encode_start_key, encode_end_key, data_kvs);
  if (BAIDU_UNLIKELY(!ret.ok())) {
    DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }
  for (const auto& kv : data_kvs) {
    // the min key len is : 1 byte region prefix + 8 byte start_ts + >=1 byte key
    if (BAIDU_UNLIKELY(kv.key().length() < 10)) {
      DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "data_reader->KvScan failed");
    }

    std::string user_key;
    int64_t ts = 0;
    mvcc::Codec::DecodeKey(kv.key(), user_key, ts);

    pb::store::TxnDataKey txn_data_key;
    txn_data_key.set_key(user_key);
    txn_data_key.set_start_ts(ts);

    txn_data_keys.push_back(txn_data_key);

    pb::store::TxnDataValue txn_data_value;
    txn_data_value.set_value(kv.value());
    txn_data_values.push_back(txn_data_value);

    DINGO_LOG(INFO) << fmt::format("TxnDump data key : {} value : {}", txn_data_key.ShortDebugString(),
                                   txn_data_value.ShortDebugString());
  }

  // scan [start_key, end_key) for lock
  std::vector<pb::common::KeyValue> lock_kvs;
  ctx->SetCfName(Constant::kTxnLockCF);
  ret = reader->KvScan(ctx, mvcc::Codec::EncodeKey(start_key, Constant::kLockVer),
                       mvcc::Codec::EncodeKey(end_key, Constant::kLockVer), lock_kvs);
  if (BAIDU_UNLIKELY(!ret.ok())) {
    DINGO_LOG(ERROR) << fmt::format("lock_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }

  for (const auto& kv : lock_kvs) {
    // the min key len is : 1 byte region prefix + 8 byte start_ts + >=1 byte key
    if (BAIDU_UNLIKELY(kv.key().length() < 10)) {
      DINGO_LOG(ERROR) << fmt::format("lock_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "lock_reader->KvScan failed");
    }
    std::string user_key;
    int64_t ts = 0;
    mvcc::Codec::DecodeKey(kv.key(), user_key, ts);

    pb::store::TxnLockKey txn_lock_key;
    txn_lock_key.set_key(user_key);
    txn_lock_keys.push_back(txn_lock_key);

    pb::store::TxnLockValue txn_lock_value;
    txn_lock_value.mutable_lock_info()->ParseFromString(kv.value());
    txn_lock_values.push_back(txn_lock_value);

    DINGO_LOG(INFO) << fmt::format("TxnDump lock key : {} value : {}", Helper::StringToHex(txn_lock_key.key()),
                                   txn_lock_value.ShortDebugString());
  }

  // scan [start_key, end_key) for write
  std::vector<pb::common::KeyValue> write_kvs;
  ctx->SetCfName(Constant::kTxnWriteCF);
  ret = reader->KvScan(ctx, encode_start_key, encode_end_key, write_kvs);
  if (BAIDU_UNLIKELY(!ret.ok())) {
    DINGO_LOG(ERROR) << fmt::format("data_reader->KvScan failed : {}", ret.error_cstr());
    return ret;
  }

  for (const auto& kv : write_kvs) {
    // the min key len is : 1 byte region prefix + 8 byte start_ts + >=1 byte key
    if (BAIDU_UNLIKELY(kv.key().length() < 10)) {
      DINGO_LOG(ERROR) << fmt::format("write_reader->KvScan read key faild: {}", kv.ShortDebugString());
      return butil::Status(pb::error::EINTERNAL, "write_reader->KvScan failed");
    }

    std::string user_key;
    int64_t ts = 0;
    mvcc::Codec::DecodeKey(kv.key(), user_key, ts);

    pb::store::TxnWriteKey txn_write_key;
    txn_write_key.set_key(user_key);
    txn_write_key.set_commit_ts(ts);

    txn_write_keys.push_back(txn_write_key);

    pb::store::TxnWriteValue txn_write_value;
    txn_write_value.mutable_write_info()->ParseFromString(kv.value());

    txn_write_values.push_back(txn_write_value);

    DINGO_LOG(INFO) << fmt::format("TxnDump write key : {} value : {}", txn_write_key.ShortDebugString(),
                                   txn_write_value.ShortDebugString());
  }

  return butil::Status::OK();
}

butil::Status Storage::PrepareMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                                    const pb::common::RegionDefinition& region_definition, int64_t min_applied_log_id) {
  if (BAIDU_LIKELY(ctx->StoreEngineType() == pb::common::StorageEngine::STORE_ENG_RAFT_STORE)) {
    return raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(job_id, region_definition, min_applied_log_id));
  } else if (ctx->StoreEngineType() == pb::common::StorageEngine::STORE_ENG_MONO_STORE) {
    return mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(job_id, region_definition, min_applied_log_id));
  } else {
    return butil::Status(pb::error::EENGINE_NOT_FOUND, "engine not found");
  }
}

butil::Status Storage::CommitMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                                   const pb::common::RegionDefinition& region_definition, int64_t prepare_merge_log_id,
                                   const std::vector<pb::raft::LogEntry>& entries) {
  if (BAIDU_LIKELY(ctx->StoreEngineType() == pb::common::StorageEngine::STORE_ENG_RAFT_STORE)) {
    return raft_engine_->AsyncWrite(
        ctx, WriteDataBuilder::BuildWrite(job_id, region_definition, prepare_merge_log_id, entries));
  } else if (ctx->StoreEngineType() == pb::common::StorageEngine::STORE_ENG_MONO_STORE) {
    return mono_engine_->AsyncWrite(
        ctx, WriteDataBuilder::BuildWrite(job_id, region_definition, prepare_merge_log_id, entries));
  } else {
    return butil::Status(pb::error::EENGINE_NOT_FOUND, "engine not found");
  }
}

}  // namespace dingodb
