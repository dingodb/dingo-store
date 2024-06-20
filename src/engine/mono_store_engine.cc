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
#include "engine/mono_store_engine.h"

#include <cstdint>

#include "common/role.h"
#include "document/document_reader.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "engine/txn_engine_helper.h"
#include "engine/write_data.h"
#include "event/store_state_machine_event.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "mvcc/reader.h"
#include "vector/vector_reader.h"

namespace dingodb {

MonoStoreEngine::MonoStoreEngine(RawEnginePtr rocks_raw_engine, RawEnginePtr bdb_raw_engine,
                                 EventListenerCollectionPtr listeners, mvcc::TsProviderPtr ts_provider)
    : rocks_raw_engine_(rocks_raw_engine),
      bdb_raw_engine_(bdb_raw_engine),
      listeners_(listeners),
      ts_provider_(ts_provider) {}

bool MonoStoreEngine::Init([[maybe_unused]] std::shared_ptr<Config> config) { return true; }
std::string MonoStoreEngine::GetName() {
  return pb::common::StorageEngine_Name(pb::common::StorageEngine::STORE_ENG_MONO_STORE);
}

MonoStoreEnginePtr MonoStoreEngine::GetSelfPtr() {
  return std::dynamic_pointer_cast<MonoStoreEngine>(shared_from_this());
}

// Invoke when server starting.
bool MonoStoreEngine::Recover() {
  auto store_region_meta = GET_STORE_REGION_META;
  auto regions = store_region_meta->GetAllRegion();

  int count = 0;
  for (auto& region : regions) {
    if ((region->State() == pb::common::StoreRegionState::NORMAL ||
         region->State() == pb::common::StoreRegionState::STANDBY ||
         region->State() == pb::common::StoreRegionState::SPLITTING ||
         region->State() == pb::common::StoreRegionState::MERGING ||
         region->State() == pb::common::StoreRegionState::TOMBSTONE) &&
        region->GetStoreEngineType() == pb::common::StorageEngine::STORE_ENG_MONO_STORE) {
      if (GetRole() == pb::common::INDEX) {
        auto vector_index_wrapper = region->VectorIndexWrapper();
        VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(vector_index_wrapper, false, false, 0,
                                                            "rocks engine recover");
        ++count;
      }
      if (GetRole() == pb::common::DOCUMENT) {
        auto document_index_wrapper = region->DocumentIndexWrapper();
        DocumentIndexManager::LaunchLoadAsyncBuildDocumentIndex(document_index_wrapper, false, false, 0,
                                                                "rocks engine recover");
        ++count;
      }
    }
  }

  DINGO_LOG(INFO) << fmt::format("[rocks.engine][region(*)] recover Raft node num({}).", count);

  return true;
}

pb::common::StorageEngine MonoStoreEngine::GetID() { return pb::common::StorageEngine::STORE_ENG_MONO_STORE; }

RawEnginePtr MonoStoreEngine::GetRawEngine(pb::common::RawEngine type) {
  if (type == pb::common::RawEngine::RAW_ENG_ROCKSDB) {
    return rocks_raw_engine_;
  } else if (type == pb::common::RawEngine::RAW_ENG_BDB) {
    return bdb_raw_engine_;
  }

  DINGO_LOG(FATAL) << "[rocks.engine] unknown raw engine type.";
  return nullptr;
}

bvar::LatencyRecorder g_rocks_write_latency("dingo_rocks_store_engine_write_latency");

int MonoStoreEngine::DispatchEvent(dingodb::EventType event_type, std::shared_ptr<dingodb::Event> event) {
  if (listeners_ == nullptr) return -1;

  for (auto& listener : listeners_->Get(event_type)) {
    int ret = listener->OnEvent(event);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

// todo
butil::Status MonoStoreEngine::Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  BvarLatencyGuard bvar_guard(&g_rocks_write_latency);
  auto store_region_meta = GET_STORE_REGION_META;

  auto region = store_region_meta->GetRegion(ctx->RegionId());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", ctx->RegionId()));
  }
  // CAUTION: sync mode cannot pass Done here
  if (ctx->Done()) {
    DINGO_LOG(FATAL) << fmt::format("[raft.engine][region({})] sync mode cannot pass Done here.", ctx->RegionId());
  }

  auto store_region_metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto region_metrics = store_region_metrics->GetMetrics(region->Id());
  if (region_metrics == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[rock.engine][region({})] metrics not found.", region->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region metrics {}", region->Id()));
  }
  DINGO_LOG(INFO) << fmt::format("[rock.engine][region({})] rocksengine write.", region->Id());
  RawEnginePtr raw_engine = GetRawEngine(region->GetRawEngineType());
  auto event = std::make_shared<SmApplyEvent>();
  auto raft_cmd = dingodb::GenRaftCmdRequest(ctx, write_data);
  event->region = region;
  event->engine = raw_engine;
  event->ctx = ctx;
  event->raft_cmd = raft_cmd;
  event->region_metrics = region_metrics;
  event->term_id = -1;
  event->log_id = -1;
  if (DispatchEvent(EventType::kSmApply, event) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[rock.engine][region({})] rocksengine write failed.", region->Id());
    return butil::Status(pb::error::EROCKS_ENGINE_UPDATE, "Update in place failed");
  }

  if (!ctx->Status().ok()) {
    return ctx->Status();
  }

  return butil::Status();
}

butil::Status MonoStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  return AsyncWrite(ctx, write_data, [](std::shared_ptr<Context> ctx, butil::Status status) {});
}
bvar::LatencyRecorder g_rocks_async_write_latency("dingo_rocks_store_engine_async_write_latency");

butil::Status MonoStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                                          WriteCbFunc write_cb) {
  BvarLatencyGuard bvar_guard(&g_rocks_async_write_latency);

  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(ctx->RegionId());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", ctx->RegionId()));
  }
  auto store_region_metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto region_metrics = store_region_metrics->GetMetrics(region->Id());
  if (region_metrics == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[rock.engine][region({})] metrics not found.", region->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region metrics {}", region->Id()));
  }
  DINGO_LOG(INFO) << fmt::format("[rock.engine][region({})] rocksengine async write.", region->Id());
  ctx->SetWriteCb(write_cb);
  RawEnginePtr raw_engine = GetRawEngine(region->GetRawEngineType());
  auto event = std::make_shared<SmApplyEvent>();
  auto raft_cmd = dingodb::GenRaftCmdRequest(ctx, write_data);
  event->region = region;
  event->engine = raw_engine;
  event->ctx = ctx;
  event->raft_cmd = raft_cmd;
  event->region_metrics = region_metrics;
  event->term_id = -1;
  event->log_id = -1;
  if (DispatchEvent(EventType::kSmApply, event) != 0) {
    return butil::Status(pb::error::EROCKS_ENGINE_UPDATE, "Update in place failed");
  }

  if (!ctx->Status().ok()) {
    return ctx->Status();
  }

  return butil::Status();
}

butil::Status MonoStoreEngine::Reader::KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) {
  return reader_->KvGet(ctx->CfName(), key, value);
}

butil::Status MonoStoreEngine::Reader::KvScan(std::shared_ptr<Context> ctx, const std::string& start_key,
                                              const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  return reader_->KvScan(ctx->CfName(), start_key, end_key, kvs);
}

butil::Status MonoStoreEngine::Reader::KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                               const std::string& end_key, int64_t& count) {
  return reader_->KvCount(ctx->CfName(), start_key, end_key, count);
}

// vector
butil::Status MonoStoreEngine::VectorReader::VectorBatchSearch(
    std::shared_ptr<VectorReader::Context> ctx, std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearch(ctx, results);
}

butil::Status MonoStoreEngine::VectorReader::VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                              std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchQuery(ctx, vector_with_ids);
}

butil::Status MonoStoreEngine::VectorReader::VectorGetBorderId(int64_t ts, const pb::common::Range& region_range,
                                                               bool get_min, int64_t& vector_id) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetBorderId(ts, region_range, get_min, vector_id);
}

butil::Status MonoStoreEngine::VectorReader::VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                             std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorScanQuery(ctx, vector_with_ids);
}

butil::Status MonoStoreEngine::VectorReader::VectorGetRegionMetrics(int64_t region_id,
                                                                    const pb::common::Range& region_range,
                                                                    VectorIndexWrapperPtr vector_index,
                                                                    pb::common::VectorIndexMetrics& region_metrics) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetRegionMetrics(region_id, region_range, vector_index, region_metrics);
}

butil::Status MonoStoreEngine::VectorReader::VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorCount(ts, range, count);
}

butil::Status MonoStoreEngine::VectorReader::VectorBatchSearchDebug(
    std::shared_ptr<VectorReader::Context> ctx,  // NOLINT
    std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us,
                                               search_time_us);
}

// document
butil::Status MonoStoreEngine::DocumentReader::DocumentSearch(std::shared_ptr<DocumentReader::Context> ctx,
                                                              std::vector<pb::common::DocumentWithScore>& results) {
  auto vector_reader = dingodb::DocumentReader::New(reader_);
  return vector_reader->DocumentSearch(ctx, results);
}

butil::Status MonoStoreEngine::DocumentReader::DocumentBatchQuery(
    std::shared_ptr<DocumentReader::Context> ctx, std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto vector_reader = dingodb::DocumentReader::New(reader_);
  return vector_reader->DocumentBatchQuery(ctx, document_with_ids);
}

butil::Status MonoStoreEngine::DocumentReader::DocumentGetBorderId(int64_t ts, const pb::common::Range& region_range,
                                                                   bool get_min, int64_t& document_id) {
  auto vector_reader = dingodb::DocumentReader::New(reader_);
  return vector_reader->DocumentGetBorderId(ts, region_range, get_min, document_id);
}

butil::Status MonoStoreEngine::DocumentReader::DocumentScanQuery(
    std::shared_ptr<DocumentReader::Context> ctx, std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto vector_reader = dingodb::DocumentReader::New(reader_);
  return vector_reader->DocumentScanQuery(ctx, document_with_ids);
}

butil::Status MonoStoreEngine::DocumentReader::DocumentGetRegionMetrics(
    int64_t region_id, const pb::common::Range& region_range, DocumentIndexWrapperPtr document_index,
    pb::common::DocumentIndexMetrics& region_metrics) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentGetRegionMetrics(region_id, region_range, document_index, region_metrics);
}

butil::Status MonoStoreEngine::DocumentReader::DocumentCount(int64_t ts, const pb::common::Range& range,
                                                             int64_t& count) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentCount(ts, range, count);
}

// normal

butil::Status MonoStoreEngine::Writer::KvPut(std::shared_ptr<Context> ctx,
                                             const std::vector<pb::common::KeyValue>& kvs) {
  int64_t ts = ts_provider_->GetTs();
  auto encode_kvs = mvcc::Codec::EncodeKeyValuesWithPut(ts, kvs);
  return mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), encode_kvs, ts));
}

butil::Status MonoStoreEngine::Writer::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                                std::vector<bool>& key_states) {
  int64_t ts = ts_provider_->GetTs();
  auto reader = mono_engine_->NewMVCCReader(ctx->RawEngineType());

  key_states.resize(keys.size(), false);
  for (int i = 0; i < keys.size(); ++i) {
    const auto& key = keys[i];
    std::string value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
  }

  auto encode_keys = mvcc::Codec::EncodeKeys(ts, keys);

  return mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), encode_keys, ts));
}

butil::Status MonoStoreEngine::Writer::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range,
                                                     int64_t& count) {
  auto encode_range = mvcc::Codec::EncodeRange(range);
  auto reader = mono_engine_->NewMVCCReader(ctx->RawEngineType());

  reader->KvCount(ctx->CfName(), ctx->Ts(), encode_range.start_key(), encode_range.end_key(), count);
  return mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), range));
}

butil::Status MonoStoreEngine::Writer::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                                     const std::vector<pb::common::KeyValue>& kvs, bool is_atomic,
                                                     std::vector<bool>& key_states) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  key_states.resize(kvs.size(), false);

  std::vector<bool> temp_key_states;
  temp_key_states.resize(kvs.size(), false);

  int64_t ts = ts_provider_->GetTs();
  auto reader = mono_engine_->NewMVCCReader(ctx->RawEngineType());
  std::vector<pb::common::KeyValue> put_kvs;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto encode_key = mvcc::Codec::EncodeKey(kv.key(), ts);
    auto encode_key_without_ts = mvcc::Codec::TruncateTsForKey(encode_key);

    std::string old_value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), kv.key(), old_value);
    if (!status.ok() && status.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
      return butil::Status(pb::error::EINTERNAL, "Internal get error");
    }

    if (is_atomic) {
      if (status.ok()) {
        return butil::Status();
      }
    } else {
      if (status.ok()) {
        continue;
      }
    }

    pb::common::KeyValue encode_kv;
    encode_kv.mutable_key()->swap(encode_key);
    mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, kv.value(), *encode_kv.mutable_value());

    put_kvs.push_back(std::move(encode_kv));

    temp_key_states[i] = true;
  }

  if (put_kvs.empty()) {
    return butil::Status::OK();
  }

  auto status = mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), put_kvs, ts));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  return butil::Status();
}

butil::Status MonoStoreEngine::Writer::KvCompareAndSet(std::shared_ptr<Context> ctx,
                                                       const std::vector<pb::common::KeyValue>& kvs,
                                                       const std::vector<std::string>& expect_values, bool is_atomic,
                                                       std::vector<bool>& key_states) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }
  if (BAIDU_UNLIKELY(kvs.size() != expect_values.size())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is mismatch");
  }

  key_states.resize(kvs.size(), false);

  std::vector<bool> temp_key_states;
  temp_key_states.resize(kvs.size(), false);

  int64_t ts = ts_provider_->GetTs();
  auto reader = mono_engine_->NewMVCCReader(ctx->RawEngineType());
  std::vector<pb::common::KeyValue> put_kvs;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto encode_key = mvcc::Codec::EncodeKey(kv.key(), ts);
    auto encode_key_without_ts = mvcc::Codec::TruncateTsForKey(encode_key);

    std::string old_value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), kv.key(), old_value);
    if (!status.ok() && status.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
      return butil::Status(pb::error::EINTERNAL, "Internal get error");
    }

    if (is_atomic) {
      if (status.ok()) {
        if (old_value != expect_values[i]) {
          return butil::Status();
        }
      } else if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        if (!expect_values[i].empty()) {
          return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
        }
      }
    } else {
      if (status.ok()) {
        if (old_value != expect_values[i]) {
          continue;
        }
      } else if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        if (!expect_values[i].empty()) {
          continue;
        }
      }
    }

    pb::common::KeyValue encode_kv;
    encode_kv.mutable_key()->swap(encode_key);

    // value empty means delete
    if (kv.value().empty()) {
      encode_kv.set_value(mvcc::Codec::ValueFlagDelete());
    } else {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, kv.value(), *encode_kv.mutable_value());
    }

    put_kvs.push_back(std::move(encode_kv));

    temp_key_states[i] = true;
  }

  if (put_kvs.empty()) {
    return butil::Status::OK();
  }

  auto status = mono_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), put_kvs, ts));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  return butil::Status();
}

butil::Status MonoStoreEngine::TxnReader::TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                      const std::vector<std::string>& keys,
                                                      std::vector<pb::common::KeyValue>& kvs,
                                                      const std::set<int64_t>& resolved_locks,
                                                      pb::store::TxnResultInfo& txn_result_info) {
  return TxnEngineHelper::BatchGet(txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, keys, resolved_locks,
                                   txn_result_info, kvs);
}

butil::Status MonoStoreEngine::TxnReader::TxnScan(
    std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range, int64_t limit, bool key_only,
    bool is_reverse, const std::set<int64_t>& resolved_locks, bool disable_coprocessor,
    const pb::common::CoprocessorV2& coprocessor, pb::store::TxnResultInfo& txn_result_info,
    std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::Scan(txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, range, limit, key_only,
                               is_reverse, resolved_locks, disable_coprocessor, coprocessor, txn_result_info, kvs,
                               has_more, end_scan_key);
}

butil::Status MonoStoreEngine::TxnReader::TxnScanLock(std::shared_ptr<Context> /*ctx*/, int64_t min_lock_ts,
                                                      int64_t max_lock_ts, const pb::common::Range& range,
                                                      int64_t limit, std::vector<pb::store::LockInfo>& lock_infos,
                                                      bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::ScanLockInfo(txn_reader_raw_engine_, min_lock_ts, max_lock_ts, range, limit, lock_infos,
                                       has_more, end_scan_key);
}

butil::Status MonoStoreEngine::TxnWriter::TxnPessimisticLock(std::shared_ptr<Context> ctx,
                                                             const std::vector<pb::store::Mutation>& mutations,
                                                             const std::string& primary_lock, int64_t start_ts,
                                                             int64_t lock_ttl, int64_t for_update_ts) {
  return TxnEngineHelper::PessimisticLock(txn_writer_raw_engine_, mono_engine_, ctx, mutations, primary_lock, start_ts,
                                          lock_ttl, for_update_ts);
}

butil::Status MonoStoreEngine::TxnWriter::TxnPessimisticRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                                 int64_t for_update_ts,
                                                                 const std::vector<std::string>& keys) {
  return TxnEngineHelper::PessimisticRollback(txn_writer_raw_engine_, mono_engine_, ctx, start_ts, for_update_ts, keys);
}

butil::Status MonoStoreEngine::TxnWriter::TxnPrewrite(
    std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations, const std::string& primary_lock,
    int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc, int64_t max_commit_ts,
    const std::vector<int64_t>& pessimistic_checks, const std::map<int64_t, int64_t>& for_update_ts_checks,
    const std::map<int64_t, std::string>& lock_extra_datas) {
  return TxnEngineHelper::Prewrite(txn_writer_raw_engine_, mono_engine_, ctx, mutations, primary_lock, start_ts,
                                   lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                   for_update_ts_checks, lock_extra_datas);
}

butil::Status MonoStoreEngine::TxnWriter::TxnCommit(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                                    const std::vector<std::string>& keys) {
  return TxnEngineHelper::Commit(txn_writer_raw_engine_, mono_engine_, ctx, start_ts, commit_ts, keys);
}

butil::Status MonoStoreEngine::TxnWriter::TxnCheckTxnStatus(std::shared_ptr<Context> ctx,
                                                            const std::string& primary_key, int64_t lock_ts,
                                                            int64_t caller_start_ts, int64_t current_ts) {
  return TxnEngineHelper::CheckTxnStatus(txn_writer_raw_engine_, mono_engine_, ctx, primary_key, lock_ts,
                                         caller_start_ts, current_ts);
}

butil::Status MonoStoreEngine::TxnWriter::TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                         int64_t commit_ts, const std::vector<std::string>& keys) {
  return TxnEngineHelper::ResolveLock(txn_writer_raw_engine_, mono_engine_, ctx, start_ts, commit_ts, keys);
}

butil::Status MonoStoreEngine::TxnWriter::TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                           const std::vector<std::string>& keys) {
  return TxnEngineHelper::BatchRollback(txn_writer_raw_engine_, mono_engine_, ctx, start_ts, keys);
}

butil::Status MonoStoreEngine::TxnWriter::TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock,
                                                       int64_t start_ts, int64_t advise_lock_ttl) {
  return TxnEngineHelper::HeartBeat(txn_writer_raw_engine_, mono_engine_, ctx, primary_lock, start_ts, advise_lock_ttl);
}

butil::Status MonoStoreEngine::TxnWriter::TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                                         const std::string& end_key) {
  return TxnEngineHelper::DeleteRange(txn_writer_raw_engine_, mono_engine_, ctx, start_key, end_key);
}

butil::Status MonoStoreEngine::TxnWriter::TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  return TxnEngineHelper::Gc(txn_writer_raw_engine_, mono_engine_, ctx, safe_point_ts);
}

mvcc::ReaderPtr MonoStoreEngine::NewMVCCReader(pb::common::RawEngine type) {
  return std::make_shared<mvcc::KvReader>(GetRawEngine(type)->Reader());
}

Engine::ReaderPtr MonoStoreEngine::NewReader(pb::common::RawEngine type) {
  return std::make_shared<MonoStoreEngine::Reader>(GetRawEngine(type)->Reader());
}

Engine::WriterPtr MonoStoreEngine::NewWriter(pb::common::RawEngine) {
  return std::make_shared<MonoStoreEngine::Writer>(GetSelfPtr(), ts_provider_);
}

Engine::VectorReaderPtr MonoStoreEngine::NewVectorReader(pb::common::RawEngine type) {
  return std::make_shared<MonoStoreEngine::VectorReader>(mvcc::VectorReader::New(GetRawEngine(type)->Reader()));
}

Engine::DocumentReaderPtr MonoStoreEngine::NewDocumentReader(pb::common::RawEngine type) {
  return std::make_shared<MonoStoreEngine::DocumentReader>(mvcc::DocumentReader::New(GetRawEngine(type)->Reader()));
}

Engine::TxnReaderPtr MonoStoreEngine::NewTxnReader(pb::common::RawEngine type) {
  return std::make_shared<MonoStoreEngine::TxnReader>(GetRawEngine(type));
}

Engine::TxnWriterPtr MonoStoreEngine::NewTxnWriter(pb::common::RawEngine type) {
  return std::make_shared<MonoStoreEngine::TxnWriter>(GetRawEngine(type), GetSelfPtr());
}

}  // namespace dingodb
