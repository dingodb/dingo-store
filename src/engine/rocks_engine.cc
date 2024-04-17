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
#include "engine/rocks_engine.h"

#include "common/role.h"
#include "engine/engine.h"
#include "engine/txn_engine_helper.h"
#include "engine/write_data.h"
#include "engine/raft_store_engine.h"
#include "event/store_state_machine_event.h"
#include "handler/raft_vote_handler.h"
#include "meta/store_meta_manager.h"
#include "vector/vector_reader.h"

namespace dingodb {

RocksEngine::RocksEngine(std::shared_ptr<RawEngine> rocks_engine, std::shared_ptr<RawEngine> bdb_engine, std::shared_ptr<EventListenerCollection> listeners)
    : raw_rocks_engine(rocks_engine),
      raw_bdb_engine(bdb_engine),
      listeners_(listeners) {}

bool RocksEngine::Init([[maybe_unused]] std::shared_ptr<Config> config) { return true; }
std::string RocksEngine::GetName() {
  return pb::common::StorageEngine_Name(pb::common::StorageEngine::STORE_ENG_MONO_STORE);
}

std::shared_ptr<RocksEngine> RocksEngine::GetSelfPtr() {
  return std::dynamic_pointer_cast<RocksEngine>(shared_from_this());
}

// Invoke when server starting.
bool RocksEngine::Recover() {
  auto store_region_meta = GET_STORE_REGION_META;
  auto regions = store_region_meta->GetAllRegion();

  int count = 0;
  for (auto& region : regions) {
    if ((region->State() == pb::common::StoreRegionState::NORMAL ||
        region->State() == pb::common::StoreRegionState::STANDBY ||
        region->State() == pb::common::StoreRegionState::SPLITTING ||
        region->State() == pb::common::StoreRegionState::MERGING ||
        region->State() == pb::common::StoreRegionState::TOMBSTONE) &&
        region->GetStoreEngineType() == pb::common::StorageEngine::STORE_ENG_MONO_STORE){
      if (GetRole() == pb::common::INDEX) {
        auto vector_index_wrapper = region->VectorIndexWrapper();
        VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(vector_index_wrapper, false, false, 0, "rocks engine recover");
        ++count;
      }
    }
  }

  DINGO_LOG(INFO) << fmt::format("[rocks.engine][region(*)] recover Raft node num({}).", count);

  return true;
}

pb::common::StorageEngine RocksEngine::GetID() { return pb::common::StorageEngine::STORE_ENG_MONO_STORE; }

std::shared_ptr<RawEngine> RocksEngine::GetRawEngine(pb::common::RawEngine type) {
  if (type == pb::common::RawEngine::RAW_ENG_ROCKSDB) {
    return raw_rocks_engine;
  } else if (type == pb::common::RawEngine::RAW_ENG_BDB) {
    return raw_bdb_engine;
  }

  DINGO_LOG(FATAL) << "[rocks.engine] unknown raw engine type.";
  return nullptr;
}

bvar::LatencyRecorder g_rocks_write_latency("dingo_rocks_store_engine_write_latency");

int RocksEngine::DispatchEvent(dingodb::EventType event_type, std::shared_ptr<dingodb::Event> event) {
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
butil::Status RocksEngine::Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
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
  std::shared_ptr<RawEngine> raw_engine = GetRawEngine(region->GetRawEngineType());
  auto event = std::make_shared<SmApplyEvent>();
  auto raft_cmd = dingodb::GenRaftCmdRequest(ctx, write_data);
  event->region = region;
  event->engine = raw_engine;
  event->ctx = ctx;
  event->raft_cmd = raft_cmd;
  event->region_metrics = region_metrics;
  event->term_id = -1;
  event->log_id = -1;
  if(DispatchEvent(EventType::kSmApply, event) != 0){
    DINGO_LOG(ERROR) << fmt::format("[rock.engine][region({})] rocksengine write failed.", region->Id());
    return butil::Status(pb::error::EROCKS_ENGINE_UPDATE, "Update in place failed");
  }

  if (!ctx->Status().ok()) {
    return ctx->Status();
  }
  
  return butil::Status();
}

butil::Status RocksEngine::AsyncWrite(std::shared_ptr<Context>  ctx, std::shared_ptr<WriteData> write_data) {
  return AsyncWrite(ctx, write_data, [](std::shared_ptr<Context> ctx, butil::Status status) {});
}
bvar::LatencyRecorder g_rocks_async_write_latency("dingo_rocks_store_engine_async_write_latency");

butil::Status RocksEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
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
  std::shared_ptr<RawEngine> raw_engine = GetRawEngine(region->GetRawEngineType());
  auto event = std::make_shared<SmApplyEvent>();
  auto raft_cmd = dingodb::GenRaftCmdRequest(ctx, write_data);
  event->region = region;
  event->engine = raw_engine;
  event->ctx = ctx;
  event->raft_cmd = raft_cmd;
  event->region_metrics = region_metrics;
  event->term_id = -1;
  event->log_id = -1;
  if(DispatchEvent(EventType::kSmApply, event) != 0){ 
    return butil::Status(pb::error::EROCKS_ENGINE_UPDATE, "Update in place failed");
  }

  if (!ctx->Status().ok()) {
    return ctx->Status();
  }

  return butil::Status();
}

butil::Status RocksEngine::Reader::KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) {
  return reader_->KvGet(ctx->CfName(), key, value);
}

butil::Status RocksEngine::Reader::KvScan(std::shared_ptr<Context> ctx, const std::string& start_key,
                                              const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  return reader_->KvScan(ctx->CfName(), start_key, end_key, kvs);
}

butil::Status RocksEngine::Reader::KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                               const std::string& end_key, int64_t& count) {
  return reader_->KvCount(ctx->CfName(), start_key, end_key, count);
}

std::shared_ptr<Engine::Reader> RocksEngine::NewReader(pb::common::RawEngine type) { 
  return std::make_shared<RocksEngine::Reader>(GetRawEngine(type)->Reader());

}

butil::Status RocksEngine::VectorReader::VectorBatchSearch(
    std::shared_ptr<VectorReader::Context> ctx, std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearch(ctx, results);
}

butil::Status RocksEngine::VectorReader::VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                              std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchQuery(ctx, vector_with_ids);
}

butil::Status RocksEngine::VectorReader::VectorGetBorderId(const pb::common::Range& region_range, bool get_min,
                                                               int64_t& vector_id) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetBorderId(region_range, get_min, vector_id);
}

butil::Status RocksEngine::VectorReader::VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                             std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorScanQuery(ctx, vector_with_ids);
}

butil::Status RocksEngine::VectorReader::VectorGetRegionMetrics(int64_t region_id,
                                                                    const pb::common::Range& region_range,
                                                                    VectorIndexWrapperPtr vector_index,
                                                                    pb::common::VectorIndexMetrics& region_metrics) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetRegionMetrics(region_id, region_range, vector_index, region_metrics);
}

butil::Status RocksEngine::VectorReader::VectorCount(const pb::common::Range& range, int64_t& count) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorCount(range, count);
}

butil::Status RocksEngine::VectorReader::VectorBatchSearchDebug(
    std::shared_ptr<VectorReader::Context> ctx,  // NOLINT
    std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us,
                                               search_time_us);
}

std::shared_ptr<Engine::Writer> RocksEngine::NewWriter(pb::common::RawEngine type) {
   return std::make_shared<RocksEngine::Writer>(GetRawEngine(type), GetSelfPtr());
}


butil::Status RocksEngine::Writer::KvPut(std::shared_ptr<Context> ctx,
                                             const std::vector<pb::common::KeyValue>& kvs) {
  return rocks_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), kvs));
}

butil::Status RocksEngine::Writer::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys) {
  return rocks_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), keys));
}

butil::Status RocksEngine::Writer::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  return rocks_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), range));
}

butil::Status RocksEngine::Writer::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                                     const std::vector<pb::common::KeyValue>& kvs, bool is_atomic,
                                                     std::vector<bool>& key_states) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  key_states.resize(kvs.size(), false);

  std::vector<bool> temp_key_states;
  temp_key_states.resize(kvs.size(), false);

  std::vector<pb::common::KeyValue> put_kvs;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string old_value;
    auto status = writer_raw_engine_->Reader()->KvGet(ctx->CfName(), kv.key(), old_value);
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

    put_kvs.push_back(kv);
    temp_key_states[i] = true;
  }

  if (put_kvs.empty()) {
    return butil::Status::OK();
  }

  auto status = rocks_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), put_kvs));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  return butil::Status();
}

butil::Status RocksEngine::Writer::KvCompareAndSet(std::shared_ptr<Context> ctx,
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

  std::vector<pb::common::KeyValue> put_kvs;
  std::vector<std::string> delete_keys;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string old_value;
    auto status = writer_raw_engine_->Reader()->KvGet(ctx->CfName(), kv.key(), old_value);
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

    // value empty means delete
    if (kv.value().empty()) {
      delete_keys.push_back(kv.key());
    } else {
      put_kvs.push_back(kv);
    }

    temp_key_states[i] = true;
  }

  if (put_kvs.empty() && delete_keys.empty()) {
    return butil::Status();
  }

  pb::raft::TxnRaftRequest txn_raft_request;
  auto* cf_put_delete = txn_raft_request.mutable_multi_cf_put_and_delete();

  // after all is processed, write into raft engine
  if (!put_kvs.empty()) {
    auto* default_puts = cf_put_delete->add_puts_with_cf();
    default_puts->set_cf_name(ctx->CfName());
    for (auto& kv : put_kvs) {
      *default_puts->add_kvs() = kv;
    }
  }

  if (!delete_keys.empty()) {
    auto* default_dels = cf_put_delete->add_deletes_with_cf();
    default_dels->set_cf_name(ctx->CfName());
    for (auto& key : delete_keys) {
      default_dels->add_keys(key);
    }
  }

  auto status = rocks_engine_->Write(ctx, WriteDataBuilder::BuildWrite(txn_raft_request));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  return butil::Status();
}

std::shared_ptr<Engine::VectorReader> RocksEngine::NewVectorReader(pb::common::RawEngine type) { 
  return std::make_shared<RocksEngine::VectorReader>(GetRawEngine(type)->Reader());
}

std::shared_ptr<Engine::TxnReader> RocksEngine::NewTxnReader(pb::common::RawEngine type) { 
  return std::make_shared<RocksEngine::TxnReader>(GetRawEngine(type));
}

butil::Status RocksEngine::TxnReader::TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                      const std::vector<std::string>& keys,
                                                      std::vector<pb::common::KeyValue>& kvs,
                                                      const std::set<int64_t>& resolved_locks,
                                                      pb::store::TxnResultInfo& txn_result_info) {
  return TxnEngineHelper::BatchGet(txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, keys, resolved_locks,
                                   txn_result_info, kvs);
}


butil::Status RocksEngine::TxnReader::TxnScan(
    std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range, int64_t limit, bool key_only,
    bool is_reverse, const std::set<int64_t>& resolved_locks, bool disable_coprocessor,
    const pb::common::CoprocessorV2& coprocessor, pb::store::TxnResultInfo& txn_result_info,
    std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::Scan(txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, range, limit, key_only,
                               is_reverse, resolved_locks, disable_coprocessor, coprocessor, txn_result_info, kvs,
                               has_more, end_scan_key);
}

butil::Status RocksEngine::TxnReader::TxnScanLock(std::shared_ptr<Context> /*ctx*/, int64_t min_lock_ts,
                                                      int64_t max_lock_ts, const pb::common::Range& range,
                                                      int64_t limit, std::vector<pb::store::LockInfo>& lock_infos,
                                                      bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::ScanLockInfo(txn_reader_raw_engine_, min_lock_ts, max_lock_ts, range, limit, lock_infos,
                                       has_more, end_scan_key);
}


std::shared_ptr<Engine::TxnWriter> RocksEngine::NewTxnWriter(pb::common::RawEngine type) { 
  return std::make_shared<RocksEngine::TxnWriter>(GetRawEngine(type), GetSelfPtr());
}

butil::Status RocksEngine::TxnWriter::TxnPessimisticLock(std::shared_ptr<Context> ctx,
                                                             const std::vector<pb::store::Mutation>& mutations,
                                                             const std::string& primary_lock, int64_t start_ts,
                                                             int64_t lock_ttl, int64_t for_update_ts) {
  return TxnEngineHelper::PessimisticLock(txn_writer_raw_engine_, rocks_engine_, ctx, mutations, primary_lock, start_ts,
                                          lock_ttl, for_update_ts);
}

butil::Status RocksEngine::TxnWriter::TxnPessimisticRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                                 int64_t for_update_ts,
                                                                 const std::vector<std::string>& keys) {
  return TxnEngineHelper::PessimisticRollback(txn_writer_raw_engine_, rocks_engine_, ctx, start_ts, for_update_ts, keys);
}

butil::Status RocksEngine::TxnWriter::TxnPrewrite(
    std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations, const std::string& primary_lock,
    int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc, int64_t max_commit_ts,
    const std::vector<int64_t>& pessimistic_checks, const std::map<int64_t, int64_t>& for_update_ts_checks,
    const std::map<int64_t, std::string>& lock_extra_datas) {
  return TxnEngineHelper::Prewrite(txn_writer_raw_engine_, rocks_engine_, ctx, mutations, primary_lock, start_ts,
                                   lock_ttl, txn_size, try_one_pc, max_commit_ts, pessimistic_checks,
                                   for_update_ts_checks, lock_extra_datas);
}

butil::Status RocksEngine::TxnWriter::TxnCommit(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                                    const std::vector<std::string>& keys) {
  return TxnEngineHelper::Commit(txn_writer_raw_engine_, rocks_engine_, ctx, start_ts, commit_ts, keys);
}

butil::Status RocksEngine::TxnWriter::TxnCheckTxnStatus(std::shared_ptr<Context> ctx,
                                                            const std::string& primary_key, int64_t lock_ts,
                                                            int64_t caller_start_ts, int64_t current_ts) {
  return TxnEngineHelper::CheckTxnStatus(txn_writer_raw_engine_, rocks_engine_, ctx, primary_key, lock_ts,
                                         caller_start_ts, current_ts);
}

butil::Status RocksEngine::TxnWriter::TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                         int64_t commit_ts, const std::vector<std::string>& keys) {
  return TxnEngineHelper::ResolveLock(txn_writer_raw_engine_, rocks_engine_, ctx, start_ts, commit_ts, keys);
}

butil::Status RocksEngine::TxnWriter::TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                           const std::vector<std::string>& keys) {
  return TxnEngineHelper::BatchRollback(txn_writer_raw_engine_, rocks_engine_, ctx, start_ts, keys);
}

butil::Status RocksEngine::TxnWriter::TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock,
                                                       int64_t start_ts, int64_t advise_lock_ttl) {
  return TxnEngineHelper::HeartBeat(txn_writer_raw_engine_, rocks_engine_, ctx, primary_lock, start_ts, advise_lock_ttl);
}

butil::Status RocksEngine::TxnWriter::TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                                         const std::string& end_key) {
  return TxnEngineHelper::DeleteRange(txn_writer_raw_engine_, rocks_engine_, ctx, start_key, end_key);
}

butil::Status RocksEngine::TxnWriter::TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  return TxnEngineHelper::Gc(txn_writer_raw_engine_, rocks_engine_, ctx, safe_point_ts);
}
}  // namespace dingodb
