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

#ifndef DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ROCKS_ENGINE_H_

#include <memory>

#include "config/config.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "event/event.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "mvcc/reader.h"
#include "mvcc/ts_provider.h"

namespace dingodb {

class MonoStoreEngine;
using MonoStoreEnginePtr = std::shared_ptr<MonoStoreEngine>;

class MonoStoreEngine : public Engine {
 public:
  MonoStoreEngine(RawEnginePtr rocks_raw_engine, RawEnginePtr bdb_raw_engine, EventListenerCollectionPtr listeners,
                  mvcc::TsProviderPtr ts_provider, std::shared_ptr<StoreMetaManager> store_meta_manager,
                  std::shared_ptr<StoreMetricsManager> store_metrics_manager);
  ~MonoStoreEngine() override = default;

  MonoStoreEngine(const MonoStoreEngine& rhs) = delete;
  MonoStoreEngine& operator=(const MonoStoreEngine& rhs) = delete;
  MonoStoreEngine(MonoStoreEngine&& rhs) = delete;
  MonoStoreEngine& operator=(MonoStoreEngine&& rhs) = delete;

  MonoStoreEnginePtr GetSelfPtr();

  static MonoStoreEnginePtr New(RawEnginePtr rocks_raw_engine, RawEnginePtr bdb_raw_engine,
                                EventListenerCollectionPtr listeners, mvcc::TsProviderPtr ts_provider,
                                std::shared_ptr<StoreMetaManager> store_meta_manager,
                                std::shared_ptr<StoreMetricsManager> store_metrics_manager) {
    return std::make_shared<MonoStoreEngine>(rocks_raw_engine, bdb_raw_engine, listeners, ts_provider,
                                             store_meta_manager, store_metrics_manager);
  }

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover() override;
  std::string GetName() override;
  pb::common::StorageEngine GetID() override;
  RawEnginePtr GetRawEngine(pb::common::RawEngine type) override;

  std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }
  butil::Status SaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }
  butil::Status AyncSaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }

  butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                           WriteCbFunc write_cb) override;
  int DispatchEvent(dingodb::EventType, std::shared_ptr<dingodb::Event> event);

  std::shared_ptr<StoreMetaManager> GetStoreMetaManager();
  std::shared_ptr<StoreMetricsManager> GetStoreMetricsManager();

  class Reader : public Engine::Reader {
   public:
    Reader(RawEngine::ReaderPtr reader) : reader_(reader) {}
    butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;

    butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                         std::vector<pb::common::KeyValue>& kvs) override;

    butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                          int64_t& count) override;

   private:
    RawEngine::ReaderPtr reader_;
  };

  class Writer : public Engine::Writer {
   public:
    Writer(MonoStoreEnginePtr mono_engine, mvcc::TsProviderPtr ts_provider)
        : mono_engine_(mono_engine), ts_provider_(ts_provider) {}
    butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                           std::vector<bool>& key_states) override;
    butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) override;
    butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                bool is_atomic, std::vector<bool>& key_states) override;
    butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                  const std::vector<std::string>& expect_values, bool is_atomic,
                                  std::vector<bool>& key_states) override;

   private:
    MonoStoreEnginePtr mono_engine_;
    mvcc::TsProviderPtr ts_provider_;
  };

  // Vector reader
  class VectorReader : public Engine::VectorReader {
   public:
    VectorReader(mvcc::ReaderPtr reader) : reader_(reader) {}

    butil::Status VectorBatchSearch(std::shared_ptr<VectorReader::Context> ctx,
                                    std::vector<pb::index::VectorWithDistanceResult>& results) override;
    butil::Status VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                   std::vector<pb::common::VectorWithId>& vector_with_ids) override;
    butil::Status VectorGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                    int64_t& vector_id) override;
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
    butil::Status VectorGetBorderIdForDocument(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                               int64_t& vector_id) override;
#endif
    butil::Status VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                  std::vector<pb::common::VectorWithId>& vector_with_ids) override;
    butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                         VectorIndexWrapperPtr vector_index,
                                         pb::common::VectorIndexMetrics& region_metrics) override;

    butil::Status VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count) override;
    butil::Status VectorCountMemory(std::shared_ptr<VectorReader::Context> ctx, int64_t& count) override;
    butil::Status VectorBuild(std::shared_ptr<VectorReader::Context> ctx,
                              const pb::common::VectorBuildParameter& parameter, int64_t ts,
                              pb::common::VectorStateParameter& vector_state_parameter) override;
    butil::Status VectorLoad(std::shared_ptr<VectorReader::Context> ctx,
                             const pb::common::VectorLoadParameter& parameter,
                             pb::common::VectorStateParameter& vector_state_parameter) override;
    butil::Status VectorStatus(std::shared_ptr<VectorReader::Context> ctx,
                               pb::common::VectorStateParameter& vector_state_parameter,
                               pb::error::Error& internal_error) override;
    butil::Status VectorReset(std::shared_ptr<VectorReader::Context> ctx, bool delete_data_file,
                              pb::common::VectorStateParameter& vector_state_parameter) override;
    butil::Status VectorDump(std::shared_ptr<VectorReader::Context> ctx, bool dump_all,
                             std::vector<std::string>& dump_datas) override;

    butil::Status VectorBatchSearchDebug(std::shared_ptr<VectorReader::Context> ctx,
                                         std::vector<pb::index::VectorWithDistanceResult>& results,
                                         int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                         int64_t& search_time_us) override;

   private:
    mvcc::ReaderPtr reader_;
  };

  // Document reader
  class DocumentReader : public Engine::DocumentReader {
   public:
    DocumentReader(mvcc::ReaderPtr reader) : reader_(reader) {}

    butil::Status DocumentSearch(std::shared_ptr<DocumentReader::Context> ctx,
                                 std::vector<pb::common::DocumentWithScore>& results) override;
    butil::Status DocumentSearchAll(std::shared_ptr<DocumentReader::Context> ctx, bool& has_more,
                                    std::vector<pb::common::DocumentWithScore>& results) override;
    butil::Status DocumentBatchQuery(std::shared_ptr<DocumentReader::Context> ctx,
                                     std::vector<pb::common::DocumentWithId>& document_with_ids) override;
    butil::Status DocumentGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                      int64_t& document_id) override;
    butil::Status DocumentScanQuery(std::shared_ptr<DocumentReader::Context> ctx,
                                    std::vector<pb::common::DocumentWithId>& document_with_ids) override;
    butil::Status DocumentGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                           DocumentIndexWrapperPtr document_index,
                                           pb::common::DocumentIndexMetrics& region_metrics) override;

    butil::Status DocumentCount(int64_t ts, const pb::common::Range& range, int64_t& count) override;

   private:
    mvcc::ReaderPtr reader_;
  };

  class TxnReader : public Engine::TxnReader {
   public:
    TxnReader(RawEnginePtr engine) : txn_reader_raw_engine_(engine) {}

    butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys,
                              std::vector<pb::common::KeyValue>& kvs, const std::set<int64_t>& resolved_locks,
                              pb::store::TxnResultInfo& txn_result_info) override;
    butil::Status TxnScan(std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range, int64_t limit,
                          bool key_only, bool is_reverse, const std::set<int64_t>& resolved_locks,
                          bool disable_coprocessor, const pb::common::CoprocessorV2& coprocessor,
                          pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                          bool& has_more, std::string& end_scan_key) override;
    butil::Status TxnScanLock(std::shared_ptr<Context> ctx, int64_t min_lock_ts, int64_t max_lock_ts,
                              const pb::common::Range& range, int64_t limit,
                              std::vector<pb::store::LockInfo>& lock_infos, bool& has_more,
                              std::string& end_scan_key) override;

   private:
    RawEnginePtr txn_reader_raw_engine_;
  };

  class TxnWriter : public Engine::TxnWriter {
   public:
    TxnWriter(RawEnginePtr raw_engine, MonoStoreEnginePtr rocks_engine)
        : txn_writer_raw_engine_(raw_engine), mono_engine_(rocks_engine) {}

    butil::Status TxnPessimisticLock(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                                     const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                     int64_t for_update_ts, bool return_values,
                                     std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status TxnPessimisticRollback(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                         int64_t for_update_ts, const std::vector<std::string>& keys) override;
    butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, store::RegionPtr region,
                              const std::vector<pb::store::Mutation>& mutations, const std::string& primary_lock,
                              int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc,
                              int64_t min_commit_ts, int64_t max_commit_ts,
                              const std::vector<int64_t>& pessimistic_checks,
                              const std::map<int64_t, int64_t>& for_update_ts_checks,
                              const std::map<int64_t, std::string>& lock_extra_datas,
                              const std::vector<std::string>& secondaries) override;
    butil::Status TxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts, int64_t commit_ts,
                            const std::vector<std::string>& keys) override;
    butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                    int64_t caller_start_ts, int64_t current_ts, bool force_sync_commit) override;
    butil::Status TxnCheckSecondaryLocks(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                         const std::vector<std::string>& keys) override;
    butil::Status TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                 const std::vector<std::string>& keys) override;
    butil::Status TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                   const std::vector<std::string>& keys) override;
    butil::Status TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                               int64_t advise_lock_ttl) override;
    butil::Status TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                 const std::string& end_key) override;
    butil::Status TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) override;

   private:
    RawEnginePtr txn_writer_raw_engine_;
    MonoStoreEnginePtr mono_engine_;
  };

  mvcc::ReaderPtr NewMVCCReader(pb::common::RawEngine type) override;
  Engine::ReaderPtr NewReader(pb::common::RawEngine type) override;
  Engine::WriterPtr NewWriter(pb::common::RawEngine type) override;
  Engine::VectorReaderPtr NewVectorReader(pb::common::RawEngine type) override;
  Engine::DocumentReaderPtr NewDocumentReader(pb::common::RawEngine type) override;
  Engine::TxnReaderPtr NewTxnReader(pb::common::RawEngine type) override;
  Engine::TxnWriterPtr NewTxnWriter(pb::common::RawEngine type) override;

 private:
  RawEnginePtr rocks_raw_engine_;  // RocksDB, the system engine, for meta and data
  RawEnginePtr bdb_raw_engine_;    // BDB, the engine for data
  EventListenerCollectionPtr listeners_;

  mvcc::TsProviderPtr ts_provider_;

  std::shared_ptr<StoreMetaManager> store_meta_manager_;
  std::shared_ptr<StoreMetricsManager> store_metrics_manager_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
