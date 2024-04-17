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


#include "config/config.h"
#include "common/meta_control.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "engine/raft_store_engine.h"
#include "event/event.h"
//#include "standalone/stand_alone_node_manager.h"


namespace dingodb {

class RocksEngine : public Engine {
 public:
  RocksEngine(std::shared_ptr<RawEngine> raw_rocks_engine, std::shared_ptr<RawEngine> raw_bdb_engine, std::shared_ptr<EventListenerCollection> listeners);
  ~RocksEngine() override = default;
  std::shared_ptr<RocksEngine> GetSelfPtr();

  RocksEngine(const RocksEngine& rhs) = delete;
  RocksEngine& operator=(const RocksEngine& rhs) = delete;
  RocksEngine(RocksEngine&& rhs) = delete;
  RocksEngine& operator=(RocksEngine&& rhs) = delete;

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover();
  std::string GetName() override;
  pb::common::StorageEngine GetID() override;
  std::shared_ptr<RawEngine> GetRawEngine(pb::common::RawEngine type) override;

  std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }
  butil::Status SaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }
  butil::Status AyncSaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }

  butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                           WriteCbFunc write_cb) override;
  int DispatchEvent(dingodb::EventType, std::shared_ptr<dingodb::Event> event);
  //butil::Status AddNode(store::RegionPtr region, const RaftControlAble::AddNodeParameter& parameter);
  //std::shared_ptr<StandAloneNode> GetNode(int64_t region_id);
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
    Writer(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<RocksEngine> rocks_engine)
        : writer_raw_engine_(raw_engine), rocks_engine_(rocks_engine) {}
    butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys) override;
    butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) override;
    butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                bool is_atomic, std::vector<bool>& key_states) override;
    butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                  const std::vector<std::string>& expect_values, bool is_atomic,
                                  std::vector<bool>& key_states) override;

   private:
    std::shared_ptr<RawEngine> writer_raw_engine_;
    std::shared_ptr<RocksEngine> rocks_engine_;
  };


  // Vector reader
  class VectorReader : public Engine::VectorReader {
   public:
    VectorReader(RawEngine::ReaderPtr reader) : reader_(reader) {}

    butil::Status VectorBatchSearch(std::shared_ptr<VectorReader::Context> ctx,                           // NOLINT
                                    std::vector<pb::index::VectorWithDistanceResult>& results) override;  // NOLINT
    butil::Status VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,                            // NOLINT
                                   std::vector<pb::common::VectorWithId>& vector_with_ids) override;      // NOLINT
    butil::Status VectorGetBorderId(const pb::common::Range& region_range, bool get_min,                  // NOLINT
                                    int64_t& vector_id) override;                                         // NOLINT
    butil::Status VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,                             // NOLINT
                                  std::vector<pb::common::VectorWithId>& vector_with_ids) override;       // NOLINT
    butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,        // NOLINT
                                         VectorIndexWrapperPtr vector_index,                              // NOLINT
                                         pb::common::VectorIndexMetrics& region_metrics) override;        // NOLINT

    butil::Status VectorCount(const pb::common::Range& range, int64_t& count) override;  // NOLINT

    butil::Status VectorBatchSearchDebug(std::shared_ptr<VectorReader::Context> ctx,  // NOLINT
                                         std::vector<pb::index::VectorWithDistanceResult>& results,
                                         int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                         int64_t& search_time_us) override;  // NOLINT

   private:
    RawEngine::ReaderPtr reader_;
  };


  class TxnReader : public Engine::TxnReader {
   public:
    TxnReader(std::shared_ptr<RawEngine> engine) : txn_reader_raw_engine_(engine) {}

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
    std::shared_ptr<RawEngine> txn_reader_raw_engine_;
  };


  class TxnWriter : public Engine::TxnWriter {
   public:
    TxnWriter(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<RocksEngine> rocks_engine)
        : txn_writer_raw_engine_(raw_engine), rocks_engine_(rocks_engine) {}

    butil::Status TxnPessimisticLock(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                                     const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                     int64_t for_update_ts) override;
    butil::Status TxnPessimisticRollback(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t for_update_ts,
                                         const std::vector<std::string>& keys) override;
    butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                              const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                              bool try_one_pc, int64_t max_commit_ts, const std::vector<int64_t>& pessimistic_checks,
                              const std::map<int64_t, int64_t>& for_update_ts_checks,
                              const std::map<int64_t, std::string>& lock_extra_datas) override;
    butil::Status TxnCommit(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                            const std::vector<std::string>& keys) override;
    butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                    int64_t caller_start_ts, int64_t current_ts) override;
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
    std::shared_ptr<RawEngine> txn_writer_raw_engine_;
    std::shared_ptr<RocksEngine> rocks_engine_;
  };
  
  std::shared_ptr<Engine::Reader> NewReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::Writer> NewWriter(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::VectorReader> NewVectorReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::TxnReader> NewTxnReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::TxnWriter> NewTxnWriter(pb::common::RawEngine type) override;

  protected:
  std::shared_ptr<RawEngine> raw_rocks_engine;  // RocksDB, the system engine, for meta and data
  std::shared_ptr<RawEngine> raw_bdb_engine;    // BDB, the engine for data
  std::shared_ptr<EventListenerCollection> listeners_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
