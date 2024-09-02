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

#ifndef DINGODB_STORE_SERVER_H_
#define DINGODB_STORE_SERVER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "bthread/types.h"
#include "common/safe_map.h"
#include "common/stream.h"
#include "common/threadpool.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/coordinator_interaction.h"
#include "coordinator/kv_control.h"
#include "coordinator/tso_control.h"
#include "crontab/crontab.h"
#include "document/document_index_manager.h"
#include "engine/mono_store_engine.h"
#include "engine/raw_engine.h"
#include "engine/rocks_raw_engine.h"
#include "engine/storage.h"
#include "log/rocks_log_storage.h"
#include "meta/meta_reader.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "mvcc/ts_provider.h"
#include "proto/common.pb.h"
#include "split/split_checker.h"
#include "store/heartbeat.h"
#include "store/region_controller.h"
#include "store/store_controller.h"
#include "vector/vector_index_manager.h"

namespace dingodb {

class Server {
 public:
  static Server& GetInstance();

  // Init config.
  bool InitConfig(const std::string& filename);

  // Init log.
  bool InitLog();

  // Every server instance has id, the id is allocated by coordinator.
  bool InitServerID();

  // Init directory
  bool InitDirectory();

  // Init RocksRawEngine
  bool InitRocksRawEngine();

  // Init storage engines;
  bool InitEngine();

  // Init coordinator interaction
  bool InitCoordinatorInteraction();
  bool InitCoordinatorInteractionForAutoIncrement();

  // Init log Storage manager.
  bool InitLogStorage();

  // Init storage engine.
  bool InitStorage();

  // Init store meta manager
  bool InitStoreMetaManager();

  // Init crontab heartbeat
  bool InitCrontabManager();

  // Init store controller
  bool InitStoreController();

  // Init region command manager
  bool InitRegionCommandManager();

  // Init region controller
  bool InitRegionController();

  bool InitStoreMetricsManager();

  // Init vector index manager
  bool InitVectorIndexManager();

  // Init document index manager
  bool InitDocumentIndexManager();

  static LogLevel GetDingoLogLevel(std::shared_ptr<dingodb::Config> config);

  // Init Heartbeat
  bool InitHeartbeat();

  // Init PreSplitChecker
  bool InitPreSplitChecker();

  // Init TsProvider
  bool InitTsProvider();

  // Init StreamManager
  bool InitStreamManager();

  butil::Status StartMetaRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartKvRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartTsoRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartAutoIncrementRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);

  // Recover server state, include store/region/raft.
  bool Recover();

  void Destroy();

  bool Ip2Hostname(std::string& ip2hostname);

  int64_t Id() const;
  std::string Keyring() const;

  const std::string& ServiceDumpDir();
  std::string LogDir();
  std::string PidFilePath();
  std::string ServerAddr();
  pb::common::Location ServerLocation();
  void SetServerLocation(const pb::common::Location& location);
  butil::EndPoint ServerListenEndpoint();
  void SetServerListenEndpoint(const butil::EndPoint& endpoint);
  butil::EndPoint RaftEndpoint();
  void SetRaftEndpoint(const butil::EndPoint& endpoint);
  butil::EndPoint RaftListenEndpoint();
  void SetRaftListenEndpoint(const butil::EndPoint& endpoint);

  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteraction();
  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteractionIncr();

  std::shared_ptr<RawEngine> GetRawEngine(pb::common::RawEngine type);
  std::shared_ptr<Engine> GetEngine(pb::common::StorageEngine store_engine_type);

  std::shared_ptr<RaftStoreEngine> GetRaftStoreEngine();
  std::shared_ptr<MonoStoreEngine> GetMonoStoreEngine();

  std::shared_ptr<MetaReader> GetMetaReader();
  std::shared_ptr<MetaWriter> GetMetaWriter();

  wal::LogStoragePtr GetRaftLogStorage();

  std::shared_ptr<Storage> GetStorage();

  std::shared_ptr<StoreMetaManager> GetStoreMetaManager();

  // Shortcut
  store::RegionPtr GetRegion(int64_t region_id);
  std::vector<store::RegionPtr> GetAllAliveRegion();
  store::RaftMetaPtr GetRaftMeta(int64_t region_id);

  std::shared_ptr<StoreMetricsManager> GetStoreMetricsManager();
  std::shared_ptr<CrontabManager> GetCrontabManager();

  std::shared_ptr<StoreController> GetStoreController();
  std::shared_ptr<RegionController> GetRegionController();
  std::shared_ptr<RegionCommandManager> GetRegionCommandManager();
  VectorIndexManagerPtr GetVectorIndexManager();
  DocumentIndexManagerPtr GetDocumentIndexManager();
  std::shared_ptr<CoordinatorControl> GetCoordinatorControl();
  std::shared_ptr<AutoIncrementControl> GetAutoIncrementControl();
  std::shared_ptr<TsoControl> GetTsoControl();
  std::shared_ptr<KvControl> GetKvControl();
  void SetCoordinatorPeerEndpoints(const std::vector<butil::EndPoint>& endpoints);

  std::shared_ptr<Heartbeat> GetHeartbeat();

  std::string GetCheckpointPath();

  static std::string GetStorePath();

  static std::string GetRaftPath();
  static std::string GetRaftMetaPath();
  static std::string GetRaftLogPath();
  static std::string GetRaftSnapshotPath();

  static std::string GetVectorIndexPath();
  static std::string GetDocumentIndexPath();

  bool IsClusterReadOnlyOrForceReadOnly() const;

  bool IsClusterReadOnly() const;
  std::string GetClusterReadOnlyReason();
  void SetClusterReadOnly(bool is_read_only, const std::string& read_only_reason);

  bool IsClusterForceReadOnly() const;
  std::string GetClusterForceReadOnlyReason();
  void SetClusterForceReadOnly(bool is_read_only, const std::string& read_only_reason);

  bool IsLeader(int64_t region_id);
  std::shared_ptr<PreSplitChecker> GetPreSplitChecker();

  void SetStoreServiceReadWorkerSet(WorkerSetPtr worker_set);
  void SetStoreServiceWriteWorkerSet(WorkerSetPtr worker_set);
  void SetIndexServiceReadWorkerSet(WorkerSetPtr worker_set);
  void SetIndexServiceWriteWorkerSet(WorkerSetPtr worker_set);
  void SetApplyWorkerSet(WorkerSetPtr worker_set);

  WorkerSetPtr GetApplyWorkerSet();

  std::vector<std::vector<std::string>> GetStoreServiceReadWorkerSetTrace();
  std::vector<std::vector<std::string>> GetStoreServiceWriteWorkerSetTrace();
  std::vector<std::vector<std::string>> GetIndexServiceReadWorkerSetTrace();
  std::vector<std::vector<std::string>> GetIndexServiceWriteWorkerSetTrace();
  std::vector<std::vector<std::string>> GetVectorIndexBackgroundWorkerSetTrace();
  uint64_t GetVectorIndexManagerBackgroundPendingTaskCount();

  std::vector<std::vector<std::string>> GetDocumentIndexBackgroundWorkerSetTrace();
  uint64_t GetDocumentIndexManagerBackgroundPendingTaskCount();

  std::string GetAllWorkSetPendingTaskCount();

  ThreadPoolPtr GetVectorIndexThreadPool();

  mvcc::TsProviderPtr GetTsProvider();

  StreamManagerPtr GetStreamManager();

  Server(const Server&) = delete;
  const Server& operator=(const Server&) = delete;

 private:
  Server() {
    bthread_mutex_init(&cluster_read_only_reason_mutex_, nullptr);
    bthread_mutex_init(&cluster_force_read_only_reason_mutex_, nullptr);
  };

  ~Server() {
    bthread_mutex_destroy(&cluster_read_only_reason_mutex_);
    bthread_mutex_destroy(&cluster_force_read_only_reason_mutex_);
  };

  std::shared_ptr<pb::common::RegionDefinition> CreateCoordinatorRegion(const std::shared_ptr<Config>& config,
                                                                        int64_t region_id,
                                                                        const std::string& region_name
                                                                        /*std::shared_ptr<Context>& ctx*/);

  // This is server instance id, every store server has one id, it's unique,
  // represent store's identity, provided by coordinator.
  // read from store config file.
  int64_t id_;
  // This is keyring, the password for this instance to join in the cluster
  std::string keyring_;
  // Service host and port.
  pb::common::Location server_location_;
  // Service listen ip and port.
  butil::EndPoint server_listen_endpoint_;
  // Service ip and port.
  std::string server_addr_;
  // Raft peer ip and port.
  butil::EndPoint raft_endpoint_;
  // Raft listen ip and port.
  butil::EndPoint raft_listen_endpoint_;
  std::vector<butil::EndPoint> coordinator_peer_endpoints_;

  struct HostnameItem {
    std::string hostname;
    int64_t timestamp;
  };
  DingoSafeMap<std::string, HostnameItem> ip2hostname_cache_;

  // coordinator interaction
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_;
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_incr_;

  // All store engine, include MemEngine/RaftStoreEngine/RocksEngine
  std::shared_ptr<Engine> raft_engine_;

  std::shared_ptr<Engine> mono_engine_;

  std::shared_ptr<RocksRawEngine> rocks_raw_engine_;
  // Meta reader
  std::shared_ptr<MetaReader> meta_reader_;
  // Meta writer
  std::shared_ptr<MetaWriter> meta_writer_;

  // This is log storage manager
  wal::LogStoragePtr log_storage_;

  // This is a Storage class, deal with all about storage stuff.
  std::shared_ptr<Storage> storage_;

  // This is manage store meta data, like store state and region state.
  std::shared_ptr<StoreMetaManager> store_meta_manager_;
  // This is manage store metric data, like store metric/region metric/rocksdb metric.
  std::shared_ptr<StoreMetricsManager> store_metrics_manager_;
  // This is manage crontab, like heartbeat.
  std::shared_ptr<CrontabManager> crontab_manager_;

  // This is store control, execute admin operation
  std::shared_ptr<StoreController> store_controller_;
  // This is region control, execute admin operation
  std::shared_ptr<RegionController> region_controller_;
  // This is region command manager, save region command
  std::shared_ptr<RegionCommandManager> region_command_manager_;

  // This is vector index manager.
  VectorIndexManagerPtr vector_index_manager_;

  // This is document index manager.
  DocumentIndexManagerPtr document_index_manager_;

  // This is manage coordinator meta data, like store state and region state.
  std::shared_ptr<CoordinatorControl> coordinator_control_;

  // This is manage kv data
  std::shared_ptr<KvControl> kv_control_;

  // This is store and coordinator heartbeat.
  std::shared_ptr<Heartbeat> heartbeat_;
  // This is manage auto increment meta data, of table auto increment.
  std::shared_ptr<AutoIncrementControl> auto_increment_control_;

  // This is manage tso meta data, of timestamp oracle.
  std::shared_ptr<TsoControl> tso_control_;

  // checkpoint directory
  std::string checkpoint_path_;

  // log directory
  std::string log_dir_;

  // service request/response dump directory
  std::string service_dump_dir_;

  // Pre split checker
  std::shared_ptr<PreSplitChecker> pre_split_checker_;

  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;

  // Is cluster read-only
  bool cluster_is_read_only_{false};
  bool cluster_is_force_read_only_{false};

  // read_only reason
  bthread_mutex_t cluster_read_only_reason_mutex_;
  bthread_mutex_t cluster_force_read_only_reason_mutex_;
  std::string cluster_read_only_reason_{};
  std::string cluster_force_read_only_reason_{};

  // reference worker queue, just for trace
  WorkerSetPtr store_service_read_worker_set_{nullptr};
  WorkerSetPtr store_service_write_worker_set_{nullptr};
  WorkerSetPtr index_service_read_worker_set_{nullptr};
  WorkerSetPtr index_service_write_worker_set_{nullptr};

  WorkerSetPtr apply_worker_set_{nullptr};

  // vector index thread pool
  ThreadPoolPtr vector_index_thread_pool_;

  // document index thread pool
  ThreadPoolPtr document_index_thread_pool_;

  // ts provider
  mvcc::TsProviderPtr ts_provider_{nullptr};

  // stream manager
  StreamManagerPtr stream_manager_;
};

// Shortcut
#define GET_REGION_CHANGE_RECORDER Server::GetInstance().GetStoreMetaManager()->GetRegionChangeRecorder()
#define ADD_REGION_CHANGE_RECORD GET_REGION_CHANGE_RECORDER->AddChangeRecord
#define ADD_REGION_CHANGE_RECORD_TIMEPOINT GET_REGION_CHANGE_RECORDER->AddChangeRecordTimePoint

#define GET_STORE_REGION_META Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta()

#define ADD_RAFT_META Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->AddRaftMeta

#define ADD_REGION_METRICS Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVER_H_
