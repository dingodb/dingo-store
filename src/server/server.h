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

#include <cstdint>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "common/meta_control.h"
#include "common/safe_map.h"
#include "config/config_manager.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/coordinator_interaction.h"
#include "coordinator/kv_control.h"
#include "coordinator/tso_control.h"
#include "crontab/crontab.h"
#include "engine/raw_engine.h"
#include "engine/storage.h"
#include "log/log_storage_manager.h"
#include "meta/meta_reader.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
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

  // Valiate coordinator is connected and valid.
  static bool ValiateCoordinator() { return true; }

  // Every server instance has id, the id is allocated by coordinator.
  bool InitServerID();

  // Init directory
  bool InitDirectory();

  // Init raw storage engines;
  bool InitRawEngine();

  // Init storage engines;
  bool InitEngine();

  // Init coordinator interaction
  bool InitCoordinatorInteraction();
  bool InitCoordinatorInteractionForAutoIncrement();

  // Init log Storage manager.
  bool InitLogStorageManager();

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

  static pb::node::LogLevel GetDingoLogLevel(std::shared_ptr<dingodb::Config> config);

  // Init Heartbeat
  bool InitHeartbeat();

  // Init PreSplitChecker
  bool InitPreSplitChecker();

  butil::Status StartMetaRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartKvRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartTsoRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);
  butil::Status StartAutoIncrementRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);

  // Recover server state, include store/region/raft.
  bool Recover();

  void Destroy();

  bool Ip2Hostname(std::string& ip2hostname);

  int64_t Id() const { return id_; }
  std::string Keyring() const { return keyring_; }

  std::string ServerAddr() { return server_addr_; }
  butil::EndPoint ServerEndpoint() { return server_endpoint_; }
  void SetServerEndpoint(const butil::EndPoint& endpoint) {
    server_endpoint_ = endpoint;
    server_addr_ = Helper::EndPointToStr(endpoint);
  }

  butil::EndPoint RaftEndpoint() { return raft_endpoint_; }
  void SetRaftEndpoint(const butil::EndPoint& endpoint) { raft_endpoint_ = endpoint; }

  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteraction() { return coordinator_interaction_; }
  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteractionIncr() { return coordinator_interaction_incr_; }

  std::shared_ptr<Engine> GetEngine() { return raft_engine_; }
  std::shared_ptr<RawEngine> GetRawEngine() { return raw_engine_; }

  std::shared_ptr<RaftStoreEngine> GetRaftStoreEngine() {
    auto engine = GetEngine();
    if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
      return std::dynamic_pointer_cast<RaftStoreEngine>(engine);
    }
    return nullptr;
  }

  std::shared_ptr<MetaReader> GetMetaReader() { return meta_reader_; }
  std::shared_ptr<MetaWriter> GetMetaWriter() { return meta_writer_; }

  std::shared_ptr<LogStorageManager> GetLogStorageManager() { return log_storage_; }

  std::shared_ptr<Storage> GetStorage() { return storage_; }
  std::shared_ptr<StoreMetaManager> GetStoreMetaManager() { return store_meta_manager_; }
  store::RegionPtr GetRegion(int64_t region_id);
  std::vector<store::RegionPtr> GetAllAliveRegion();
  std::shared_ptr<StoreMetricsManager> GetStoreMetricsManager() { return store_metrics_manager_; }
  std::shared_ptr<CrontabManager> GetCrontabManager() { return crontab_manager_; }

  std::shared_ptr<StoreController> GetStoreController() { return store_controller_; }
  std::shared_ptr<RegionController> GetRegionController() { return region_controller_; }
  std::shared_ptr<RegionCommandManager> GetRegionCommandManager() { return region_command_manager_; }
  VectorIndexManagerPtr GetVectorIndexManager() { return vector_index_manager_; }
  std::shared_ptr<CoordinatorControl> GetCoordinatorControl() { return coordinator_control_; }
  std::shared_ptr<AutoIncrementControl>& GetAutoIncrementControl() { return auto_increment_control_; }
  std::shared_ptr<TsoControl> GetTsoControl() { return tso_control_; }
  std::shared_ptr<KvControl> GetKvControl() { return kv_control_; }

  void SetEndpoints(const std::vector<butil::EndPoint> endpoints) { endpoints_ = endpoints; }

  std::shared_ptr<Heartbeat> GetHeartbeat() { return heartbeat_; }

  std::string GetCheckpointPath() { return checkpoint_path_; }

  std::string GetStorePath() {
    auto config = ConfigManager::GetInstance().GetRoleConfig();
    return config == nullptr ? "" : config->GetString("store.path");
  }

  std::string GetRaftPath() {
    auto config = ConfigManager::GetInstance().GetRoleConfig();
    return config == nullptr ? "" : config->GetString("raft.path");
  }

  std::string GetRaftLogPath() {
    auto config = ConfigManager::GetInstance().GetRoleConfig();
    return config == nullptr ? "" : config->GetString("raft.log_path");
  }

  std::string GetIndexPath() {
    auto config = ConfigManager::GetInstance().GetRoleConfig();
    return config == nullptr ? "" : config->GetString("vector.index_path");
  }

  bool IsReadOnly() const { return is_read_only_; }
  void SetReadOnly(bool is_read_only) { is_read_only_ = is_read_only; }

  bool IsLeader(int64_t region_id) { return storage_->IsLeader(region_id); }

  std::shared_ptr<PreSplitChecker> GetPreSplitChecker() { return pre_split_checker_; }

  Server(const Server&) = delete;
  const Server& operator=(const Server&) = delete;

 private:
  Server() { heartbeat_ = std::make_shared<Heartbeat>(); }
  ~Server() = default;

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
  // Service ip and port.
  butil::EndPoint server_endpoint_;
  // Service ip and port.
  std::string server_addr_;
  // Raft ip and port.
  butil::EndPoint raft_endpoint_;
  std::vector<butil::EndPoint> endpoints_;

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
  std::shared_ptr<RawEngine> raw_engine_;

  // Meta reader
  std::shared_ptr<MetaReader> meta_reader_;
  // Meta writer
  std::shared_ptr<MetaWriter> meta_writer_;

  // This is log storage manager
  std::shared_ptr<LogStorageManager> log_storage_;

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

  // Pre split checker
  std::shared_ptr<PreSplitChecker> pre_split_checker_;

  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;

  // Is cluster read-only
  bool is_read_only_ = false;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVER_H_
