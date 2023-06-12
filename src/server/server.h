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

#include <memory>
#include <string>

#include "brpc/channel.h"
#include "common/meta_control.h"
#include "config/config_manager.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/coordinator_interaction.h"
#include "crontab/crontab.h"
#include "engine/raw_engine.h"
#include "engine/storage.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "proto/common.pb.h"
#include "store/heartbeat.h"
#include "store/region_controller.h"
#include "store/store_controller.h"

template <typename T>
struct DefaultSingletonTraits;

namespace dingodb {

class Server {
 public:
  static Server* GetInstance();

  void SetRole(pb::common::ClusterRole role);
  pb::common::ClusterRole GetRole() { return this->role_; };

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

  static pb::node::LogLevel GetDingoLogLevel(std::shared_ptr<dingodb::Config> config);

  // Init Heartbeat
  bool InitHeartbeat();

  butil::Status StartMetaRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);

  butil::Status StartAutoIncrementRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine);

  // Recover server state, include store/region/raft.
  bool Recover();

  void Destroy();

  uint64_t Id() const { return id_; }
  std::string Keyring() const { return keyring_; }

  butil::EndPoint ServerEndpoint() { return server_endpoint_; }
  void SetServerEndpoint(const butil::EndPoint& endpoint) { server_endpoint_ = endpoint; }

  butil::EndPoint RaftEndpoint() { return raft_endpoint_; }
  void SetRaftEndpoint(const butil::EndPoint& endpoint) { raft_endpoint_ = endpoint; }

  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteraction() { return coordinator_interaction_; }
  std::shared_ptr<CoordinatorInteraction> GetCoordinatorInteractionIncr() { return coordinator_interaction_incr_; }

  std::shared_ptr<Engine> GetEngine() { return engine_; }
  std::shared_ptr<RawEngine> GetRawEngine() { return raw_engine_; }

  std::shared_ptr<Storage> GetStorage() { return storage_; }
  std::shared_ptr<StoreMetaManager> GetStoreMetaManager() { return store_meta_manager_; }
  std::shared_ptr<StoreMetricsManager> GetStoreMetricsManager() { return store_metrics_manager_; }
  std::shared_ptr<CrontabManager> GetCrontabManager() { return crontab_manager_; }

  std::shared_ptr<StoreController> GetStoreController() { return store_controller_; }
  std::shared_ptr<RegionController> GetRegionController() { return region_controller_; }
  std::shared_ptr<RegionCommandManager> GetRegionCommandManager() { return region_command_manager_; }
  std::shared_ptr<CoordinatorControl> GetCoordinatorControl() { return coordinator_control_; }
  std::shared_ptr<AutoIncrementControl>& GetAutoIncrementControlReference() { return auto_increment_control_; }

  void SetEndpoints(const std::vector<butil::EndPoint> endpoints) { endpoints_ = endpoints; }

  std::shared_ptr<Heartbeat> GetHeartbeat() { return heartbeat_; }

  std::shared_ptr<Config> GetConfig() { return ConfigManager::GetInstance()->GetConfig(role_); }

  std::string GetCheckpointPath() { return checkpoint_path_; }

  std::string GetIndexPath() { return index_path_; }

  Server(const Server&) = delete;
  const Server& operator=(const Server&) = delete;

 private:
  Server() { heartbeat_ = std::make_shared<Heartbeat>(); }
  ~Server() = default;

  std::shared_ptr<pb::common::RegionDefinition> CreateCoordinatorRegion(const std::shared_ptr<Config>& config,
                                                                        uint64_t region_id,
                                                                        const std::string& region_name
                                                                        /*std::shared_ptr<Context>& ctx*/);

  friend struct DefaultSingletonTraits<Server>;

  // This is server instance id, every store server has one id, it's unique,
  // represent store's identity, provided by coordinator.
  // read from store config file.
  uint64_t id_;
  // This is keyring, the password for this instance to join in the cluster
  std::string keyring_;
  // Role, include store/coordinator
  pb::common::ClusterRole role_;
  // Service ip and port.
  butil::EndPoint server_endpoint_;
  // Raft ip and port.
  butil::EndPoint raft_endpoint_;
  std::vector<butil::EndPoint> endpoints_;

  // coordinator interaction
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_;
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_incr_;

  // All store engine, include MemEngine/RaftKvEngine/RocksEngine
  std::shared_ptr<Engine> engine_;
  std::shared_ptr<RawEngine> raw_engine_;

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

  // This is manage coordinator meta data, like store state and region state.
  std::shared_ptr<CoordinatorControl> coordinator_control_;

  // This is store and coordinator heartbeat.
  std::shared_ptr<Heartbeat> heartbeat_;
  // This is manage auto increment meta data,
  std::shared_ptr<AutoIncrementControl> auto_increment_control_;

  // checkpoint directory
  std::string checkpoint_path_;

  // index directory
  std::string index_path_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVER_H_
