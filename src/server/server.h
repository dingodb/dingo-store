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

#include "brpc/channel.h"
#include "coordinator/coordinator_interaction.h"
#include "crontab/crontab.h"
#include "engine/storage.h"
#include "meta/store_meta_manager.h"
#include "store/store_control.h"

template <typename T>
struct DefaultSingletonTraits;

namespace dingodb {

class Server {
 public:
  static Server* GetInstance();

  void set_role(const std::string& role);

  // Init config.
  bool InitConfig(const std::string& filename);

  // Init log.
  bool InitLog();

  // Valiate coordinator is connected and valid.
  bool ValiateCoordinator();

  // Every server instance has id, the id is allocated by coordinator.
  bool InitServerID();

  // Init storage engines;
  bool InitEngines();

  // Init coordinator interaction
  bool InitCoordinatorInteraction();

  // Pull region infomation for init current node own region.
  bool InitRaftNodeManager();

  // Init storage engine.
  bool InitStorage();

  // Init store meta manager
  bool InitStoreMetaManager();

  // Init crontab heartbeat
  bool InitCrontabManager();

  // Init store control
  bool InitStoreControl();

  void Destroy();

  uint64_t get_id() { return id_; }

  butil::EndPoint get_server_endpoint() { return server_endpoint_; }
  void set_server_endpoint(const butil::EndPoint& endpoint) {
    server_endpoint_ = endpoint;
  }

  butil::EndPoint get_raft_endpoint() { return raft_endpoint_; }
  void set_raft_endpoint(const butil::EndPoint& endpoint) {
    raft_endpoint_ = endpoint;
  }

  std::shared_ptr<CoordinatorInteraction> get_coordinator_interaction() {
    return coordinator_interaction_;
  }

  std::shared_ptr<Engine> get_engine(pb::common::Engine type) {
    auto it = engines_.find(type);
    return (it != engines_.end()) ? it->second : nullptr;
  }

  std::shared_ptr<Storage> get_storage() { return storage_; }
  std::shared_ptr<StoreMetaManager> get_store_meta_manager() {
    return store_meta_manager_;
  }
  std::shared_ptr<CrontabManager> get_crontab_manager() {
    return crontab_manager_;
  }

  std::shared_ptr<StoreControl> get_store_control() { return store_control_; }

 private:
  Server(){};
  ~Server(){};
  friend struct DefaultSingletonTraits<Server>;
  DISALLOW_COPY_AND_ASSIGN(Server);

  // This is server instance id, every store server has one id, it's unique,
  // represent store's identity, provided by coordinator.
  // read from store config file.
  uint64_t id_;
  // Role, include store/coordinator
  std::string role_;
  // Service ip and port.
  butil::EndPoint server_endpoint_;
  // Raft ip and port.
  butil::EndPoint raft_endpoint_;

  // coordinator interaction
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_;

  // All store engine, include MemEngine/RaftKvEngine/RocksEngine
  std::map<pb::common::Engine, std::shared_ptr<Engine> > engines_;

  // This is a Storage class, deal with all about storage stuff.
  std::shared_ptr<Storage> storage_;
  // This is manage store meta data, like store state and region state.
  std::shared_ptr<StoreMetaManager> store_meta_manager_;
  // This is manage crontab, like heartbeat.
  std::shared_ptr<CrontabManager> crontab_manager_;

  // This is store control, execute admin operation, like add/del region etc.
  std::shared_ptr<StoreControl> store_control_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVER_H_