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

#include "server/server.h"

#include <algorithm>
#include <any>
#include <cstdint>
#include <memory>
#include <vector>

#include "braft/util.h"
#include "butil/endpoint.h"
#include "butil/files/file_path.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "coordinator/coordinator_control.h"
#include "engine/engine.h"
#include "engine/mem_engine.h"
#include "engine/raft_kv_engine.h"
#include "engine/raft_meta_engine.h"
#include "engine/raw_rocks_engine.h"
#include "engine/rocks_engine.h"
#include "scan/scan_manager.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "store/heartbeat.h"

namespace dingodb {

void Server::SetRole(pb::common::ClusterRole role) { role_ = role; }

Server* Server::GetInstance() { return Singleton<Server>::get(); }

bool Server::InitConfig(const std::string& filename) {
  std::shared_ptr<Config> const config = std::make_shared<YamlConfig>();
  if (config->LoadFile(filename) != 0) {
    return false;
  }

  ConfigManager::GetInstance()->Register(role_, config);
  return true;
}

bool Server::InitLog() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  FLAGS_log_dir = config->GetString("log.logPath");
  auto role_name = pb::common::ClusterRole_Name(role_);
  DingoLogger::InitLogger(FLAGS_log_dir, role_name);

  DINGO_LOG(INFO) << "log_dir: " << FLAGS_log_dir << " role:" << role_name;

  return true;
}

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  id_ = config->GetInt("cluster.instance_id");
  keyring_ = config->GetString("cluster.keyring");
  return id_ != 0 && (!keyring_.empty());
}

bool Server::InitRawEngines() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  std::shared_ptr<RawEngine> const rock_engine = std::make_shared<RawRocksEngine>();
  if (!rock_engine->Init(config)) {
    DINGO_LOG(ERROR) << "Init RawRocksEngine Failed with Config[" << config->ToString();
    return false;
  }

  raw_engines_.insert(std::make_pair(rock_engine->GetID(), rock_engine));
  return true;
}

bool Server::InitEngines() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  // default Key-Value storage engine
  std::shared_ptr<Engine> raft_kv_engine;

  auto raw_engine = raw_engines_[pb::common::RAW_ENG_ROCKSDB];

  // cooridnator use RaftMetaEngine
  if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // init CoordinatorController
    coordinator_control_ = std::make_shared<CoordinatorControl>(std::make_shared<MetaReader>(raw_engine),
                                                                std::make_shared<MetaWriter>(raw_engine), raw_engine);

    if (!coordinator_control_->Recover()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Recover Failed";
      return false;
    }
    if (!coordinator_control_->Init()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Init Failed";
      return false;
    }

    // init raft_meta_engine
    raft_kv_engine = std::make_shared<RaftMetaEngine>(raw_engine, coordinator_control_);

    // set raft_meta_engine to coordinator_control
    coordinator_control_->SetKvEngine(raft_kv_engine);
  } else {
    raft_kv_engine = std::make_shared<RaftKvEngine>(raw_engine);
    if (!raft_kv_engine->Init(config)) {
      DINGO_LOG(ERROR) << "Init RaftKvEngine failed with Config[" << config->ToString() << "]";
      return false;
    }
  }

  engines_.insert(std::make_pair(raft_kv_engine->GetID(), raft_kv_engine));
  return true;
}

butil::Status Server::StartMetaRegion(std::shared_ptr<Config> config,       // NOLINT
                                      std::shared_ptr<Engine> kv_engine) {  // NOLINT
  /**
   * 1. construct context(must contains role)
   */
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetClusterRole(pb::common::COORDINATOR);

  /*
   * 2. construct region list
   *    1) Region ID
   *    2) Region PeerList
   */
  std::shared_ptr<pb::common::Region> region = std::make_shared<pb::common::Region>();
  region->set_id(Constant::kCoordinatorRegionId);
  region->set_table_id(Constant::kCoordinatorTableId);
  region->set_schema_id(Constant::kCoordinatorSchemaId);
  uint64_t timestamp_us = butil::gettimeofday_ms();
  region->set_create_timestamp(timestamp_us);
  region->set_state(pb::common::RegionState::REGION_NEW);
  region->set_name("COORDINATOR");

  std::string coordinator_list = config->GetString("coordinator.peers");
  std::vector<butil::EndPoint> peer_nodes = Helper::StrToEndpoints(coordinator_list);

  for (auto& peer_node : peer_nodes) {
    auto* peer = region->add_peers();
    auto* location = peer->mutable_raft_location();
    location->set_host(butil::ip2str(peer_node.ip).c_str());
    location->set_port(peer_node.port);
    DINGO_LOG(INFO) << "COORDINATOR set peer node:" << (butil::ip2str(peer_node.ip).c_str()) << ":" << peer_node.port;
  }

  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000");
  range->set_end_key("FFFF");

  DINGO_LOG(INFO) << "Create Region Request:" << region->DebugString();
  auto raft_engine = std::dynamic_pointer_cast<RaftMetaEngine>(kv_engine);
  butil::Status status = raft_engine->InitCoordinatorRegion(ctx, region);

  // set node-manager to coordinator_control here

  return status;
}

bool Server::InitCoordinatorInteraction() {
  coordinator_interaction_ = std::make_shared<CoordinatorInteraction>();

  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  return coordinator_interaction_->Init(config->GetString("cluster.coordinators"));
}

bool Server::InitStorage() {
  storage_ = std::make_shared<Storage>(engines_[pb::common::ENG_RAFT_STORE]);
  return true;
}

bool Server::InitStoreMetaManager() {
  auto engine = raw_engines_[pb::common::RAW_ENG_ROCKSDB];
  store_meta_manager_ =
      std::make_shared<StoreMetaManager>(std::make_shared<MetaReader>(engine), std::make_shared<MetaWriter>(engine));
  return store_meta_manager_->Init();
}

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();

  // Add heartbeat crontab
  std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  crontab->name = "HEARTBEA";
  uint64_t push_interval = config->GetInt("server.pushInterval");
  if (push_interval <= 0) {
    DINGO_LOG(INFO) << "server.pushInterval illegal";
    return false;
  }
  crontab->interval = push_interval;
  crontab->func = Heartbeat::SendStoreHeartbeat;
  crontab->arg = coordinator_interaction_.get();

  crontab_manager_->AddAndRunCrontab(crontab);

  return true;
}

bool Server::InitCrontabManagerForCoordinator() {
  crontab_manager_ = std::make_shared<CrontabManager>();

  // Add heartbeat crontab
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  // add push crontab
  std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
  crontab->name = "PUSH";
  uint64_t push_interval = config->GetInt("server.pushInterval");
  if (push_interval <= 0) {
    DINGO_LOG(INFO) << "server.pushInterval illegal";
    return false;
  }
  crontab->interval = push_interval;
  crontab->func = Heartbeat::SendCoordinatorPushToStore;
  crontab->arg = coordinator_control_.get();

  crontab_manager_->AddAndRunCrontab(crontab);

  // add calculate crontab
  std::shared_ptr<Crontab> crontab_calc = std::make_shared<Crontab>();
  crontab_calc->name = "CALCULATE";
  crontab_calc->interval = push_interval * 60;
  crontab_calc->func = Heartbeat::CalculateTableMetrics;
  crontab_calc->arg = coordinator_control_.get();

  crontab_manager_->AddAndRunCrontab(crontab_calc);

  return true;
}

bool Server::InitStoreControl() {
  store_control_ = std::make_shared<StoreControl>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  uint64_t heartbeat_interval = config->GetInt("server.heartbeatInterval");
  if (heartbeat_interval <= 0) {
    DINGO_LOG(INFO) << "server.heartbeatInterval illegal";
    return false;
  }

  uint64_t push_interval = config->GetInt("server.pushInterval");
  if (push_interval <= 0) {
    DINGO_LOG(INFO) << "server.pushInterval illegal";
    return false;
  }

  if (heartbeat_interval < push_interval) {
    DINGO_LOG(INFO) << "server.heartbeatInterval must bigger than server.pushInterval";
    return false;
  }

  store_control_->SetHeartbeatIntervalMultiple(heartbeat_interval / push_interval);

  DINGO_LOG(INFO) << "SetHeartbeatIntervalMultiple to " << heartbeat_interval / push_interval;

  return store_control_ != nullptr;
}

// add scan factory to CrontabManager
bool Server::AddScanToCrontabManager() {
  // Add scan crontab
  std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  crontab->name = "SCAN";
  uint64_t scan_interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
  if (scan_interval <= 0) {
    DINGO_LOG(ERROR) << "store.scan.scan_interval_ms";
    return false;
  }
  crontab->interval = scan_interval;
  crontab->func = ScanManager::RegularCleaningHandler;
  crontab->arg = ScanManager::GetInstance();

  crontab_manager_->AddAndRunCrontab(crontab);
  return true;
}

bool Server::Recover() {
  if (this->role_ == pb::common::STORE) {
    // Recover region meta data.
    if (!store_meta_manager_->Recover()) {
      DINGO_LOG(ERROR) << "Recover store region meta data failed";
      return false;
    }

    // Recover engine state.
    for (auto& it : engines_) {
      if (!it.second->Recover()) {
        DINGO_LOG(ERROR) << "Recover engine failed, engine " << it.second->GetName();
        return false;
      }
    }
  } else if (this->role_ == pb::common::COORDINATOR) {
  }

  return true;
}

void Server::Destroy() {
  crontab_manager_->Destroy();
  google::ShutdownGoogleLogging();
}

}  // namespace dingodb
