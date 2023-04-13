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
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "scan/scan_manager.h"
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

bool Server::InitRawEngine() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  raw_engine_ = std::make_shared<RawRocksEngine>();
  if (!raw_engine_->Init(config)) {
    DINGO_LOG(ERROR) << "Init RawRocksEngine Failed with Config[" << config->ToString();
    return false;
  }

  return true;
}

bool Server::InitEngine() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  // cooridnator use RaftMetaEngine
  if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // init CoordinatorController
    coordinator_control_ = std::make_shared<CoordinatorControl>(std::make_shared<MetaReader>(raw_engine_),
                                                                std::make_shared<MetaWriter>(raw_engine_), raw_engine_);

    if (!coordinator_control_->Recover()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Recover Failed";
      return false;
    }
    if (!coordinator_control_->Init()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Init Failed";
      return false;
    }

    // init raft_meta_engine
    engine_ = std::make_shared<RaftMetaEngine>(raw_engine_, coordinator_control_);

    // set raft_meta_engine to coordinator_control
    coordinator_control_->SetKvEngine(engine_);
  } else {
    engine_ = std::make_shared<RaftKvEngine>(raw_engine_);
    if (!engine_->Init(config)) {
      DINGO_LOG(ERROR) << "Init RaftKvEngine failed with Config[" << config->ToString() << "]";
      return false;
    }
  }

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
  region->set_create_timestamp(butil::gettimeofday_ms());
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
  storage_ = std::make_shared<Storage>(engine_);
  return true;
}

bool Server::InitStoreMetaManager() {
  store_meta_manager_ = std::make_shared<StoreMetaManager>(std::make_shared<MetaReader>(raw_engine_),
                                                           std::make_shared<MetaWriter>(raw_engine_));
  return store_meta_manager_->Init();
}

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  if (role_ == pb::common::ClusterRole::STORE) {
    // Add heartbeat crontab
    std::shared_ptr<Crontab> heartbeat_crontab = std::make_shared<Crontab>();
    heartbeat_crontab->name = "HEARTBEA";
    uint64_t heartbeat_interval = config->GetInt("server.heartbeatInterval");
    if (heartbeat_interval <= 0) {
      DINGO_LOG(ERROR) << "config server.heartbeatInterval illegal";
      return false;
    }
    heartbeat_crontab->interval = heartbeat_interval;
    heartbeat_crontab->func = Heartbeat::TriggerStoreHeartbeat;
    heartbeat_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(heartbeat_crontab);

    // Add store metrics crontab
    std::shared_ptr<Crontab> metrics_crontab = std::make_shared<Crontab>();
    metrics_crontab->name = "METRICS";
    uint64_t metrics_interval = config->GetInt("server.metricsCollectInterval");
    if (metrics_interval <= 0) {
      DINGO_LOG(ERROR) << "config server.metricsCollectInterval illegal";
      return false;
    }
    metrics_crontab->interval = metrics_interval;
    metrics_crontab->func = [](void*) { Server::GetInstance()->GetStoreMetricsManager()->CollectMetrics(); };
    metrics_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(metrics_crontab);

    // Add scan crontab
    std::shared_ptr<Crontab> scan_crontab = std::make_shared<Crontab>();
    scan_crontab->name = "SCAN";
    uint64_t scan_interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
    if (scan_interval <= 0) {
      DINGO_LOG(ERROR) << "store.scan.scan_interval_ms illegal";
      return false;
    }
    scan_crontab->interval = scan_interval;
    scan_crontab->func = ScanManager::RegularCleaningHandler;
    scan_crontab->arg = ScanManager::GetInstance();

    crontab_manager_->AddAndRunCrontab(scan_crontab);

  } else if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // Add push crontab
    std::shared_ptr<Crontab> push_crontab = std::make_shared<Crontab>();
    push_crontab->name = "PUSH";
    uint64_t push_interval = config->GetInt("server.pushInterval");
    if (push_interval <= 0) {
      DINGO_LOG(INFO) << "server.pushInterval illegal";
      return false;
    }
    push_crontab->interval = push_interval;
    push_crontab->func = Heartbeat::TriggerCoordinatorPushToStore;
    push_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(push_crontab);

    // Add calculate crontab
    std::shared_ptr<Crontab> calc_crontab = std::make_shared<Crontab>();
    calc_crontab->name = "CALCULATE";
    calc_crontab->interval = push_interval * 60;
    calc_crontab->func = Heartbeat::TriggerCalculateTableMetrics;
    calc_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(calc_crontab);
  }

  return true;
}

bool Server::InitStoreController() {
  store_controller_ = std::make_shared<StoreController>();
  return store_controller_->Init();
}

bool Server::InitRegionController() {
  region_controller_ = std::make_shared<RegionController>();
  return region_controller_->Init();
}

bool Server::InitRegionCommandManager() {
  region_command_manager_ = std::make_shared<RegionCommandManager>(std::make_shared<MetaReader>(raw_engine_),
                                                                   std::make_shared<MetaWriter>(raw_engine_));
  return region_command_manager_->Init();
}

bool Server::InitStoreMetricsManager() {
  store_metrics_manager_ = std::make_shared<StoreMetricsManager>(std::make_shared<MetaReader>(raw_engine_),
                                                                 std::make_shared<MetaWriter>(raw_engine_));
  return store_metrics_manager_->Init();
}

bool Server::Recover() {
  if (this->role_ == pb::common::STORE) {
    // Recover engine state.
    if (!engine_->Recover()) {
      DINGO_LOG(ERROR) << "Recover engine failed, engine " << engine_->GetName();
      return false;
    }
  }

  return true;
}

bool Server::InitHeartbeat() {
  heartbeat_ = std::make_shared<Heartbeat>();
  return heartbeat_->Init();
}

void Server::Destroy() {
  crontab_manager_->Destroy();
  heartbeat_->Destroy();
  region_controller_->Destroy();
  store_controller_->Destroy();

  google::ShutdownGoogleLogging();
}

}  // namespace dingodb
