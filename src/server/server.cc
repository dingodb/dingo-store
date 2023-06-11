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
#include <filesystem>
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
#include "glog/logging.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "scan/scan_manager.h"
#include "store/heartbeat.h"

namespace dingodb {

DEFINE_string(coor_url, "",
              "coor service name, e.g. file://<path>, list://<addr1>,<addr2>..., bns://<bns-name>, "
              "consul://<service-name>, http://<url>, https://<url>");

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

pb::node::LogLevel Server::GetDingoLogLevel(std::shared_ptr<dingodb::Config> config) {
  using dingodb::pb::node::LogLevel;
  LogLevel log_level = LogLevel::INFO;

  std::string const input_log_level = config->GetString("log.level");
  if (dingodb::Helper::IsEqualIgnoreCase(LogLevel_Name(LogLevel::DEBUG), input_log_level)) {
    log_level = LogLevel::DEBUG;
  } else if (dingodb::Helper::IsEqualIgnoreCase(LogLevel_Name(LogLevel::WARNING), input_log_level)) {
    log_level = LogLevel::WARNING;
  } else if (dingodb::Helper::IsEqualIgnoreCase(LogLevel_Name(LogLevel::ERROR), input_log_level)) {
    log_level = LogLevel::ERROR;
  } else if (dingodb::Helper::IsEqualIgnoreCase(LogLevel_Name(LogLevel::FATAL), input_log_level)) {
    log_level = LogLevel::FATAL;
  } else {
    log_level = LogLevel::INFO;
  }
  return log_level;
}

bool Server::InitLog() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  dingodb::pb::node::LogLevel const log_level = GetDingoLogLevel(config);

  FLAGS_log_dir = config->GetString("log.path");
  auto role_name = pb::common::ClusterRole_Name(role_);
  DingoLogger::InitLogger(FLAGS_log_dir, role_name, log_level);

  DINGO_LOG(INFO) << "log_dir: " << FLAGS_log_dir << " role:" << role_name
                  << " LogLevel:" << dingodb::pb::node::LogLevel_Name(log_level);

  return true;
}

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  id_ = config->GetInt("cluster.instance_id");
  keyring_ = config->GetString("cluster.keyring");
  return id_ != 0 && (!keyring_.empty());
}

bool Server::InitDirectory() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  std::filesystem::path db_path(config->GetString("store.path"));
  checkpoint_path_ = fmt::format("{}/checkpoint", db_path.parent_path().string());
  if (!std::filesystem::exists(checkpoint_path_)) {
    if (!std::filesystem::create_directories(checkpoint_path_)) {
      DINGO_LOG(ERROR) << "Create checkpoint directory failed: " << checkpoint_path_;
      return false;
    }
  }

  return true;
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
    engine_ = std::make_shared<RaftMetaEngine>(raw_engine_);

    // set raft_meta_engine to coordinator_control
    coordinator_control_->SetKvEngine(engine_);

    auto_increment_control_ = std::make_shared<AutoIncrementControl>();
    if (!auto_increment_control_->Recover()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Recover Failed";
      return false;
    }
    if (!auto_increment_control_->Init()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Init Failed";
      return false;
    }
  } else {
    engine_ = std::make_shared<RaftKvEngine>(raw_engine_);
    if (!engine_->Init(config)) {
      DINGO_LOG(ERROR) << "Init RaftKvEngine failed with Config[" << config->ToString() << "]";
      return false;
    }
  }

  return true;
}

butil::Status Server::StartMetaRegion(const std::shared_ptr<Config>& config,  // NOLINT
                                      std::shared_ptr<Engine>& kv_engine) {   // NOLINT
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kCoordinatorRegionId, Constant::kMetaRegionName, ctx);

  auto raft_engine = std::dynamic_pointer_cast<RaftMetaEngine>(kv_engine);
  return raft_engine->InitCoordinatorRegion(ctx, region, coordinator_control_, false);
}

butil::Status Server::StartAutoIncrementRegion(const std::shared_ptr<Config>& config,
                                               std::shared_ptr<Engine>& kv_engine) {
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kAutoIncrementRegionId, Constant::kAutoIncrementRegionName, ctx);

  auto raft_engine = std::dynamic_pointer_cast<RaftMetaEngine>(kv_engine);
  return raft_engine->InitCoordinatorRegion(ctx, region, auto_increment_control_, true);
}

bool Server::InitCoordinatorInteraction() {
  coordinator_interaction_ = std::make_shared<CoordinatorInteraction>();

  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  if (!FLAGS_coor_url.empty()) {
    return coordinator_interaction_->InitByNameService(FLAGS_coor_url,
                                                       pb::common::CoordinatorServiceType::ServiceTypeCoordinator);
  } else {
    DINGO_LOG(ERROR) << "FLAGS_coor_url is empty";
    return false;
  }
}

bool Server::InitCoordinatorInteractionForAutoIncrement() {
  coordinator_interaction_incr_ = std::make_shared<CoordinatorInteraction>();

  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  if (!FLAGS_coor_url.empty()) {
    return coordinator_interaction_incr_->InitByNameService(
        FLAGS_coor_url, pb::common::CoordinatorServiceType::ServiceTypeAutoIncrement);

  } else {
    return coordinator_interaction_incr_->Init(config->GetString("coordinator.peers"),
                                               pb::common::CoordinatorServiceType::ServiceTypeAutoIncrement);
  }
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
    uint64_t heartbeat_interval = config->GetInt("server.heartbeat_interval");
    if (heartbeat_interval < 0) {
      DINGO_LOG(ERROR) << "config server.heartbeat_interval illegal";
      return false;
    } else if (heartbeat_interval > 0) {
      std::shared_ptr<Crontab> heartbeat_crontab = std::make_shared<Crontab>();
      heartbeat_crontab->name = "HEARTBEA";
      heartbeat_crontab->interval = heartbeat_interval;
      heartbeat_crontab->func = [](void*) { Heartbeat::TriggerStoreHeartbeat(0); };
      heartbeat_crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(heartbeat_crontab);
    }

    // Add store metrics crontab
    uint64_t metrics_interval = config->GetInt("server.metrics_collect_interval");
    if (metrics_interval < 0) {
      DINGO_LOG(ERROR) << "config server.metrics_collect_interval illegal";
      return false;
    } else if (metrics_interval > 0) {
      std::shared_ptr<Crontab> metrics_crontab = std::make_shared<Crontab>();
      metrics_crontab->name = "METRICS";
      metrics_crontab->interval = metrics_interval;
      metrics_crontab->func = [](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void*) -> void* {
              Server::GetInstance()->GetStoreMetricsManager()->CollectMetrics();
              return nullptr;
            },
            nullptr);
      };
      metrics_crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(metrics_crontab);
    }

    // Add scan crontab
    ScanManager::GetInstance()->Init(config);
    uint64_t scan_interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
    if (scan_interval < 0) {
      DINGO_LOG(ERROR) << "store.scan.scan_interval_ms illegal";
      return false;
    } else if (scan_interval > 0) {
      std::shared_ptr<Crontab> scan_crontab = std::make_shared<Crontab>();
      scan_crontab->name = "SCAN";
      scan_crontab->interval = scan_interval;
      scan_crontab->func = ScanManager::RegularCleaningHandler;
      scan_crontab->arg = ScanManager::GetInstance();

      crontab_manager_->AddAndRunCrontab(scan_crontab);
    }

  } else if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // Add push crontab
    std::shared_ptr<Crontab> push_crontab = std::make_shared<Crontab>();
    push_crontab->name = "PUSH";
    uint64_t push_interval = config->GetInt("server.push_interval");
    if (push_interval <= 0) {
      DINGO_LOG(INFO) << "server.push_interval illegal";
      return false;
    }
    push_crontab->interval = push_interval;
    push_crontab->func = Heartbeat::TriggerCoordinatorPushToStore;
    push_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(push_crontab);

    // Add update state crontab
    std::shared_ptr<Crontab> update_crontab = std::make_shared<Crontab>();
    update_crontab->name = "UPDATE";
    update_crontab->interval = push_interval * 10;
    update_crontab->func = Heartbeat::TriggerCoordinatorUpdateState;
    update_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(update_crontab);

    // Add task list process crontab
    std::shared_ptr<Crontab> tasklist_crontab = std::make_shared<Crontab>();
    tasklist_crontab->name = "TASKLIST";
    tasklist_crontab->interval = push_interval;
    tasklist_crontab->func = Heartbeat::TriggerCoordinatorTaskListProcess;
    tasklist_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(tasklist_crontab);

    // Add calculate crontab
    std::shared_ptr<Crontab> calc_crontab = std::make_shared<Crontab>();
    calc_crontab->name = "CALCULATE";
    calc_crontab->interval = push_interval * 60;
    calc_crontab->func = Heartbeat::TriggerCalculateTableMetrics;
    calc_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(calc_crontab);

    // Add recycle orphan crontab
    std::shared_ptr<Crontab> recycle_crontab = std::make_shared<Crontab>();
    recycle_crontab->name = "RECYCLE";
    recycle_crontab->interval = push_interval * 60;
    recycle_crontab->func = Heartbeat::TriggerCoordinatorRecycleOrphan;
    recycle_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(recycle_crontab);
  } else if (role_ == pb::common::ClusterRole::INDEX) {
    // Add heartbeat crontab
    uint64_t heartbeat_interval = config->GetInt("server.heartbeat_interval");
    if (heartbeat_interval < 0) {
      DINGO_LOG(ERROR) << "config server.heartbeat_interval illegal";
      return false;
    } else if (heartbeat_interval > 0) {
      std::shared_ptr<Crontab> heartbeat_crontab = std::make_shared<Crontab>();
      heartbeat_crontab->name = "HEARTBEA";
      heartbeat_crontab->interval = heartbeat_interval;
      heartbeat_crontab->func = [](void*) { Heartbeat::TriggerStoreHeartbeat(0); };
      heartbeat_crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(heartbeat_crontab);
    }

    // Add store metrics crontab
    uint64_t metrics_interval = config->GetInt("server.metrics_collect_interval");
    if (metrics_interval < 0) {
      DINGO_LOG(ERROR) << "config server.metrics_collect_interval illegal";
      return false;
    } else if (metrics_interval > 0) {
      std::shared_ptr<Crontab> metrics_crontab = std::make_shared<Crontab>();
      metrics_crontab->name = "METRICS";
      metrics_crontab->interval = metrics_interval;
      metrics_crontab->func = [](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void*) -> void* {
              Server::GetInstance()->GetStoreMetricsManager()->CollectMetrics();
              return nullptr;
            },
            nullptr);
      };
      metrics_crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(metrics_crontab);
    }

    // // Add scan crontab
    // ScanManager::GetInstance()->Init(config);
    // uint64_t scan_interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
    // if (scan_interval < 0) {
    //   DINGO_LOG(ERROR) << "store.scan.scan_interval_ms illegal";
    //   return false;
    // } else if (scan_interval > 0) {
    //   std::shared_ptr<Crontab> scan_crontab = std::make_shared<Crontab>();
    //   scan_crontab->name = "SCAN";
    //   scan_crontab->interval = scan_interval;
    //   scan_crontab->func = ScanManager::RegularCleaningHandler;
    //   scan_crontab->arg = ScanManager::GetInstance();

    //   crontab_manager_->AddAndRunCrontab(scan_crontab);
    // }
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
  store_metrics_manager_ = std::make_shared<StoreMetricsManager>(raw_engine_, std::make_shared<MetaReader>(raw_engine_),
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

    if (!region_controller_->Recover()) {
      DINGO_LOG(ERROR) << "Recover region controller failed";
      return false;
    }
  }

  return true;
}

bool Server::InitHeartbeat() { return heartbeat_->Init(); }

void Server::Destroy() {
  crontab_manager_->Destroy();
  heartbeat_->Destroy();
  region_controller_->Destroy();
  store_controller_->Destroy();

  google::ShutdownGoogleLogging();
}

std::shared_ptr<pb::common::RegionDefinition> Server::CreateCoordinatorRegion(const std::shared_ptr<Config>& /*config*/,
                                                                              const uint64_t region_id,
                                                                              const std::string& region_name,
                                                                              std::shared_ptr<Context>& ctx) {
  /**
   * 1. context must contains role)
   */
  // ctx->SetClusterRole(pb::common::COORDINATOR);

  /*
   * 2. construct region list
   *    1) Region ID
   *    2) Region PeerList
   */
  std::shared_ptr<pb::common::RegionDefinition> region = std::make_shared<pb::common::RegionDefinition>();
  region->set_id(region_id);
  region->set_table_id(Constant::kCoordinatorTableId);
  region->set_schema_id(Constant::kCoordinatorSchemaId);
  region->set_name(region_name);

  for (auto& endpoint : endpoints_) {
    auto* peer = region->add_peers();
    auto* location = peer->mutable_raft_location();
    location->set_host(butil::ip2str(endpoint.ip).c_str());
    location->set_port(endpoint.port);
    DINGO_LOG(INFO) << region_name << " set peer node:" << (butil::ip2str(endpoint.ip).c_str()) << ":" << endpoint.port;
  }

  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000");
  range->set_end_key("FFFF");

  DINGO_LOG(INFO) << region_name << " Create Region Request:" << region->DebugString();
  return region;
}

}  // namespace dingodb
