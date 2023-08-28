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
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "coordinator/coordinator_control.h"
#include "engine/engine.h"
#include "engine/mem_engine.h"
#include "engine/raft_store_engine.h"
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
#include "store/region_controller.h"

DEFINE_string(coor_url, "",
              "coor service name, e.g. file://<path>, list://<addr1>,<addr2>..., bns://<bns-name>, "
              "consul://<service-name>, http://<url>, https://<url>");

namespace dingodb {

DECLARE_uint32(compaction_retention_rev_count);
DECLARE_bool(auto_compaction);

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

  DingoLogVerion();

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

  // cooridnator
  if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // 1.init CoordinatorController
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
    engine_ = std::make_shared<RaftStoreEngine>(raw_engine_);

    // set raft_meta_engine to coordinator_control
    coordinator_control_->SetKvEngine(engine_);

    // 2.init AutoIncrementController
    auto_increment_control_ = std::make_shared<AutoIncrementControl>();
    if (!auto_increment_control_->Recover()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Recover Failed";
      return false;
    }
    if (!auto_increment_control_->Init()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Init Failed";
      return false;
    }

    // 3.init TsoController
    tso_control_ = std::make_shared<TsoControl>();
    if (!tso_control_->Recover()) {
      DINGO_LOG(ERROR) << "tso_control_->Recover Failed";
      return false;
    }
    if (!tso_control_->Init()) {
      DINGO_LOG(ERROR) << "tso_control_->Init Failed";
      return false;
    }

    // set raft_meta_engine to tso_control
    tso_control_->SetKvEngine(engine_);

  } else {
    engine_ = std::make_shared<RaftStoreEngine>(raw_engine_);
    if (!engine_->Init(config)) {
      DINGO_LOG(ERROR) << "Init RaftStoreEngine failed with Config[" << config->ToString() << "]";
      return false;
    }
  }

  return true;
}

butil::Status Server::StartMetaRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kCoordinatorRegionId, Constant::kMetaRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, coordinator_control_, false);
}

butil::Status Server::StartAutoIncrementRegion(const std::shared_ptr<Config>& config,
                                               std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kAutoIncrementRegionId, Constant::kAutoIncrementRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, auto_increment_control_, true);
}

butil::Status Server::StartTsoRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kTsoRegionId, Constant::kTsoRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, tso_control_, true);
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

bool Server::InitLogStorageManager() {
  log_storage_ = std::make_shared<LogStorageManager>();
  return true;
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
    uint64_t heartbeat_interval_s = config->GetInt("server.heartbeat_interval_s");
    if (heartbeat_interval_s < 0) {
      DINGO_LOG(ERROR) << "config server.heartbeat_interval_s illegal";
      return false;
    } else if (heartbeat_interval_s == 0) {
      DINGO_LOG(WARNING) << "config server.heartbeat_interval_s is 0, heartbeat will not be triggered";
    } else if (heartbeat_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "HEARTBEA";
      crontab->interval = heartbeat_interval_s * 1000;
      crontab->func = [](void*) { Heartbeat::TriggerStoreHeartbeat(0); };
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add store metrics crontab
    int metrics_collect_interval_s = config->GetInt("server.metrics_collect_interval_s");
    metrics_collect_interval_s =
        metrics_collect_interval_s > 0 ? metrics_collect_interval_s : Constant::kMetricsCollectIntervalS;
    if (metrics_collect_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "METRICS";
      crontab->interval = metrics_collect_interval_s * 1000;
      crontab->func = [](void*) {
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
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add store approximate size metrics crontab
    int approimate_size_metrics_collect_interval_s =
        config->GetInt("server.approximate_size_metrics_collect_interval_s");
    approimate_size_metrics_collect_interval_s = approimate_size_metrics_collect_interval_s > 0
                                                     ? approimate_size_metrics_collect_interval_s
                                                     : Constant::kApproximateSizeMetricsCollectIntervalS;
    if (approimate_size_metrics_collect_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "APPROXIMATE_SIZE_METRICS";
      crontab->interval = approimate_size_metrics_collect_interval_s * 1000;
      crontab->func = [](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void*) -> void* {
              Server::GetInstance()->GetStoreMetricsManager()->CollectApproximateSizeMetrics();
              return nullptr;
            },
            nullptr);
      };
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add scan crontab
    ScanManager::GetInstance()->Init(config);
    uint64_t scan_interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
    if (scan_interval < 0) {
      DINGO_LOG(ERROR) << "store.scan.scan_interval_ms illegal";
      return false;
    } else if (scan_interval == 0) {
      DINGO_LOG(WARNING) << "store.scan.scan_interval_ms is 0, scan will not be triggered";
    } else if (scan_interval > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "SCAN";
      crontab->interval = scan_interval;
      crontab->func = ScanManager::RegularCleaningHandler;
      crontab->arg = ScanManager::GetInstance();

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add split checker crontab
    bool enable_auto_split = config->GetBool("region.enable_auto_split");
    if (enable_auto_split) {
      int split_check_interval_s = config->GetInt("region.split_check_interval_s");
      split_check_interval_s =
          split_check_interval_s > 0 ? split_check_interval_s : Constant::kDefaultStoreSplitCheckIntervalS;
      if (split_check_interval_s < 0) {
        DINGO_LOG(ERROR) << "config region.split_check_interval_s illegal";
        return false;
      } else if (split_check_interval_s == 0) {
        DINGO_LOG(WARNING) << "config region.split_check_interval_s is 0, split checker will not be triggered";
      } else if (split_check_interval_s > 0) {
        std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
        crontab->name = "SPLIT_CHECKER";
        crontab->interval = split_check_interval_s * 1000;
        crontab->func = [](void*) { PreSplitChecker::TriggerPreSplitCheck(nullptr); };
        crontab->arg = nullptr;

        crontab_manager_->AddAndRunCrontab(crontab);
      }
    }

  } else if (role_ == pb::common::ClusterRole::COORDINATOR) {
    // Add push crontab
    std::shared_ptr<Crontab> push_crontab = std::make_shared<Crontab>();
    push_crontab->name = "PUSH";
    uint64_t push_interval_s = config->GetInt("coordinator.push_interval_s");
    if (push_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.push_interval_s illegal";
      return false;
    }
    push_crontab->interval = push_interval_s * 1000;
    push_crontab->func = Heartbeat::TriggerCoordinatorPushToStore;
    push_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(push_crontab);

    // Add update state crontab
    std::shared_ptr<Crontab> update_crontab = std::make_shared<Crontab>();
    update_crontab->name = "UPDATE";
    uint64_t update_state_interval_s = config->GetInt("coordinator.update_state_interval_s");
    if (update_state_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.update_state_interval_s illegal";
      return false;
    }
    update_crontab->interval = update_state_interval_s * 1000;
    update_crontab->func = Heartbeat::TriggerCoordinatorUpdateState;
    update_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(update_crontab);

    // Add task list process crontab
    std::shared_ptr<Crontab> tasklist_crontab = std::make_shared<Crontab>();
    tasklist_crontab->name = "TASKLIST";
    uint64_t task_list_interval_s = config->GetInt("coordinator.task_list_interval_s");
    if (task_list_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.task_list_interval_s illegal";
      return false;
    }
    tasklist_crontab->interval = task_list_interval_s * 1000;
    tasklist_crontab->func = Heartbeat::TriggerCoordinatorTaskListProcess;
    tasklist_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(tasklist_crontab);

    // Add calculate crontab
    std::shared_ptr<Crontab> calc_crontab = std::make_shared<Crontab>();
    calc_crontab->name = "CALCULATE";
    uint64_t calc_metrics_interval_s = config->GetInt("coordinator.calc_metrics_interval_s");
    if (calc_metrics_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.calc_metrics_interval_s illegal";
      return false;
    }
    calc_crontab->interval = calc_metrics_interval_s * 1000;
    calc_crontab->func = Heartbeat::TriggerCalculateTableMetrics;
    calc_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(calc_crontab);

    // Add recycle orphan crontab
    std::shared_ptr<Crontab> recycle_crontab = std::make_shared<Crontab>();
    uint64_t recycle_orphan_interval_s = config->GetInt("coordinator.recycle_orphan_interval_s");
    if (recycle_orphan_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.recycle_orphan_interval_s illegal";
      return false;
    }
    recycle_crontab->name = "RECYCLE";
    recycle_crontab->interval = recycle_orphan_interval_s * 1000;
    recycle_crontab->func = Heartbeat::TriggerCoordinatorRecycleOrphan;
    recycle_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(recycle_crontab);

    // Add lease crontab
    std::shared_ptr<Crontab> lease_crontab = std::make_shared<Crontab>();
    lease_crontab->name = "LEASE";
    uint64_t lease_interval_s = config->GetInt("coordinator.lease_interval_s");
    if (lease_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.lease_interval_s illegal";
      return false;
    }
    lease_crontab->interval = lease_interval_s * 1000;
    lease_crontab->func = Heartbeat::TriggerLeaseTask;
    lease_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(lease_crontab);

    // Add compaction crontab
    std::shared_ptr<Crontab> compaction_crontab = std::make_shared<Crontab>();
    compaction_crontab->name = "compaction";
    FLAGS_auto_compaction = config->GetBool("coordinator.auto_compaction");
    DINGO_LOG(INFO) << "coordinator.auto_compaction:" << FLAGS_auto_compaction;

    uint64_t compaction_interval_s = config->GetInt("coordinator.compaction_interval_s");
    if (compaction_interval_s <= 0) {
      DINGO_LOG(INFO) << "coordinator.compaction_interval_s illegal";
      return false;
    }
    uint32_t compaction_retention_rev_count = config->GetInt("coordinator.compaction_retention_rev_count");
    if (compaction_retention_rev_count <= 0) {
      DINGO_LOG(INFO) << "coordinator.compaction_retention_rev_count illegal, use default value :"
                      << FLAGS_compaction_retention_rev_count;
    } else {
      FLAGS_compaction_retention_rev_count = compaction_retention_rev_count;
    }

    compaction_crontab->interval = compaction_interval_s * 1000;
    compaction_crontab->func = Heartbeat::TriggerCompactionTask;
    compaction_crontab->arg = nullptr;

    crontab_manager_->AddAndRunCrontab(compaction_crontab);

  } else if (role_ == pb::common::ClusterRole::INDEX) {
    // Add heartbeat crontab
    uint64_t heartbeat_interval_s = config->GetInt("server.heartbeat_interval_s");
    if (heartbeat_interval_s < 0) {
      DINGO_LOG(ERROR) << "config server.heartbeat_interval_s illegal";
      return false;
    } else if (heartbeat_interval_s == 0) {
      DINGO_LOG(WARNING) << "config server.heartbeat_interval_s is 0, heartbeat will not be triggered";
    } else if (heartbeat_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "HEARTBEA";
      crontab->interval = heartbeat_interval_s * 1000;
      crontab->func = [](void*) { Heartbeat::TriggerStoreHeartbeat(0); };
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add store metrics crontab
    int metrics_collect_interval_s = config->GetInt("server.metrics_collect_interval_s");
    metrics_collect_interval_s =
        metrics_collect_interval_s > 0 ? metrics_collect_interval_s : Constant::kMetricsCollectIntervalS;
    if (metrics_collect_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "METRICS";
      crontab->interval = metrics_collect_interval_s * 1000;
      crontab->func = [](void*) {
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
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add store approximate size metrics crontab
    int approimate_size_metrics_collect_interval_s =
        config->GetInt("server.approximate_size_metrics_collect_interval_s");
    approimate_size_metrics_collect_interval_s = approimate_size_metrics_collect_interval_s > 0
                                                     ? approimate_size_metrics_collect_interval_s
                                                     : Constant::kApproximateSizeMetricsCollectIntervalS;
    if (approimate_size_metrics_collect_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "APPROXIMATE_SIZE_METRICS";
      crontab->interval = approimate_size_metrics_collect_interval_s * 1000;
      crontab->func = [](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void*) -> void* {
              Server::GetInstance()->GetStoreMetricsManager()->CollectApproximateSizeMetrics();
              return nullptr;
            },
            nullptr);
      };
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add scrub vector index crontab
    uint64_t scrub_vector_index_interval_s = config->GetInt("server.scrub_vector_index_interval_s");
    if (scrub_vector_index_interval_s < 0) {
      DINGO_LOG(ERROR) << "config server.scrub_vector_index_interval_s illegal";
      return false;
    } else if (scrub_vector_index_interval_s == 0) {
      DINGO_LOG(WARNING)
          << "config server.scrub_vector_index_interval_s is 0, scrub vector index will not be triggered";
    } else if (scrub_vector_index_interval_s > 0) {
      std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
      crontab->name = "SCRUB_VECTOR_INDEX";
      crontab->interval = scrub_vector_index_interval_s * 1000;
      crontab->func = [](void*) { Heartbeat::TriggerScrubVectorIndex(nullptr); };
      crontab->arg = nullptr;

      crontab_manager_->AddAndRunCrontab(crontab);
    }

    // Add split checker crontab
    bool enable_auto_split = config->GetBool("region.enable_auto_split");
    if (enable_auto_split) {
      int split_check_interval_s = config->GetInt("region.split_check_interval_s");
      split_check_interval_s =
          split_check_interval_s > 0 ? split_check_interval_s : Constant::kDefaultIndexSplitCheckIntervalS;
      if (split_check_interval_s < 0) {
        DINGO_LOG(ERROR) << "config region.split_check_interval_s illegal";
        return false;
      } else if (split_check_interval_s == 0) {
        DINGO_LOG(WARNING) << "config region.split_check_interval_s is 0, split checker will not be triggered";
      } else if (split_check_interval_s > 0) {
        std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
        crontab->name = "SPLIT_CHECKER";
        crontab->interval = split_check_interval_s * 1000;
        crontab->func = [](void*) { PreSplitChecker::TriggerPreSplitCheck(nullptr); };
        crontab->arg = nullptr;

        crontab_manager_->AddAndRunCrontab(crontab);
      }
    }
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
                                                                 std::make_shared<MetaWriter>(raw_engine_), engine_);
  return store_metrics_manager_->Init();
}

bool Server::InitVectorIndexManager() {
  vector_index_manager_ = std::make_shared<VectorIndexManager>(raw_engine_, std::make_shared<MetaReader>(raw_engine_),
                                                               std::make_shared<MetaWriter>(raw_engine_));

  return vector_index_manager_->Init(store_meta_manager_->GetStoreRegionMeta()->GetAllAliveRegion());
}

bool Server::InitPreSplitChecker() {
  pre_split_checker_ = std::make_shared<PreSplitChecker>();
  auto config = GetConfig();
  int split_check_concurrency = config->GetInt("region.split_check_concurrency");
  split_check_concurrency =
      split_check_concurrency > 0 ? split_check_concurrency : Constant::kDefaultSplitCheckConcurrency;
  return pre_split_checker_->Init(split_check_concurrency);
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
  } else if (this->role_ == pb::common::INDEX) {
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
                                                                              const std::string& region_name
                                                                              /*std::shared_ptr<Context>& ctx*/) {
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
