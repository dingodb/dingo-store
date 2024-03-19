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

#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>

#include "butil/endpoint.h"
#include "butil/time.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "config/yaml_config.h"
#include "coordinator/coordinator_control.h"
#include "engine/bdb_raw_engine.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "engine/rocks_raw_engine.h"
#include "fmt/core.h"
#ifdef ENABLE_XDPROCKS
#include "engine/xdprocks_raw_engine.h"
#endif
#include "engine/txn_engine_helper.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/node.pb.h"
#include "scan/scan_manager.h"
#include "store/heartbeat.h"
#include "store/region_controller.h"

DEFINE_string(coor_url, "",
              "coor service name, e.g. file://<path>, list://<addr1>,<addr2>..., bns://<bns-name>, "
              "consul://<service-name>, http://<url>, https://<url>");

namespace dingodb {

DECLARE_int64(compaction_retention_rev_count);
DECLARE_bool(auto_compaction);

DEFINE_bool(ip2hostname, false, "resolve ip to hostname for get map api");
DEFINE_bool(enable_ip2hostname_cache, true, "enable ip2hostname cache");
DEFINE_int64(ip2hostname_cache_seconds, 300, "ip2hostname cache seconds");

DEFINE_int32(vector_operation_parallel_thread_num, 1, "vector operation parallel thread num");
DEFINE_string(pid_file_name, "pid", "pid file name");

DEFINE_int32(omp_num_threads, 1, "omp num threads");

extern "C" {
extern void omp_set_num_threads(int) noexcept;  // NOLINT
extern int omp_get_max_threads(void) noexcept;  // NOLINT
}

Server& Server::GetInstance() {
  static Server instance;
  return instance;
}

bool Server::InitConfig(const std::string& filename) {
  std::shared_ptr<Config> const config = std::make_shared<YamlConfig>();
  if (config->LoadFile(filename) != 0) {
    return false;
  }

  ConfigManager::GetInstance().Register(GetRoleName(), config);

  // init ip2hostname_cache
  ip2hostname_cache_.Init(256);

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
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  dingodb::pb::node::LogLevel const log_level = GetDingoLogLevel(config);

  if (!Helper::IsExistPath(log_dir_)) {
    DINGO_LOG(ERROR) << "log_dir: " << log_dir_ << " not exist";
    return false;
  }

  FLAGS_log_dir = log_dir_;
  auto role_name = GetRoleName();
  DingoLogger::InitLogger(FLAGS_log_dir, GetRoleName(), log_level);

  DINGO_LOG(INFO) << "log_dir: " << FLAGS_log_dir << " role:" << role_name
                  << " LogLevel:" << dingodb::pb::node::LogLevel_Name(log_level);

  DingoLogVerion();

  return true;
}

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  id_ = config->GetInt("cluster.instance_id");
  keyring_ = config->GetString("cluster.keyring");
  return id_ != 0 && (!keyring_.empty());
}

bool Server::InitDirectory() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  // db path
  auto db_path = config->GetString("store.path");
  auto ret = Helper::CreateDirectories(db_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create data directory failed: " << db_path;
    return false;
  }

  // checkpoint path
  checkpoint_path_ = fmt::format("{}/rocksdb_checkpoint", db_path);
  ret = Helper::CreateDirectories(checkpoint_path_);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create checkpoint directory failed: " << checkpoint_path_;
    return false;
  }

  // raft data path
  auto raft_data_path = config->GetString("raft.path");
  ret = Helper::CreateDirectories(raft_data_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create raft directory failed: " << raft_data_path;
    return false;
  }

  // raft log path
  auto raft_log_path = config->GetString("raft.log_path");
  ret = Helper::CreateDirectories(raft_log_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create raft log directory failed: " << raft_log_path;
    return false;
  }

  // vector index path
  if (GetRole() == pb::common::INDEX) {
    auto vector_index_path = config->GetString("vector.index_path");
    ret = Helper::CreateDirectories(vector_index_path);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "Create vector index directory failed: " << vector_index_path;
      return false;
    }
  }

  // program log path
  log_dir_ = config->GetString("log.path");
  ret = Helper::CreateDirectories(log_dir_);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create log directory failed: " << log_dir_;
    return false;
  }

  return true;
}

bool Server::InitEngine() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();

#ifdef ENABLE_XDPROCKS
  // init xdprocks
  auto raw_rocks_engine = std::make_shared<XDPRocksRawEngine>();
  if (!raw_rocks_engine->Init(config, Helper::GetColumnFamilyNamesByRole())) {
    DINGO_LOG(ERROR) << "Init XDPRocksRawEngine Failed with Config[" << config->ToString();
    return false;
  }
#else
  // init rocksdb
  auto raw_rocks_engine = std::make_shared<RocksRawEngine>();
  if (!raw_rocks_engine->Init(config, Helper::GetColumnFamilyNamesByRole())) {
    DINGO_LOG(ERROR) << "Init RocksRawEngine Failed with Config[" << config->ToString();
    return false;
  }
#endif

  meta_reader_ = std::make_shared<MetaReader>(raw_rocks_engine);
  meta_writer_ = std::make_shared<MetaWriter>(raw_rocks_engine);

  // init bdb
  auto raw_bdb_engine = std::make_shared<BdbRawEngine>();
  if (!raw_bdb_engine->Init(config, Helper::GetColumnFamilyNamesByRole())) {
    DINGO_LOG(ERROR) << "Init BdbRawEngine Failed with Config[" << config->ToString();
    return false;
  }

  // cooridnator
  if (GetRole() == pb::common::ClusterRole::COORDINATOR) {
    // 1.init CoordinatorController
    coordinator_control_ =
        std::make_shared<CoordinatorControl>(std::make_shared<MetaReader>(raw_rocks_engine),
                                             std::make_shared<MetaWriter>(raw_rocks_engine), raw_rocks_engine);

    if (!coordinator_control_->Recover()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Recover Failed";
      return false;
    }
    if (!coordinator_control_->Init()) {
      DINGO_LOG(ERROR) << "coordinator_control_->Init Failed";
      return false;
    }

    // init raft_meta_engine
    raft_engine_ = std::make_shared<RaftStoreEngine>(raw_rocks_engine, raw_bdb_engine);

    // set raft_meta_engine to coordinator_control
    coordinator_control_->SetKvEngine(raft_engine_);

    // 2.init KvController
    kv_control_ = std::make_shared<KvControl>(std::make_shared<MetaReader>(raw_rocks_engine),
                                              std::make_shared<MetaWriter>(raw_rocks_engine), raw_rocks_engine);

    if (!kv_control_->Recover()) {
      DINGO_LOG(ERROR) << "kv_control_->Recover Failed";
      return false;
    }
    if (!kv_control_->Init()) {
      DINGO_LOG(ERROR) << "kv_control_->Init Failed";
      return false;
    }

    // set raft_meta_engine to coordinator_control
    kv_control_->SetKvEngine(raft_engine_);

    // 3.init AutoIncrementController
    auto_increment_control_ = std::make_shared<AutoIncrementControl>();
    if (!auto_increment_control_->Recover()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Recover Failed";
      return false;
    }
    if (!auto_increment_control_->Init()) {
      DINGO_LOG(ERROR) << "auto_increment_control_->Init Failed";
      return false;
    }

    // set raft_meta_engine to auto_increment_control
    auto_increment_control_->SetKvEngine(raft_engine_);

    // 4.init TsoController
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
    tso_control_->SetKvEngine(raft_engine_);

  } else {
    raft_engine_ = std::make_shared<RaftStoreEngine>(raw_rocks_engine, raw_bdb_engine);
    if (!raft_engine_->Init(config)) {
      DINGO_LOG(ERROR) << "Init RaftStoreEngine failed with Config[" << config->ToString() << "]";
      return false;
    }
  }

  return true;
}

butil::Status Server::StartMetaRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kMetaRegionId, Constant::kMetaRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, coordinator_control_, false);
}

butil::Status Server::StartKvRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kKvRegionId, Constant::kKvRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, kv_control_, false);
}

butil::Status Server::StartTsoRegion(const std::shared_ptr<Config>& config, std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kTsoRegionId, Constant::kTsoRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, tso_control_, true);
}

butil::Status Server::StartAutoIncrementRegion(const std::shared_ptr<Config>& config,
                                               std::shared_ptr<Engine>& kv_engine) {
  // std::shared_ptr<Context> ctx = std::make_shared<Context>();
  std::shared_ptr<pb::common::RegionDefinition> region =
      CreateCoordinatorRegion(config, Constant::kAutoIncrementRegionId, Constant::kAutoIncrementRegionName /*, ctx*/);

  auto raft_engine = std::dynamic_pointer_cast<RaftStoreEngine>(kv_engine);
  return raft_engine->AddNode(region, auto_increment_control_, true);
}

bool Server::InitCoordinatorInteraction() {
  coordinator_interaction_ = std::make_shared<CoordinatorInteraction>();

  auto config = ConfigManager::GetInstance().GetRoleConfig();

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

  auto config = ConfigManager::GetInstance().GetRoleConfig();

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
  storage_ = std::make_shared<Storage>(raft_engine_);
  return true;
}

bool Server::InitStoreMetaManager() {
  store_meta_manager_ = std::make_shared<StoreMetaManager>(meta_reader_, meta_writer_);
  return store_meta_manager_->Init();
}

static int32_t GetInterval(std::shared_ptr<Config> config, const std::string& config_name,  // NOLINT
                           int32_t default_value) {                                         // NOLINT
  int32_t interval_s = config->GetInt(config_name);
  return interval_s > 0 ? interval_s : default_value;
}

DEFINE_int32(server_heartbeat_interval_s, 10, "heartbeat interval seconds");
DEFINE_int32(server_metrics_collect_interval_s, 300, "metrics collect interval seconds");
DEFINE_int32(server_store_metrics_collect_interval_s, 30, "store metrics collect interval seconds");
DEFINE_int32(server_approximate_size_metrics_collect_interval_s, 300,
             "approximate size metrics collect interval seconds");
DEFINE_int32(scan_scan_interval_s, 30, "scan interval seconds");
DEFINE_int32(scanv2_scan_interval_s, 30, "scan interval seconds");
DEFINE_bool(region_enable_auto_split, true, "enable auto split");
DEFINE_int32(region_split_check_interval_s, 300, "split check interval seconds");
DEFINE_int32(coordinator_push_interval_s, 1, "coordinator push interval seconds");
DEFINE_int32(coordinator_update_state_interval_s, 10, "coordinator update state interval seconds");
DEFINE_int32(coordinator_task_list_interval_s, 1, "coordinator task list interval seconds");
DEFINE_int32(coordinator_calc_metrics_interval_s, 60, "coordinator calc metrics interval seconds");
DEFINE_int32(coordinator_recycle_orphan_interval_s, 60, "coordinator recycle orphan interval seconds");
DEFINE_int32(coordinator_meta_watch_clean_interval_s, 60, "coordinator meta watch clean interval seconds");
DEFINE_int32(coordinator_remove_watch_interval_s, 10, "coordinator remove watch interval seconds");
DEFINE_int32(coordinator_lease_interval_s, 1, "coordinator lease interval seconds");
DEFINE_int32(coordinator_compaction_interval_s, 300, "coordinator compaction interval seconds");
DEFINE_int32(server_scrub_vector_index_interval_s, 60, "scrub vector index interval seconds");
DEFINE_int32(raft_snapshot_interval_s, 120, "raft snapshot interval seconds");
DEFINE_int32(gc_update_safe_point_interval_s, 60, "gc update safe point interval seconds");
DEFINE_int32(gc_do_gc_interval_s, 60, "gc do gc interval seconds");

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  // Add heartbeat crontab
  FLAGS_server_heartbeat_interval_s =
      GetInterval(config, "server.heartbeat_interval_s", FLAGS_server_heartbeat_interval_s);
  crontab_configs_.push_back({
      "HEARTBEA",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_server_heartbeat_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerStoreHeartbeat({}, true); },
  });

  // Add store region metrics crontab
  FLAGS_server_metrics_collect_interval_s =
      GetInterval(config, "server.metrics_collect_interval_s", FLAGS_server_metrics_collect_interval_s);
  crontab_configs_.push_back({
      "STORE_REGION_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_server_metrics_collect_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectStoreRegionMetrics(); },
  });

  // Add store metrics crontab
  FLAGS_server_store_metrics_collect_interval_s =
      GetInterval(config, "server.store_metrics_collect_interval_s", FLAGS_server_store_metrics_collect_interval_s);
  crontab_configs_.push_back({
      "STORE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_server_store_metrics_collect_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectStoreMetrics(); },
  });

  // Add store approximate size metrics crontab
  FLAGS_server_approximate_size_metrics_collect_interval_s =
      GetInterval(config, "server.approximate_size_metrics_collect_interval_s",
                  FLAGS_server_approximate_size_metrics_collect_interval_s);
  crontab_configs_.push_back({
      "APPROXIMATE_SIZE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_server_approximate_size_metrics_collect_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectApproximateSizeMetrics(); },
  });

  // Add scan crontab
  if (GetRole() == pb::common::STORE) {
    ScanManager::GetInstance().Init(config);

    FLAGS_scan_scan_interval_s = GetInterval(config, "scan.scan_interval_s", FLAGS_scan_scan_interval_s);
    crontab_configs_.push_back({
        "SCAN",
        {pb::common::STORE},
        FLAGS_scan_scan_interval_s * 1000,
        true,
        [](void*) { ScanManager::RegularCleaningHandler(nullptr); },
    });
  }

  // Add scan v2 crontab
  if (GetRole() == pb::common::STORE) {
    ScanManagerV2::GetInstance().Init(config);

    FLAGS_scanv2_scan_interval_s = GetInterval(config, "scan_v2.scan_interval_s", FLAGS_scanv2_scan_interval_s);
    crontab_configs_.push_back({
        "SCAN_V2",
        {pb::common::STORE},
        FLAGS_scanv2_scan_interval_s * 1000,
        true,
        [](void*) { ScanManagerV2::RegularCleaningHandler(nullptr); },
    });
  }

  // Add split checker crontab
  if (GetRole() == pb::common::STORE || GetRole() == pb::common::INDEX) {
    FLAGS_region_enable_auto_split = config->GetBool("region.enable_auto_split");
    if (FLAGS_region_enable_auto_split) {
      FLAGS_region_split_check_interval_s =
          GetInterval(config, "region.split_check_interval_s", FLAGS_region_split_check_interval_s);
      crontab_configs_.push_back({
          "SPLIT_CHECKER",
          {pb::common::STORE, pb::common::INDEX},
          FLAGS_region_split_check_interval_s * 1000,
          true,
          [](void*) { PreSplitChecker::TriggerPreSplitCheck(nullptr); },
      });
    }
  }

  // Add push crontab
  FLAGS_coordinator_push_interval_s =
      GetInterval(config, "coordinator.push_interval_s", FLAGS_coordinator_push_interval_s);
  crontab_configs_.push_back({
      "PUSH",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_push_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCoordinatorPushToStore(nullptr); },
  });

  // Add update state crontab
  FLAGS_coordinator_update_state_interval_s =
      GetInterval(config, "coordinator.update_state_interval_s", FLAGS_coordinator_update_state_interval_s);
  crontab_configs_.push_back({
      "UPDATE",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_update_state_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCoordinatorUpdateState(nullptr); },
  });

  // Add task list process crontab
  FLAGS_coordinator_task_list_interval_s =
      GetInterval(config, "coordinator.task_list_interval_s", FLAGS_coordinator_task_list_interval_s);
  crontab_configs_.push_back({
      "TASKLIST",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_task_list_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCoordinatorTaskListProcess(nullptr); },
  });

  // Add calculate crontab
  FLAGS_coordinator_calc_metrics_interval_s =
      GetInterval(config, "coordinator.calc_metrics_interval_s", FLAGS_coordinator_calc_metrics_interval_s);
  crontab_configs_.push_back({
      "CALCULATE",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_calc_metrics_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCalculateTableMetrics(nullptr); },
  });

  // Add recycle orphan crontab
  FLAGS_coordinator_recycle_orphan_interval_s =
      GetInterval(config, "coordinator.recycle_orphan_interval_s", FLAGS_coordinator_recycle_orphan_interval_s);
  crontab_configs_.push_back({
      "RECYCLE",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_recycle_orphan_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCoordinatorRecycleOrphan(nullptr); },
  });

  // Add meta_watch_clean orphan crontab
  FLAGS_coordinator_meta_watch_clean_interval_s =
      GetInterval(config, "coordinator.meta_watch_clean_interval_s", FLAGS_coordinator_meta_watch_clean_interval_s);
  crontab_configs_.push_back({
      "meta_watch_clean",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_meta_watch_clean_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCoordinatorMetaWatchClean(nullptr); },
  });

  // Add recycle orphan crontab
  FLAGS_coordinator_remove_watch_interval_s =
      GetInterval(config, "coordinator.remove_watch_interval_s", FLAGS_coordinator_remove_watch_interval_s);
  crontab_configs_.push_back({
      "REMOVE_WATCH",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_remove_watch_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerKvRemoveOneTimeWatch(nullptr); },
  });

  // Add lease crontab
  FLAGS_coordinator_lease_interval_s =
      GetInterval(config, "coordinator.lease_interval_s", FLAGS_coordinator_lease_interval_s);
  crontab_configs_.push_back({
      "LEASE",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_lease_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerLeaseTask(nullptr); },
  });

  // Add compaction crontab
  if (GetRole() == pb::common::COORDINATOR) {
    FLAGS_auto_compaction = config->GetBool("coordinator.auto_compaction");
    DINGO_LOG(INFO) << "coordinator.auto_compaction:" << FLAGS_auto_compaction;

    uint32_t compaction_retention_rev_count = config->GetInt("coordinator.compaction_retention_rev_count");
    if (compaction_retention_rev_count <= 0) {
      DINGO_LOG(INFO) << "coordinator.compaction_retention_rev_count illegal, use default value :"
                      << FLAGS_compaction_retention_rev_count;
    } else {
      FLAGS_compaction_retention_rev_count = compaction_retention_rev_count;
    }
  }

  FLAGS_coordinator_compaction_interval_s =
      GetInterval(config, "coordinator.compaction_interval_s", FLAGS_coordinator_compaction_interval_s);
  crontab_configs_.push_back({
      "COMPACTION",
      {pb::common::COORDINATOR},
      FLAGS_coordinator_compaction_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerCompactionTask(nullptr); },
  });

  // Add scrub vector index crontab
  FLAGS_server_scrub_vector_index_interval_s =
      GetInterval(config, "server.scrub_vector_index_interval_s", FLAGS_server_scrub_vector_index_interval_s);
  crontab_configs_.push_back({
      "SCRUB_VECTOR_INDEX",
      {pb::common::INDEX},
      FLAGS_server_scrub_vector_index_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerScrubVectorIndex(nullptr); },
  });

  auto raft_store_engine = GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    // Add raft snapshot controller crontab
    FLAGS_raft_snapshot_interval_s = GetInterval(config, "raft.snapshot_interval_s", FLAGS_raft_snapshot_interval_s);
    crontab_configs_.push_back({
        "RAFT_SNAPSHOT_CONTROLLER",
        {pb::common::COORDINATOR, pb::common::STORE, pb::common::INDEX},
        FLAGS_raft_snapshot_interval_s * 1000,
        true,
        [](void*) { Server::GetInstance().GetRaftStoreEngine()->DoSnapshotPeriodicity(); },
    });
  }

  // Add gc update safe point ts crontab
  FLAGS_gc_update_safe_point_interval_s =
      GetInterval(config, "gc.update_safe_point_interval_s", FLAGS_gc_update_safe_point_interval_s);
  crontab_configs_.push_back({
      "GC_UPDATE_SAFE_POINT",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_gc_update_safe_point_interval_s * 1000,
      true,
      [](void*) { TxnEngineHelper::RegularUpdateSafePointTsHandler(nullptr); },
  });

  // Add gc  do gc crontab
  FLAGS_gc_do_gc_interval_s = GetInterval(config, "gc.do_gc_interval_s", FLAGS_gc_do_gc_interval_s);
  crontab_configs_.push_back({
      "GC_DO_GC",
      {pb::common::STORE, pb::common::INDEX},
      FLAGS_gc_do_gc_interval_s * 1000,
      true,
      [](void*) { TxnEngineHelper::RegularDoGcHandler(nullptr); },
  });

  crontab_manager_->AddCrontab(crontab_configs_);

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
  region_command_manager_ = std::make_shared<RegionCommandManager>(meta_reader_, meta_writer_);
  return region_command_manager_->Init();
}

bool Server::InitStoreMetricsManager() {
  store_metrics_manager_ = std::make_shared<StoreMetricsManager>(meta_reader_, meta_writer_, raft_engine_);
  return store_metrics_manager_->Init();
}

bool Server::InitVectorIndexManager() {
  vector_index_thread_pool_ =
      std::make_shared<ThreadPool>("vector_index", FLAGS_vector_operation_parallel_thread_num, []() {
        omp_set_num_threads(FLAGS_omp_num_threads);

        LOG(INFO) << fmt::format("omp max thread num per ancestor: {}", omp_get_max_threads());
      });

  vector_index_manager_ = VectorIndexManager::New();
  return vector_index_manager_->Init();
}

bool Server::InitPreSplitChecker() {
  pre_split_checker_ = std::make_shared<PreSplitChecker>();
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  int split_check_concurrency = config->GetInt("region.split_check_concurrency");
  split_check_concurrency =
      split_check_concurrency > 0 ? split_check_concurrency : Constant::kDefaultSplitCheckConcurrency;
  return pre_split_checker_->Init(split_check_concurrency);
}

bool Server::Recover() {
  if (GetRole() == pb::common::STORE) {
    // Recover engine state.
    if (!raft_engine_->Recover()) {
      DINGO_LOG(ERROR) << "Recover engine failed, engine " << raft_engine_->GetName();
      return false;
    }

    if (!region_controller_->Recover()) {
      DINGO_LOG(ERROR) << "Recover region controller failed";
      return false;
    }
  } else if (GetRole() == pb::common::INDEX) {
    // Recover engine state.
    if (!raft_engine_->Recover()) {
      DINGO_LOG(ERROR) << "Recover engine failed, engine " << raft_engine_->GetName();
      return false;
    }

    if (!region_controller_->Recover()) {
      DINGO_LOG(ERROR) << "Recover region controller failed";
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
  if (crontab_manager_) {
    crontab_manager_->Destroy();
  }
  if (heartbeat_) {
    heartbeat_->Destroy();
  }
  if (region_controller_) {
    region_controller_->Destroy();
  }
  if (store_controller_) {
    store_controller_->Destroy();
  }

  if (GetRole() == pb::common::INDEX && vector_index_manager_) {
    vector_index_manager_->Destroy();
  }

  google::ShutdownGoogleLogging();
}

bool Server::Ip2Hostname(std::string& ip2hostname) {
  if (!FLAGS_ip2hostname) {
    return true;
  }

  HostnameItem item;

  if (FLAGS_enable_ip2hostname_cache) {
    auto ret1 = ip2hostname_cache_.Get(ip2hostname, item);
    if (ret1 > 0) {
      if (item.timestamp + FLAGS_ip2hostname_cache_seconds >= butil::gettimeofday_ms()) {
        ip2hostname = item.hostname;
        return true;
      }
    }

    item.timestamp = butil::gettimeofday_ms();
  }

  item.hostname = Helper::Ip2HostName(ip2hostname);
  if (item.hostname.empty()) {
    return false;
  }

  if (FLAGS_enable_ip2hostname_cache) {
    ip2hostname_cache_.Put(ip2hostname, item);
  }

  ip2hostname = item.hostname;

  return true;
}

int64_t Server::Id() const {
  assert(id_ != 0);
  return id_;
}

std::string Server::Keyring() const { return keyring_; }

std::string Server::ServerAddr() { return server_addr_; }

std::string Server::LogDir() { return log_dir_; }

std::string Server::PidFilePath() { return Helper::ConcatPath(log_dir_, FLAGS_pid_file_name); }

butil::EndPoint Server::ServerEndpoint() { return server_endpoint_; }

void Server::SetServerEndpoint(const butil::EndPoint& endpoint) {
  server_endpoint_ = endpoint;
  server_addr_ = Helper::EndPointToStr(endpoint);
}

butil::EndPoint Server::RaftEndpoint() { return raft_endpoint_; }

void Server::SetRaftEndpoint(const butil::EndPoint& endpoint) { raft_endpoint_ = endpoint; }

std::shared_ptr<CoordinatorInteraction> Server::GetCoordinatorInteraction() {
  assert(coordinator_interaction_ != nullptr);
  return coordinator_interaction_;
}

std::shared_ptr<CoordinatorInteraction> Server::GetCoordinatorInteractionIncr() {
  assert(coordinator_interaction_incr_ != nullptr);
  return coordinator_interaction_incr_;
}

std::shared_ptr<Engine> Server::GetEngine() {
  assert(raft_engine_ != nullptr);
  return raft_engine_;
}

std::shared_ptr<RawEngine> Server::GetRawEngine(pb::common::RawEngine type) {
  assert(raft_engine_ != nullptr);
  return raft_engine_->GetRawEngine(type);
}

std::shared_ptr<RaftStoreEngine> Server::GetRaftStoreEngine() {
  auto engine = GetEngine();
  if (engine->GetID() == pb::common::StorageEngine::STORE_ENG_RAFT_STORE) {
    return std::dynamic_pointer_cast<RaftStoreEngine>(engine);
  }
  return nullptr;
}

std::shared_ptr<MetaReader> Server::GetMetaReader() {
  assert(meta_reader_ != nullptr);
  return meta_reader_;
}

std::shared_ptr<MetaWriter> Server::GetMetaWriter() {
  assert(meta_writer_ != nullptr);
  return meta_writer_;
}

std::shared_ptr<LogStorageManager> Server::GetLogStorageManager() {
  assert(log_storage_ != nullptr);
  return log_storage_;
}

std::shared_ptr<Storage> Server::GetStorage() {
  assert(storage_ != nullptr);
  return storage_;
}

std::shared_ptr<StoreMetaManager> Server::GetStoreMetaManager() {
  assert(store_meta_manager_ != nullptr);
  return store_meta_manager_;
}

store::RegionPtr Server::GetRegion(int64_t region_id) {
  return GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
}

std::vector<store::RegionPtr> Server::GetAllAliveRegion() {
  return GetStoreMetaManager()->GetStoreRegionMeta()->GetAllAliveRegion();
}

store::RaftMetaPtr Server::GetRaftMeta(int64_t region_id) {
  return GetStoreMetaManager()->GetStoreRaftMeta()->GetRaftMeta(region_id);
}

std::shared_ptr<StoreMetricsManager> Server::GetStoreMetricsManager() {
  assert(store_metrics_manager_ != nullptr);
  return store_metrics_manager_;
}

std::shared_ptr<CrontabManager> Server::GetCrontabManager() {
  assert(crontab_manager_ != nullptr);
  return crontab_manager_;
}

std::shared_ptr<StoreController> Server::GetStoreController() {
  assert(store_controller_ != nullptr);
  return store_controller_;
}

std::shared_ptr<RegionController> Server::GetRegionController() {
  assert(region_controller_ != nullptr);
  return region_controller_;
}

std::shared_ptr<RegionCommandManager> Server::GetRegionCommandManager() {
  assert(region_command_manager_ != nullptr);
  return region_command_manager_;
}

VectorIndexManagerPtr Server::GetVectorIndexManager() {
  assert(vector_index_manager_ != nullptr);
  return vector_index_manager_;
}

std::shared_ptr<CoordinatorControl> Server::GetCoordinatorControl() {
  assert(coordinator_control_ != nullptr);
  return coordinator_control_;
}

std::shared_ptr<AutoIncrementControl>& Server::GetAutoIncrementControl() {
  assert(auto_increment_control_ != nullptr);
  return auto_increment_control_;
}

std::shared_ptr<TsoControl> Server::GetTsoControl() {
  assert(tso_control_ != nullptr);
  return tso_control_;
}

std::shared_ptr<KvControl> Server::GetKvControl() {
  assert(kv_control_ != nullptr);
  return kv_control_;
}

void Server::SetEndpoints(const std::vector<butil::EndPoint>& endpoints) { endpoints_ = endpoints; }

std::shared_ptr<Heartbeat> Server::GetHeartbeat() { return heartbeat_; }

std::string Server::GetCheckpointPath() { return checkpoint_path_; }

std::string Server::GetStorePath() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  return config == nullptr ? "" : config->GetString("store.path");
}

std::string Server::GetRaftPath() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  return config == nullptr ? "" : config->GetString("raft.path");
}

std::string Server::GetRaftLogPath() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  return config == nullptr ? "" : config->GetString("raft.log_path");
}

std::string Server::GetIndexPath() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  return config == nullptr ? "" : config->GetString("vector.index_path");
}

bool Server::IsReadOnly() const { return is_read_only_; }

void Server::SetReadOnly(bool is_read_only) { is_read_only_ = is_read_only; }

bool Server::IsLeader(int64_t region_id) { return storage_->IsLeader(region_id); }

std::shared_ptr<PreSplitChecker> Server::GetPreSplitChecker() { return pre_split_checker_; }

void Server::SetStoreServiceReadWorkerSet(PriorWorkerSetPtr worker_set) { store_service_read_worker_set_ = worker_set; }

void Server::SetStoreServiceWriteWorkerSet(PriorWorkerSetPtr worker_set) {
  store_service_write_worker_set_ = worker_set;
}

void Server::SetIndexServiceReadWorkerSet(PriorWorkerSetPtr worker_set) { index_service_read_worker_set_ = worker_set; }

void Server::SetIndexServiceWriteWorkerSet(PriorWorkerSetPtr worker_set) {
  index_service_write_worker_set_ = worker_set;
}

void Server::SetRaftApplyWorkerSet(PriorWorkerSetPtr worker_set) { raft_apply_worker_set_ = worker_set; }

PriorWorkerSetPtr Server::GetRaftApplyWorkerSet() { return raft_apply_worker_set_; }

std::vector<std::vector<std::string>> Server::GetStoreServiceReadWorkerSetTrace() {
  if (store_service_read_worker_set_ == nullptr) {
    return {};
  }
  return store_service_read_worker_set_->GetPendingTaskTrace();
}

std::vector<std::vector<std::string>> Server::GetStoreServiceWriteWorkerSetTrace() {
  if (store_service_write_worker_set_ == nullptr) {
    return {};
  }
  return store_service_write_worker_set_->GetPendingTaskTrace();
}

std::vector<std::vector<std::string>> Server::GetIndexServiceReadWorkerSetTrace() {
  if (index_service_read_worker_set_ == nullptr) {
    return {};
  }
  return index_service_read_worker_set_->GetPendingTaskTrace();
}

std::vector<std::vector<std::string>> Server::GetIndexServiceWriteWorkerSetTrace() {
  if (index_service_write_worker_set_ == nullptr) {
    return {};
  }
  return index_service_write_worker_set_->GetPendingTaskTrace();
}

std::vector<std::vector<std::string>> Server::GetVectorIndexBackgroundWorkerSetTrace() {
  if (vector_index_manager_ == nullptr) {
    return {};
  }
  return vector_index_manager_->GetPendingTaskTrace();
}

uint64_t Server::GetVectorIndexManagerBackgroundPendingTaskCount() {
  if (vector_index_manager_ == nullptr) {
    return 0;
  }
  return vector_index_manager_->GetBackgroundPendingTaskCount();
}

std::vector<std::vector<std::string>> Server::GetRaftApplyWorkerSetTrace() {
  if (raft_apply_worker_set_ == nullptr) {
    return {};
  }
  return raft_apply_worker_set_->GetPendingTaskTrace();
}

std::string Server::GetAllWorkSetPendingTaskCount() {
  uint64_t store_servcie_read_pending_task_count =
      store_service_read_worker_set_ ? store_service_read_worker_set_->PendingTaskCount() : 0;
  uint64_t store_servcie_write_pending_task_count =
      store_service_write_worker_set_ ? store_service_write_worker_set_->PendingTaskCount() : 0;
  uint64_t index_servcie_read_pending_task_count =
      index_service_read_worker_set_ ? index_service_read_worker_set_->PendingTaskCount() : 0;
  uint64_t index_servcie_write_pending_task_count =
      index_service_write_worker_set_ ? index_service_write_worker_set_->PendingTaskCount() : 0;
  uint64_t raft_apply_pending_task_count = raft_apply_worker_set_ ? raft_apply_worker_set_->PendingTaskCount() : 0;

  return fmt::format("store_read({}) store_write({}) index_read({}) index_write({}) raft_apply({})",
                     store_servcie_read_pending_task_count, store_servcie_write_pending_task_count,
                     index_servcie_read_pending_task_count, index_servcie_write_pending_task_count,
                     raft_apply_pending_task_count);
}

ThreadPoolPtr Server::GetVectorIndexThreadPool() { return vector_index_thread_pool_; }

std::shared_ptr<pb::common::RegionDefinition> Server::CreateCoordinatorRegion(const std::shared_ptr<Config>& /*config*/,
                                                                              const int64_t region_id,
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
