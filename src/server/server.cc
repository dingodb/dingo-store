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
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include "braft/util.h"
#include "butil/endpoint.h"
#include "butil/files/file_path.h"
#include "butil/time.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/version.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "coordinator/coordinator_control.h"
#include "engine/engine.h"
#include "engine/mem_engine.h"
#include "engine/raft_store_engine.h"
#include "engine/raw_bdb_engine.h"
#include "engine/raw_rocks_engine.h"
#include "engine/rocks_engine.h"
#include "gflags/gflags.h"
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

DECLARE_int64(compaction_retention_rev_count);
DECLARE_bool(auto_compaction);

DEFINE_bool(ip2hostname, false, "resolve ip to hostname for get map api");
DEFINE_bool(enable_ip2hostname_cache, true, "enable ip2hostname cache");
DEFINE_int64(ip2hostname_cache_seconds, 300, "ip2hostname cache seconds");

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

  FLAGS_log_dir = config->GetString("log.path");
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
  auto log_path = config->GetString("log.path");
  ret = Helper::CreateDirectories(log_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create log directory failed: " << log_path;
    return false;
  }

  return true;
}

bool Server::InitEngine() {
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  // init rocksdb
  auto raw_rocks_engine = std::make_shared<RawRocksEngine>();
  if (!raw_rocks_engine->Init(config, Helper::GetColumnFamilyNamesByRole())) {
    DINGO_LOG(ERROR) << "Init RawRocksEngine Failed with Config[" << config->ToString();
    return false;
  }

  meta_reader_ = std::make_shared<MetaReader>(raw_rocks_engine);
  meta_writer_ = std::make_shared<MetaWriter>(raw_rocks_engine);

  // init bdb
  auto raw_bdb_engine = std::make_shared<RawBdbEngine>();
  if (!raw_bdb_engine->Init(config, Helper::GetColumnFamilyNamesByRole())) {
    DINGO_LOG(ERROR) << "Init RawBdbEngine Failed with Config[" << config->ToString();
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

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.heartbeat_interval_s", Constant::kHeartbeatIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerStoreHeartbeat({}, true); },
  });

  // Add store region metrics crontab
  crontab_configs_.push_back({
      "STORE_REGION_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.metrics_collect_interval_s", Constant::kRegionMetricsCollectIntervalS) * 1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectStoreRegionMetrics(); },
  });

  // Add store metrics crontab
  crontab_configs_.push_back({
      "STORE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.store_metrics_collect_interval_s", Constant::kStoreMetricsCollectIntervalS) * 1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectStoreMetrics(); },
  });

  // Add store approximate size metrics crontab
  crontab_configs_.push_back({
      "APPROXIMATE_SIZE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.approximate_size_metrics_collect_interval_s",
                  Constant::kApproximateSizeMetricsCollectIntervalS) *
          1000,
      true,
      [](void*) { Server::GetInstance().GetStoreMetricsManager()->CollectApproximateSizeMetrics(); },
  });

  // Add scan crontab
  if (GetRole() == pb::common::STORE) {
    ScanManager::GetInstance().Init(config);
    crontab_configs_.push_back({
        "SCAN",
        {pb::common::STORE},
        GetInterval(config, "scan.scan_interval_s", Constant::kScanIntervalS) * 1000,
        false,
        [](void*) { ScanManager::RegularCleaningHandler(nullptr); },
    });
  }

  // Add split checker crontab
  if (GetRole() == pb::common::STORE || GetRole() == pb::common::INDEX) {
    bool enable_auto_split = config->GetBool("region.enable_auto_split");
    if (enable_auto_split) {
      crontab_configs_.push_back({
          "SPLIT_CHECKER",
          {pb::common::STORE, pb::common::INDEX},
          GetInterval(config, "region.split_check_interval_s", Constant::kDefaultSplitCheckIntervalS) * 1000,
          false,
          [](void*) { PreSplitChecker::TriggerPreSplitCheck(nullptr); },
      });
    }
  }

  // Add push crontab
  crontab_configs_.push_back({
      "PUSH",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.push_interval_s", Constant::kPushIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCoordinatorPushToStore(nullptr); },
  });

  // Add update state crontab
  crontab_configs_.push_back({
      "UPDATE",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.update_state_interval_s", Constant::kUpdateStateIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCoordinatorUpdateState(nullptr); },
  });

  // Add task list process crontab
  crontab_configs_.push_back({
      "TASKLIST",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.task_list_interval_s", Constant::kTaskListIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCoordinatorTaskListProcess(nullptr); },
  });

  // Add calculate crontab
  crontab_configs_.push_back({
      "CALCULATE",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.calc_metrics_interval_s", Constant::kCalcMetricsIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCalculateTableMetrics(nullptr); },
  });

  // Add recycle orphan crontab
  crontab_configs_.push_back({
      "RECYCLE",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.recycle_orphan_interval_s", Constant::kRecycleOrphanIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCoordinatorRecycleOrphan(nullptr); },
  });

  // Add recycle orphan crontab
  crontab_configs_.push_back({
      "REMOVE_WATCH",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.remove_watch_interval_s", Constant::kRemoveWatchIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerKvRemoveOneTimeWatch(nullptr); },
  });

  // Add lease crontab
  crontab_configs_.push_back({
      "LEASE",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.lease_interval_s", Constant::kLeaseIntervalS) * 1000,
      false,
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
  crontab_configs_.push_back({
      "COMPACTION",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.compaction_interval_s", Constant::kCompactionIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerCompactionTask(nullptr); },
  });

  // Add scrub vector index crontab
  crontab_configs_.push_back({
      "SCRUB_VECTOR_INDEX",
      {pb::common::INDEX},
      GetInterval(config, "server.scrub_vector_index_interval_s", Constant::kScrubVectorIndexIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerScrubVectorIndex(nullptr); },
  });

  auto raft_store_engine = GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    // Add raft snapshot controller crontab
    crontab_configs_.push_back({
        "RAFT_SNAPSHOT_CONTROLLER",
        {pb::common::COORDINATOR, pb::common::STORE, pb::common::INDEX},
        GetInterval(config, "raft.snapshot_interval_s", Constant::kRaftSnapshotIntervalS) * 1000,
        false,
        [](void*) { Server::GetInstance().GetRaftStoreEngine()->DoSnapshotPeriodicity(); },
    });
  }

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

bool Server::InitHeartbeat() { return heartbeat_->Init(); }

void Server::Destroy() {
  crontab_manager_->Destroy();
  heartbeat_->Destroy();
  region_controller_->Destroy();
  store_controller_->Destroy();
  vector_index_manager_->Destroy();

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

StoreRaftMeta::RaftMetaPtr Server::GetRaftMeta(int64_t region_id) {
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
