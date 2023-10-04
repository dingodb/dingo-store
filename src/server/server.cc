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
#include "butil/time.h"
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

DECLARE_uint32(compaction_retention_rev_count);
DECLARE_bool(auto_compaction);

DEFINE_bool(ip2hostname, false, "resolve ip to hostname for get map api");
DEFINE_bool(enable_ip2hostname_cache, true, "enable ip2hostname cache");
DEFINE_uint32(ip2hostname_cache_seconds, 300, "ip2hostname cache seconds");

void Server::SetRole(pb::common::ClusterRole role) { role_ = role; }

Server* Server::GetInstance() { return Singleton<Server>::get(); }

bool Server::InitConfig(const std::string& filename) {
  std::shared_ptr<Config> const config = std::make_shared<YamlConfig>();
  if (config->LoadFile(filename) != 0) {
    return false;
  }

  ConfigManager::GetInstance()->Register(role_, config);

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

  // raft path
  auto raft_path = config->GetString("raft.path");
  ret = Helper::CreateDirectories(raft_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create raft directory failed: " << raft_path;
    return false;
  }

  // raft log path
  auto log_path = config->GetString("raft.log_path");
  ret = Helper::CreateDirectories(log_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Create raft log directory failed: " << log_path;
    return false;
  }

  // vector index path
  if (role_ == pb::common::INDEX) {
    auto vector_index_path = config->GetString("vector.index_path");
    ret = Helper::CreateDirectories(vector_index_path);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "Create vector index directory failed: " << vector_index_path;
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

  meta_reader_ = std::make_shared<MetaReader>(raw_engine_);
  meta_writer_ = std::make_shared<MetaWriter>(raw_engine_);

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

static int32_t GetInterval(std::shared_ptr<Config> config, const std::string& config_name,  // NOLINT
                           int32_t default_value) {                                         // NOLINT
  int32_t interval_s = config->GetInt(config_name);
  return interval_s > 0 ? interval_s : default_value;
}

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

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
      [](void*) { Server::GetInstance()->GetStoreMetricsManager()->CollectStoreRegionMetrics(); },
  });

  // Add store metrics crontab
  crontab_configs_.push_back({
      "STORE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.store_metrics_collect_interval_s", Constant::kStoreMetricsCollectIntervalS) * 1000,
      true,
      [](void*) { Server::GetInstance()->GetStoreMetricsManager()->CollectStoreMetrics(); },
  });

  // Add store approximate size metrics crontab
  crontab_configs_.push_back({
      "APPROXIMATE_SIZE_METRICS",
      {pb::common::STORE, pb::common::INDEX},
      GetInterval(config, "server.approximate_size_metrics_collect_interval_s",
                  Constant::kApproximateSizeMetricsCollectIntervalS) *
          1000,
      true,
      [](void*) { Server::GetInstance()->GetStoreMetricsManager()->CollectApproximateSizeMetrics(); },
  });

  // Add scan crontab
  if (role_ == pb::common::STORE) {
    ScanManager::GetInstance()->Init(config);
    crontab_configs_.push_back({
        "SCAN",
        {pb::common::STORE},
        GetInterval(config, "scan.scan_interval_s", Constant::kScanIntervalS) * 1000,
        false,
        [](void*) { ScanManager::RegularCleaningHandler(ScanManager::GetInstance()); },
    });
  }

  // Add split checker crontab
  if (role_ == pb::common::STORE || role_ == pb::common::INDEX) {
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

  // Add lease crontab
  crontab_configs_.push_back({
      "LEASE",
      {pb::common::COORDINATOR},
      GetInterval(config, "coordinator.lease_interval_s", Constant::kLeaseIntervalS) * 1000,
      false,
      [](void*) { Heartbeat::TriggerLeaseTask(nullptr); },
  });

  // Add compaction crontab
  if (role_ == pb::common::COORDINATOR) {
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
  region_command_manager_ = std::make_shared<RegionCommandManager>(std::make_shared<MetaReader>(raw_engine_),
                                                                   std::make_shared<MetaWriter>(raw_engine_));
  return region_command_manager_->Init();
}

bool Server::InitStoreMetricsManager() {
  store_metrics_manager_ = std::make_shared<StoreMetricsManager>(raw_engine_, std::make_shared<MetaReader>(raw_engine_),
                                                                 std::make_shared<MetaWriter>(raw_engine_), engine_);
  return store_metrics_manager_->Init();
}

bool Server::InitVectorIndexManager() { return VectorIndexManager::Init(GetAllAliveRegion()); }

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

store::RegionPtr Server::GetRegion(uint64_t region_id) {
  if (store_meta_manager_ == nullptr) {
    return nullptr;
  }
  auto store_region_meta = store_meta_manager_->GetStoreRegionMeta();
  if (store_region_meta == nullptr) {
    return nullptr;
  }
  return store_region_meta->GetRegion(region_id);
}

std::vector<store::RegionPtr> Server::GetAllAliveRegion() {
  if (store_meta_manager_ == nullptr) {
    return {};
  }
  auto store_region_meta = store_meta_manager_->GetStoreRegionMeta();
  if (store_region_meta == nullptr) {
    return {};
  }
  return store_region_meta->GetAllAliveRegion();
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
