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
#include <memory>
#include <vector>

#include "braft/util.h"
#include "butil/endpoint.h"
#include "butil/files/file_path.h"
#include "butil/strings/stringprintf.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/mem_engine.h"
#include "engine/raft_kv_engine.h"
#include "engine/rocks_engine.h"
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
  LOG(INFO) << "log_dir: " << FLAGS_log_dir;
  FLAGS_logbufsecs = 0;

  auto role_name = pb::common::ClusterRole_Name(role_);
  const std::string program_name = butil::StringPrintf("./%s", role_name.c_str());
  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(google::GLOG_INFO,
                            butil::StringPrintf("%s/%s.info.log.", FLAGS_log_dir.c_str(), role_name.c_str()).c_str());
  google::SetLogDestination(google::GLOG_WARNING,
                            butil::StringPrintf("%s/%s.warn.log.", FLAGS_log_dir.c_str(), role_name.c_str()).c_str());
  google::SetLogDestination(google::GLOG_ERROR,
                            butil::StringPrintf("%s/%s.error.log.", FLAGS_log_dir.c_str(), role_name.c_str()).c_str());
  google::SetLogDestination(google::GLOG_FATAL,
                            butil::StringPrintf("%s/%s.fatal.log.", FLAGS_log_dir.c_str(), role_name.c_str()).c_str());

  return true;
}

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  id_ = config->GetInt("cluster.instance_id");
  return id_ != 0;
}

bool Server::InitEngines(MetaControl& ctl) {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  std::shared_ptr<Engine> rock_engine = std::make_shared<RocksEngine>();
  if (!rock_engine->Init(config)) {
    LOG(ERROR) << "Init RocksEngine Failed with Config[" << config->ToString();
    return false;
  }

  // default Key-Value storage engine
  MetaControl* const controller = dynamic_cast<MetaControl*>(&ctl);
  bool is_coordinator = (role_ == pb::common::ClusterRole::COORDINATOR);
  std::shared_ptr<Engine> const raft_kv_engine = std::make_shared<RaftKvEngine>(rock_engine, controller);
  // std::make_shared<RaftKvEngine>(rock_engine, is_coordinator ? controller : nullptr);
  if (!raft_kv_engine->Init(config)) {
    LOG(ERROR) << "Init RaftKvEngine Failed with Config[" << config->ToString();
    return false;
  }
  // engines_.insert(std::make_pair(mem_engine->GetID(), mem_engine));
  engines_.insert(std::make_pair(rock_engine->GetID(), rock_engine));
  engines_.insert(std::make_pair(raft_kv_engine->GetID(), raft_kv_engine));
  return true;
}

pb::error::Errno Server::StartMetaRegion(std::shared_ptr<Config> config, std::shared_ptr<Engine> kv_engine) {
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
  std::vector<butil::EndPoint> peer_nodes = Helper::StrToEndpoint(coordinator_list);

  for (auto& peer_node : peer_nodes) {
    auto* peer = region->add_peers();
    auto* location = peer->mutable_raft_location();
    location->set_host(butil::ip2str(peer_node.ip).c_str());
    location->set_port(peer_node.port);
    LOG(INFO) << "COORDINATOR set peer node:" << (butil::ip2str(peer_node.ip).c_str()) << ":" << peer_node.port;
  }

  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000");
  range->set_end_key("FFFF");

  LOG(INFO) << "Create Region Request:" << region->DebugString();
  auto raft_engine = std::dynamic_pointer_cast<RaftKvEngine>(kv_engine);
  pb::error::Errno ret = raft_engine->AddRegion(ctx, region);
  return ret;
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
  auto engine = engines_[pb::common::ENG_ROCKSDB];
  store_meta_manager_ =
      std::make_shared<StoreMetaManager>(std::make_shared<MetaReader>(engine), std::make_shared<MetaWriter>(engine));
  return store_meta_manager_->Init();
}

bool Server::InitCrontabManager() {
  crontab_manager_ = std::make_shared<CrontabManager>();

  // Add heartbeat crontab
  std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  crontab->name_ = "HEARTBEA";
  crontab->interval_ = config->GetInt("server.heartbeatInterval");
  crontab->func_ = Heartbeat::SendStoreHeartbeat;
  crontab->arg_ = coordinator_interaction_.get();

  crontab_manager_->AddAndRunCrontab(crontab);

  return true;
}

bool Server::InitStoreControl() {
  store_control_ = std::make_shared<StoreControl>();
  return store_control_ != nullptr;
}

bool Server::Recover() {
  if (this->role_ == pb::common::STORE) {
    // Recover region meta data.
    if (!store_meta_manager_->Recover()) {
      LOG(ERROR) << "Recover store region meta data failed";
      return false;
    }

    // Recover engine state.
    for (auto& it : engines_) {
      if (!it.second->Recover()) {
        LOG(ERROR) << "Recover engine failed, engine " << it.second->GetName();
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
