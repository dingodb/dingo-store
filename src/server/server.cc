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

#include <vector>

#include "butil/files/file_path.h"
#include "butil/strings/stringprintf.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/mem_engine.h"
#include "engine/raft_kv_engine.h"
#include "engine/rocks_engine.h"
#include "proto/common.pb.h"
#include "store/heartbeat.h"

namespace dingodb {

void Server::set_role(pb::common::ClusterRole role) { role_ = role; }

Server* Server::GetInstance() { return Singleton<Server>::get(); }

bool Server::InitConfig(const std::string& filename) {
  std::shared_ptr<Config> const config = std::make_shared<YamlConfig>();
  if (config->LoadFile(filename) != 0) {
    return false;
  }

  butil::FilePath const filepath(filename);

  auto role_str_value = filepath.BaseName().RemoveExtension().value();
  pb::common::ClusterRole cluster_role;
  bool const is_ok =
      pb::common::ClusterRole_Parse(role_str_value, &cluster_role);
  if (!is_ok) {
    LOG(ERROR) << "Invalid Input role value[" + role_str_value +
                      "], Init Config Failed";
    return false;
  }

  ConfigManager::GetInstance()->Register(cluster_role, config);
  return true;
}

bool Server::InitLog() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  FLAGS_log_dir = config->GetString("log.logPath");
  LOG(INFO) << "log_dir: " << FLAGS_log_dir;
  FLAGS_logbufsecs = 0;

  auto role_str_value = pb::common::ClusterRole_Name(role_);
  const std::string program_name =
      butil::StringPrintf("./%s", role_str_value.c_str());
  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      butil::StringPrintf("%s/%s.info.log.", FLAGS_log_dir.c_str(),
                          role_str_value.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      butil::StringPrintf("%s/%s.warn.log.", FLAGS_log_dir.c_str(),
                          role_str_value.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      butil::StringPrintf("%s/%s.error.log.", FLAGS_log_dir.c_str(),
                          role_str_value.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      butil::StringPrintf("%s/%s.fatal.log.", FLAGS_log_dir.c_str(),
                          role_str_value.c_str())
          .c_str());

  return true;
}

bool Server::ValiateCoordinator() { return true; }

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  id_ = config->GetInt("cluster.instance_id");

  return id_ != 0;
}

bool Server::InitEngines() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  // std::shared_ptr<Engine> mem_engine = std::make_shared<MemEngine>();
  // if (!mem_engine->Init(config)) {
  //   return false;
  // }

  std::shared_ptr<Engine> rock_engine = std::make_shared<RocksEngine>();
  if (!rock_engine->Init(config)) {
    return false;
  }

  /**
   * todo: huzx will role init different engine
   * start raft node and start to vote
   */

  // will init meta storage engine

  // default Key-Value storage engine
  std::shared_ptr<Engine> raft_kv_engine =
      std::make_shared<RaftKvEngine>(rock_engine);
  if (!raft_kv_engine->Init(config)) {
    return false;
  }
  // engines_.insert(std::make_pair(mem_engine->GetID(), mem_engine));
  engines_.insert(std::make_pair(rock_engine->GetID(), rock_engine));
  engines_.insert(std::make_pair(raft_kv_engine->GetID(), raft_kv_engine));

  return true;
}

bool Server::InitCoordinatorInteraction() {
  coordinator_interaction_ = std::make_shared<CoordinatorInteraction>();

  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  return coordinator_interaction_->Init(
      config->GetString("cluster.coordinators"));
}

bool Server::InitRaftNodeManager() { return true; }

bool Server::InitStorage() {
  storage_ = std::make_shared<Storage>(engines_[pb::common::ENG_RAFT_STORE]);
  return true;
}

bool Server::InitStoreMetaManager() {
  store_meta_manager_ = std::make_shared<StoreMetaManager>();
  return true;
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

  // crontab_manager_->AddAndRunCrontab(crontab);

  return true;
}

bool Server::InitStoreControl() {
  store_control_ = std::make_shared<StoreControl>();
}

void Server::Destroy() {
  crontab_manager_->Destroy();
  google::ShutdownGoogleLogging();
}

}  // namespace dingodb
