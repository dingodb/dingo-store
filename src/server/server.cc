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
#include "server/heartbeat.h"

namespace dingodb {

void Server::set_role(const std::string& role) { role_ = role; }

Server* Server::GetInstance() { return Singleton<Server>::get(); }

bool Server::InitConfig(const std::string& filename) {
  std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
  if (config->LoadFile(filename) != 0) {
    return false;
  }

  butil::FilePath filepath(filename);
  ConfigManager::GetInstance()->Register(
      filepath.BaseName().RemoveExtension().value(), config);

  return true;
}

bool Server::InitLog() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  FLAGS_log_dir = config->GetString("log.logPath");
  LOG(INFO) << "log_dir: " << FLAGS_log_dir;
  FLAGS_logbufsecs = 0;

  const std::string program_name = butil::StringPrintf("./%s", role_.c_str());
  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      butil::StringPrintf("%s/%s.info.log.", FLAGS_log_dir.c_str(),
                          role_.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      butil::StringPrintf("%s/%s.warn.log.", FLAGS_log_dir.c_str(),
                          role_.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      butil::StringPrintf("%s/%s.error.log.", FLAGS_log_dir.c_str(),
                          role_.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      butil::StringPrintf("%s/%s.fatal.log.", FLAGS_log_dir.c_str(),
                          role_.c_str())
          .c_str());

  return true;
}

bool Server::ValiateCoordinator() { return true; }

bool Server::InitServerID() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  id_ = config->GetInt("cluster.instance_id");
}

bool Server::InitEngines() {
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  std::shared_ptr<Engine> mem_engine = std::make_shared<MemEngine>();
  mem_engine->Init(config);
  std::shared_ptr<Engine> raft_kv_engine =
      std::make_shared<RaftKvEngine>(mem_engine);
  raft_kv_engine->Init(config);
  engines_.insert(std::make_pair(mem_engine->GetID(), mem_engine));
  engines_.insert(std::make_pair(raft_kv_engine->GetID(), raft_kv_engine));
}

bool Server::InitRaftNodeManager() { return true; }

bool Server::InitStorage() {
  storage_ = std::make_shared<Storage>(engines_[pb::common::ENG_RAFTSTORE]);
  return true;
}

bool Server::InitStoreMetaManager() {
  store_meta_manager_ = std::make_shared<StoreMetaManager>();
  return true;
}

bool Server::InitCrontabManager() {
  channel_ = std::make_shared<brpc::Channel>();
  crontab_manager_ = std::make_shared<CrontabManager>();

  // Add heartbeat crontab
  std::shared_ptr<Crontab> crontab = std::make_shared<Crontab>();
  auto config = ConfigManager::GetInstance()->GetConfig(role_);
  crontab->name_ = "HEARTBEA";
  crontab->interval_ = config->GetInt("server.heartbeatInterval");
  crontab->func_ = Heartbeat::SendStoreHeartbeat;
  crontab->arg_ = channel_.get();

  crontab_manager_->AddAndRunCrontab(crontab);

  return true;
}

void Server::Destroy() {
  crontab_manager_->Destroy();
  google::ShutdownGoogleLogging();
}

}  // namespace dingodb