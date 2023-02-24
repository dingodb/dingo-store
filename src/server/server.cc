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

namespace dingodb {

Server::Server() {}

Server::~Server() {}

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

bool Server::InitLog(const std::string& role) {
  auto config = ConfigManager::GetInstance()->GetConfig(role);
  FLAGS_log_dir = config->GetString("log.logPath");
  LOG(INFO) << "log_dir: " << FLAGS_log_dir;
  FLAGS_logbufsecs = 0;

  const std::string program_name = butil::StringPrintf("./%s", role.c_str());
  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      butil::StringPrintf("%s/%s.info.log.", FLAGS_log_dir.c_str(),
                          role.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      butil::StringPrintf("%s/%s.warn.log.", FLAGS_log_dir.c_str(),
                          role.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      butil::StringPrintf("%s/%s.error.log.", FLAGS_log_dir.c_str(),
                          role.c_str())
          .c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      butil::StringPrintf("%s/%s.fatal.log.", FLAGS_log_dir.c_str(),
                          role.c_str())
          .c_str());

  return true;
}

bool Server::ValiateCoordinator() { return true; }

bool Server::InitServerID() { return true; }

bool Server::InitRaftNodeManager() { return true; }

bool Server::InitStorage() {
  storage_ = std::make_shared<Storage>(new RaftKvEngine(new MemEngine()));
  return true;
}

void Server::Destroy() { google::ShutdownGoogleLogging(); }

}  // namespace dingodb