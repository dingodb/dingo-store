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

#include "config/config.h"
#include "config/config_manager.h"

#include "engine/engine.h"
#include "engine/raft_kv_engine.h"
#include "engine/mem_engine.h"

namespace dingodb {


Server::Server() {

}

Server::~Server() {

}

bool Server::InitConfigs(const std::vector<std::string> filenames) {
  for (auto filename : filenames) {
    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    if (config->LoadFile(filename) != 0) {
      return false;
    }

    butil::FilePath filepath(filename);
    ConfigManager::GetInstance()->Register(filepath.BaseName().RemoveExtension().value(), config);
  }

  return true;
}

bool Server::InitLog() {
  return true;
}

bool Server::ValiateCoordinator() {
  return true;
}

bool Server::InitServerID() {
  return true;
}

bool Server::InitRaftNodeManager() {
  return true;
}

bool Server::InitStorage() {
  storage_ = std::make_shared<Storage>(new RaftKvEngine(new MemEngine()));
  return true;
}

void Server::Destroy() {
}


} // namespace dingodb 