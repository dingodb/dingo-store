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


#ifndef DINGODB_STORE_SERVER_H_
#define DINGODB_STORE_SERVER_H_

#include <memory>

#include "engine/storage.h"

namespace dingodb {


class Server {
 public:
  Server();
  ~Server();

  // Init config.
  bool InitConfigs(const std::vector<std::string> filenames);

  // Init log.
  bool InitLog();

  // Valiate coordinator is connected and valid.
  bool ValiateCoordinator();

  // Every server instance has id, the id is allocated by coordinator.
  bool InitServerID();

  // Pull region infomation for init current node own region.
  bool InitRaftNodeManager();

  // Init storage engine.
  bool InitStorage();

  void Destroy();
  
  std::shared_ptr<Storage> get_storage() {
    return storage_;
  }

 private:
  uint64_t id_;
  std::shared_ptr<Storage> storage_;
};

} // namespace dingodb 

#endif // DINGODB_STORE_SERVER_H_