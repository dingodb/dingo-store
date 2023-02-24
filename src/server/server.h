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

template <typename T>
struct DefaultSingletonTraits;

namespace dingodb {

class Server {
 public:
  static Server* GetInstance();

  // Init config.
  bool InitConfig(const std::string& filename);

  // Init log.
  bool InitLog(const std::string& role);

  // Valiate coordinator is connected and valid.
  bool ValiateCoordinator();

  // Every server instance has id, the id is allocated by coordinator.
  bool InitServerID();

  // Pull region infomation for init current node own region.
  bool InitRaftNodeManager();

  // Init storage engine.
  bool InitStorage();

  void Destroy();

  std::shared_ptr<Storage> get_storage() { return storage_; }

  butil::EndPoint get_server_endpoint() { return server_endpoint_; }
  void set_server_endpoint(const butil::EndPoint& endpoint) {
    server_endpoint_ = endpoint;
  }

  butil::EndPoint get_raft_endpoint() { return raft_endpoint_; }
  void set_raft_endpoint(const butil::EndPoint& endpoint) {
    raft_endpoint_ = endpoint;
  }

 private:
  Server();
  ~Server();
  friend struct DefaultSingletonTraits<Server>;
  DISALLOW_COPY_AND_ASSIGN(Server);

  uint64_t id_;
  butil::EndPoint server_endpoint_;
  butil::EndPoint raft_endpoint_;
  std::shared_ptr<Storage> storage_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_SERVER_H_