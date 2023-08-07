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

#ifndef DINGODB_SERVER_SERVICE_HELPER_H_
#define DINGODB_SERVER_SERVICE_HELPER_H_

#include <string>

#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"

namespace dingodb {

class ServiceHelper {
 public:
  template <typename T>
  static void RedirectLeader(std::string addr, T* response);

  template <typename T>
  static butil::EndPoint RedirectLeader(std::string addr);

  static butil::Status ValidateRegionState(store::RegionPtr region);
  static butil::Status ValidateRange(const pb::common::Range& range);
  static butil::Status ValidateKeyInRange(const pb::common::Range& range, const std::vector<std::string_view>& keys);
  static butil::Status ValidateRangeInRange(const pb::common::Range& region_range, const pb::common::Range& req_range);
  static butil::Status ValidateRegion(uint64_t region_id, const std::vector<std::string_view>& keys);
  static butil::Status ValidateIndexRegion(store::RegionPtr region, std::vector<uint64_t> vector_ids);
};

template <typename T>
butil::EndPoint ServiceHelper::RedirectLeader(std::string addr) {
  DINGO_LOG(INFO) << "Redirect leader " << addr;
  auto raft_endpoint = Helper::StrToEndPoint(addr);
  if (raft_endpoint.port == 0) return butil::EndPoint();

  // From local store map query.
  butil::EndPoint server_endpoint = Helper::QueryServerEndpointByRaftEndpoint(
      Server::GetInstance()->GetStoreMetaManager()->GetStoreServerMeta()->GetAllStore(), raft_endpoint);
  if (server_endpoint.port == 0) {
    // From remote node query.
    pb::common::Location server_location;
    Helper::GetServerLocation(Helper::EndPointToLocation(raft_endpoint), server_location);
    if (server_location.port() > 0) {
      server_endpoint = Helper::LocationToEndPoint(server_location);
    }
  }

  return server_endpoint;
}

template <typename T>
void ServiceHelper::RedirectLeader(std::string addr, T* response) {
  auto server_endpoint = RedirectLeader<T>(addr);
  if (server_endpoint.ip != butil::IP_ANY) {
    Helper::SetPbMessageErrorLeader(server_endpoint, response);
  }
}

}  // namespace dingodb

#endif  // DINGODB_SERVER_SERVICE_HELPER_H_