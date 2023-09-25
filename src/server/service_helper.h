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

#include <cstdint>
#include <string>

#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"

namespace dingodb {

class ServiceHelper {
 public:
  template <typename T>
  static void RedirectLeader(std::string addr, T* response);

  template <typename T>
  static pb::node::NodeInfo RedirectLeader(std::string addr);

  static butil::Status ValidateRegionEpoch(const pb::common::RegionEpoch& req_epoch, uint64_t region_id);
  static butil::Status ValidateRegionEpoch(const pb::common::RegionEpoch& req_epoch, store::RegionPtr region);
  static butil::Status GetStoreRegionInfo(uint64_t region_id, pb::error::StoreRegionInfo& store_region_info);
  static butil::Status GetStoreRegionInfo(store::RegionPtr region, pb::error::StoreRegionInfo& store_region_info);
  static butil::Status ValidateRegionState(store::RegionPtr region);
  static butil::Status ValidateRange(const pb::common::Range& range);
  static butil::Status ValidateKeyInRange(const pb::common::Range& range, const std::vector<std::string_view>& keys);
  static butil::Status ValidateRangeInRange(const pb::common::Range& region_range, const pb::common::Range& req_range);
  static butil::Status ValidateRegion(uint64_t region_id, const std::vector<std::string_view>& keys);
  static butil::Status ValidateIndexRegion(store::RegionPtr region, std::vector<uint64_t> vector_ids);
  static butil::Status ValidateSystemCapacity();
};

template <typename T>
pb::node::NodeInfo ServiceHelper::RedirectLeader(std::string addr) {
  auto raft_endpoint = Helper::StrToEndPoint(addr);
  if (raft_endpoint.port == 0) {
    DINGO_LOG(WARNING) << fmt::format("[redirect][addr({})] invalid addr.", addr);
    return {};
  }

  // From local store map query.
  auto node_info =
      Server::GetInstance()->GetStoreMetaManager()->GetStoreServerMeta()->GetNodeInfoByRaftEndPoint(raft_endpoint);
  if (node_info.id() == 0) {
    // From remote node query.
    Helper::GetNodeInfoByRaftLocation(Helper::EndPointToLocation(raft_endpoint), node_info);
  }

  if (!node_info.server_location().host().empty()) {
    // transform ip to hostname
    Server::GetInstance()->Ip2Hostname(*node_info.mutable_server_location()->mutable_host());
  }

  DINGO_LOG(INFO) << fmt::format("[redirect][addr({})] redirect leader, node_info: {}", addr,
                                 node_info.ShortDebugString());

  return node_info;
}

template <typename T>
void ServiceHelper::RedirectLeader(std::string addr, T* response) {
  auto node_info = RedirectLeader<T>(addr);
  if (node_info.id() != 0) {
    Helper::SetPbMessageErrorLeader(node_info, response);
  } else {
    response->mutable_error()->set_store_id(Server::GetInstance()->Id());
  }
}

}  // namespace dingodb

#endif  // DINGODB_SERVER_SERVICE_HELPER_H_