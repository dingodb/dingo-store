
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

#ifndef DINGODB_SDK_TEST_TEST_COMMON_H_
#define DINGODB_SDK_TEST_TEST_COMMON_H_

#include <cstdint>
#include <string>
#include <type_traits>

#include "butil/endpoint.h"
#include "coordinator/tso_control.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/meta_cache.h"
#include "sdk/rpc/rpc.h"

namespace dingodb {

namespace sdk {
const std::string kIpOne = "192.0.0.1";
const std::string kIpTwo = "192.0.0.2";
const std::string kIpThree = "192.0.0.3";

const int kPort = 20001;

static std::string HostPortToAddrStr(std::string ip, int port) { return fmt::format("{}:{}", ip, port); }

const std::string kAddrOne = HostPortToAddrStr(kIpOne, kPort);
const std::string kAddrTwo = HostPortToAddrStr(kIpTwo, kPort);
const std::string kAddrThree = HostPortToAddrStr(kIpThree, kPort);

const std::map<std::string, RaftRole> kInitReplica = {
    {kAddrOne, kLeader}, {kAddrTwo, kFollower}, {kAddrThree, kFollower}};

static std::shared_ptr<Region> GenRegion(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch,
                                         pb::common::RegionType type) {
  std::vector<Replica> replicas;
  replicas.reserve(kInitReplica.size());
  for (const auto& entry : kInitReplica) {
    butil::EndPoint end_point;
    butil::str2endpoint(entry.first.c_str(), &end_point);
    replicas.push_back({end_point, entry.second});
  }
  return std::make_shared<Region>(id, range, epoch, type, replicas);
}

static std::shared_ptr<Region> RegionA2C(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'a';
  pb::common::Range range;
  range.set_start_key("a");
  range.set_end_key("c");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);
  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionC2E(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'c';
  pb::common::Range range;
  range.set_start_key("c");
  range.set_end_key("e");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);
  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionE2G(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'e';
  pb::common::Range range;
  range.set_start_key("e");
  range.set_end_key("g");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionB2F(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'b';
  pb::common::Range range;
  range.set_start_key("b");
  range.set_end_key("f");

  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionL2N(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'l';
  pb::common::Range range;
  range.set_start_key("l");
  range.set_end_key("n");

  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionA2Z(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'a';
  pb::common::Range range;
  range.set_start_key("a");
  range.set_end_key("z");

  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static void Region2ScanRegionInfo(const std::shared_ptr<Region>& region,
                                  pb::coordinator::ScanRegionInfo* scan_region_info) {
  scan_region_info->set_region_id(region->RegionId());

  auto* range = scan_region_info->mutable_range();
  *range = region->Range();

  auto* epoch = scan_region_info->mutable_region_epoch();
  *epoch = region->Epoch();

  auto replicas = region->Replicas();
  for (const auto& r : replicas) {
    if (r.role == kLeader) {
      auto* leader = scan_region_info->mutable_leader();
      *leader = Helper::EndPointToLocation(r.end_point);
    } else {
      auto* voter = scan_region_info->add_voters();
      *voter = Helper::EndPointToLocation(r.end_point);
    }
  }
}

static void Region2StoreRegionInfo(const std::shared_ptr<Region>& region,
                                   pb::error::StoreRegionInfo* store_region_info) {
  store_region_info->set_region_id(region->RegionId());

  auto* epoch = store_region_info->mutable_current_region_epoch();
  *epoch = region->Epoch();

  auto* range = store_region_info->mutable_current_range();
  *range = region->Range();

  auto replicas = region->Replicas();
  for (const auto& r : replicas) {
    auto* peer = store_region_info->add_peers();
    // TODO: support params
    peer->set_store_id(100);
    // TODO: support params
    peer->set_role(pb::common::PeerRole::VOTER);

    auto* location = peer->mutable_server_location();
    *location = Helper::EndPointToLocation(r.end_point);
  }
}

static pb::store::LockInfo PrepareLockInfo() {
  pb::store::LockInfo lock_info;
  lock_info.set_primary_lock("a0000000");
  lock_info.set_lock_ts(1);
  lock_info.set_lock_ttl(INT64_MAX);
  lock_info.set_txn_size(1);
  lock_info.set_lock_type(pb::store::Op::Put);

  return lock_info;
}

static bool LockInfoEqual(const pb::store::LockInfo& a, const pb::store::LockInfo& b) {
  if (a.primary_lock() != b.primary_lock()) {
    return false;
  }

  if (a.key() != b.key()) {
    return false;
  }

  if (a.lock_ts() != b.lock_ts()) {
    return false;
  }

  if (a.lock_type() != b.lock_type()) {
    return false;
  }

  return true;
}

static pb::meta::TsoTimestamp CurrentFakeTso() {
  static int64_t init_logic = 0;

  pb::meta::TsoTimestamp fake_tso;
  int64_t ct = ClockRealtimeMs();
  fake_tso.set_physical(ct);
  fake_tso.set_logical(init_logic);

  init_logic += 10;

  return fake_tso;
}

static void FillVectorIndexId(pb::meta::DingoCommonId* id, int64_t index_id, int64_t schema_id) {
  id->set_entity_type(pb::meta::EntityType::ENTITY_TYPE_INDEX);
  id->set_parent_entity_id(schema_id);
  id->set_entity_id(index_id);
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_TEST_COMMON_H_